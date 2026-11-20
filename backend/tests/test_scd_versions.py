"""缓慢变化维版本链回归测试（标准库 unittest，无需额外依赖）。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest

from app.pipeline import pipeline_bus
from app.services.change_event import ChangeEventService
from app.services.scd_handler import ScdHandlerService
from app.store import store

CUSTOMER = {"配置名称": "客户维", "维度表": "dim_customer", "SCD类型": "Type2", "自然键": "id"}
PRODUCT = {"配置名称": "产品维", "维度表": "dim_product", "SCD类型": "Type2", "自然键": "id"}


def event(event_id: str, dimension: str, natural_key: str, name: str,
          event_time: str, before: dict | None = None) -> dict:
    return {
        "事件ID": event_id,
        "来源维度": dimension,
        "来源表": f"dw.{dimension}",
        "操作类型": "UPDATE",
        "变更前镜像": before or {"id": natural_key, "name": "old"},
        "变更后镜像": {"id": natural_key, "name": name},
        "事件时间": event_time,
    }


class ScdVersionChainTest(unittest.TestCase):
    def setUp(self) -> None:
        pipeline_bus.scd_type1_store.clear()
        pipeline_bus.scd_type2_store.clear()
        pipeline_bus.scd_type3_store.clear()
        pipeline_bus.scd_replay_manifest.clear()
        pipeline_bus.scd_replay_index.clear()
        self.service = ScdHandlerService()

    def test_same_natural_key_in_two_dimensions_keeps_separate_history(self) -> None:
        first = self.service.apply_scd(
            CUSTOMER, event("C1", "dim_customer", "1001", "黄金", "2026-11-20T09:00:00"),
            processed_at="2026-11-20T09:00:05")
        second = self.service.apply_scd(
            PRODUCT, event("P1", "dim_product", "1001", "12", "2026-11-20T09:10:00"),
            processed_at="2026-11-20T09:10:05")
        self.assertEqual(first["version"], 1)
        self.assertEqual(second["version"], 1)
        # 产品维的写入不能关闭客户维的有效版本。
        customer = self.service.query_dimension_history("dim_customer", "1001")
        self.assertEqual(len(customer), 1)
        self.assertTrue(customer[0]["is_current"])
        self.assertEqual(customer[0]["effective_to"], "9999-12-31T23:59:59.999999")

    def test_late_correction_does_not_restate_effective_interval(self) -> None:
        self.service.apply_scd(
            CUSTOMER, event("C1", "dim_customer", "1001", "普通", "2026-11-20T09:00:00"),
            processed_at="2026-11-20T09:00:05")
        self.service.apply_scd(
            CUSTOMER, event("C2", "dim_customer", "1001", "钻石", "2026-11-20T11:00:00"),
            processed_at="2026-11-20T11:00:05")
        late = self.service.apply_scd(
            CUSTOMER, event("C3", "dim_customer", "1001", "白银", "2026-11-20T10:00:00"),
            processed_at="2026-11-20T13:00:00")
        self.assertTrue(late["late_correction"])
        self.assertFalse(late["restated"])
        history = self.service.query_dimension_history("dim_customer", "1001")
        self.assertEqual(history[0]["effective_to"], "2026-11-20T11:00:00.000000")
        self.assertEqual(history[1]["effective_to"], "2026-11-20T13:00:00.000000")
        self.assertEqual(history[2]["effective_from"], "2026-11-20T13:00:00.000000")
        # 已经生效的区间按当时口径重算仍是原版本。
        self.assertEqual(
            self.service.query_as_of("dim_customer", "1001", "2026-11-20T10:30:00")["version"], 1)
        self.assertEqual(
            self.service.query_as_of("dim_customer", "1001", "2026-11-20T12:30:00")["version"], 2)

    def test_duplicate_submission_does_not_close_again(self) -> None:
        self.service.apply_scd(
            CUSTOMER, event("C1", "dim_customer", "1001", "普通", "2026-11-20T09:00:00"),
            processed_at="2026-11-20T09:00:05")
        self.service.apply_scd(
            CUSTOMER, event("C2", "dim_customer", "1001", "黄金", "2026-11-20T11:00:00"),
            processed_at="2026-11-20T11:00:05")
        before = self.service.query_dimension_history("dim_customer", "1001")
        duplicate = self.service.apply_scd(
            CUSTOMER, event("C2", "dim_customer", "1001", "黄金", "2026-11-20T11:00:00"),
            processed_at="2026-11-20T15:00:00")
        after = self.service.query_dimension_history("dim_customer", "1001")
        self.assertEqual(duplicate["action"], "duplicate")
        self.assertEqual(len(before), len(after))
        self.assertEqual([row["effective_to"] for row in before],
                         [row["effective_to"] for row in after])

    def test_intervals_are_disjoint_and_reconciliation_matches(self) -> None:
        self.service.apply_scd(
            CUSTOMER, event("C1", "dim_customer", "1001", "普通", "2026-11-20T09:00:00"),
            processed_at="2026-11-20T09:00:05")
        self.service.apply_scd(
            PRODUCT, event("P1", "dim_product", "1001", "12", "2026-11-20T09:10:00"),
            processed_at="2026-11-20T09:10:05")
        self.service.apply_scd(
            CUSTOMER, event("C2", "dim_customer", "1001", "黄金", "2026-11-20T11:00:00"),
            processed_at="2026-11-20T11:00:05")
        self.service.apply_scd(
            CUSTOMER, event("C3", "dim_customer", "1001", "白银", "2026-11-20T10:00:00"),
            processed_at="2026-11-20T13:00:00")

        for table in ("dim_customer", "dim_product"):
            self.assertTrue(self.service.validate_intervals(table)["valid"])
            # 同一时点只能命中一条有效记录。
            for moment in ("2026-11-20T09:30:00", "2026-11-20T10:30:00",
                           "2026-11-20T12:30:00", "2026-11-20T20:00:00"):
                hits = [
                    row for row in self.service.query_dimension_history(table, "1001")
                    if row["effective_from"] <= moment < row["effective_to"]
                ]
                self.assertLessEqual(len(hits), 1)
        self.assertTrue(self.service.reconcile_manifest()["consistent"])
        verify = self.service.verify_replay("dim_customer", "1001")
        self.assertTrue(verify["stable"])

    def test_change_event_entry_keeps_dimension_ownership(self) -> None:
        rows = store.rows("change_event")
        rows[0]["变更前镜像"] = '{"id": "1001", "name": "old"}'
        rows[0]["变更后镜像"] = '{"id": "1001", "name": "new"}'
        service = ChangeEventService()
        result = service.dispatch_to_scd(
            [rows[0]["事件ID"]], dimension="dim_customer", natural_key="id")
        self.assertTrue(result["ok"])
        self.assertEqual(
            {entry["dim_table"] for entry in result["manifest"]}, {"dim_customer"})
        # 同一事件再提交一次只留一条 duplicate 回放记录，不会新增版本。
        repeat = service.dispatch_to_scd(
            [rows[0]["事件ID"]], dimension="dim_customer", natural_key="id")
        self.assertEqual(repeat["results"][0]["action"], "duplicate")
        self.assertEqual(len(self.service.query_dimension_history("dim_customer", "1001")), 1)


if __name__ == "__main__":
    unittest.main()
