"""变更事件领域服务：消费管道中的 ChangeEventRecord，执行事件解析、分类和状态流转。

上游：binlog_parser 产出事件推入管道 → change_event 消费解析
下游：format_adapter 格式适配 → event_filter 过滤 → route_dispatcher 路由分发
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus
from app.store import store


class ChangeEventService(BaseModuleService):
    MODULE = "change_event"
    ENTITY = "变更记录"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["事件ID", "来源表", "操作类型", "变更前镜像", "变更后镜像", "事务ID", "事件时间"]
    REQUIRED_FIELDS = ["事件ID", "来源表", "操作类型"]
    STATUS_ORDER = ["已捕获", "已解析", "已下发", "解析失败"]
    ACTION_RULES = {"解析事件": "已捕获", "下发事件": "已解析", "标记失败": "已解析"}
    NEGATIVE_ACTIONS = []

    def consume_pipeline_events(self, batch_size: int = 50) -> list[dict[str, Any]]:
        """从管道总线消费变更事件并分类汇总。

        消费 pipeline_bus 中 binlog_parser 产出的缓冲区事件，
        按操作类型 (INSERT/UPDATE/DELETE) 和来源表分组。
        """
        raw_events = pipeline_bus.consume_events(batch_size=batch_size)
        if not raw_events:
            return []
        by_op: dict[str, int] = {}
        by_table: dict[str, int] = {}
        entries: list[dict[str, Any]] = []
        for evt in raw_events:
            by_op[evt.operation] = by_op.get(evt.operation, 0) + 1
            by_table[evt.source_table] = by_table.get(evt.source_table, 0) + 1
            before_str = json.dumps(evt.before_image, ensure_ascii=False) if evt.before_image else "{}"
            after_str = json.dumps(evt.after_image, ensure_ascii=False) if evt.after_image else "{}"
            entries.append({
                "事件ID": evt.event_id,
                "来源表": f"{evt.source_db}.{evt.source_table}",
                "操作类型": evt.operation,
                "变更前镜像": before_str[:200],
                "变更后镜像": after_str[:200],
                "事务ID": evt.tx_id,
                "事件时间": evt.event_time.isoformat(),
                "raw_format": evt.raw_format,
                "binlog_pos": evt.binlog_pos,
            })
        pipeline_bus.write_audit(
            "事件消费", f"{len(entries)}条事件",
            f"消费管道事件 {len(entries)} 条，按业务类型分组完成",
            "change_event", "已解析"
        )
        return entries

    def classify_by_operation(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        """对消费后的事件按操作类型分类统计。

        为下游 format_adapter 提供操作类型分布以决定格式适配策略。
        """
        ops: dict[str, int] = {"insert": 0, "update": 0, "delete": 0}
        tables: dict[str, int] = {}
        total_rows_affected = 0
        for evt in events:
            op = evt.get("操作类型", "").upper()
            if op == "INSERT":
                ops["insert"] += 1
            elif op == "UPDATE":
                ops["update"] += 1
            elif op == "DELETE":
                ops["delete"] += 1
            tbl = evt.get("来源表", "unknown")
            tables[tbl] = tables.get(tbl, 0) + 1
            total_rows_affected += 1
        return {
            "total_events": len(events),
            "by_operation": ops,
            "by_table": tables,
            "insert_ratio": round(ops["insert"] / max(len(events), 1) * 100, 1),
            "update_ratio": round(ops["update"] / max(len(events), 1) * 100, 1),
            "delete_ratio": round(ops["delete"] / max(len(events), 1) * 100, 1),
        }

    def delta_computation(self, event: dict[str, Any]) -> dict[str, Any]:
        """对变更前后镜像执行差异计算。

        计算 UPDATE 操作中哪些字段实际发生了变化，
        产出变更增量列表供下游 SCD 策略消费。
        """
        try:
            before = json.loads(event.get("变更前镜像", "{}"))
            after_raw = event.get("变更后镜像", "{}")
            after = json.loads(after_raw) if isinstance(after_raw, str) else after_raw
        except (json.JSONDecodeError, TypeError):
            return {"delta_fields": [], "delta_count": 0}
        if not isinstance(before, dict):
            before = {}
        if not isinstance(after, dict):
            after = {}
        changed = []
        for key in after:
            if key not in before or before[key] != after[key]:
                delta_val = {"field": key, "old": before.get(key), "new": after[key]}
                changed.append(delta_val)
        for key in before:
            if key not in after:
                changed.append({"field": key, "old": before.get(key), "new": None})
        return {"delta_fields": changed, "delta_count": len(changed)}

    def dispatch_to_scd(self, event_ids: list[str], dimension: str = "",
                        natural_key: str = "", scd_type: str = "",
                        config: dict[str, Any] | None = None,
                        processing_time: str = "") -> dict[str, Any]:
        """把选定的变更事件交给缓慢变化维处理。

        入口一次只受理一个维度：事件未填写来源维度时用 `dimension` 补齐，
        这样两张维表即使提交相同的自然键，也会各自写入自己的 Type2 版本链。
        `processing_time` 用于回放/补数，让迟到判定和区间重算可复现。
        处置结果（版本、摘要、是否迟到）由 scd_handler 回写到事件回放清单。
        """
        from app.services import service_for

        scd_service = service_for("scd_handler")
        if scd_service is None:
            return {"ok": False, "message": "缓慢变化维服务未注册"}
        rows = store.rows(self.MODULE)
        selected = [row for row in rows if str(row.get("事件ID", "")) in set(event_ids)]
        if not selected:
            return {"ok": False, "message": "没有匹配到待处理的变更事件"}

        # 同一入口一次只挂一个维度配置；自然键从事件镜像里读取。
        config: dict[str, Any] = {
            "配置名称": f"变更事件入口-{dimension}" if dimension else "变更事件入口",
            "维度表": dimension,
        }
        if natural_key:
            config["自然键"] = natural_key
        if scd_type:
            config["SCD类型"] = scd_type

        payload: list[dict[str, Any]] = []
        for row in selected:
            event = dict(row)
            for field in ("变更前镜像", "变更后镜像"):
                raw = event.get(field)
                if isinstance(raw, str):
                    try:
                        event[field] = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        event[field] = {}
            if dimension:
                event["来源维度"] = dimension
            if processing_time:
                event["_processed_at"] = processing_time
            payload.append(event)

        outcome = scd_service.handle_change_events(
            payload, config=config, apply=True)
        applied = [item for item in outcome["results"] if item.get("action") != "unresolved"]
        pipeline_bus.write_audit(
            "SCD分发", dimension or "多维度",
            f"变更事件 {'、'.join(event_ids)} 提交缓慢变化维处置，写入 {len(applied)} 条",
            "change_event", "已处理" if applied else "未处理"
        )
        return {
            "ok": bool(applied),
            "message": f"已提交 {len(selected)} 条事件，写入 {len(applied)} 条维度处置结果",
            "results": outcome["results"],
            "manifest": outcome["manifest"],
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "下发事件":
            pipeline_bus.write_audit(
                "事件下发", entry.get("来源表", ""),
                f"事件 {entry_id} 下发给下游消费者",
                "change_event", "已下发"
            )
        return entry, msg