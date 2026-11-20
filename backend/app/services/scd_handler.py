"""缓慢变化维领域服务：处理维度数据的SCD Type 1/2/3更新策略。

上游：change_event + format_adapter 产出已适配事件 → scd_handler 应用SCD策略
消费：pipeline_bus 中的 SCD 状态存储为下游分析提供维度快照

版本链一律以 `维度表 -> 自然键 -> 版本列表` 两层键存储。两个维度表即使自然键
完全相同，也各自持有独立的 Type2 版本链，追加版本时只关闭本维表自己的当前版本。

迟到更正策略（显式取舍）：
    已经生效的历史区间不重述。迟到的更正（事件时间早于当前开放版本的生效时间）
    作为新的当前版本落在处理时刻，原版本的有效区间保持不变。这样已经交付给下游
    的版本、以及按当时时点重算的历史查询结果都不会被追溯改写；代价是更正只对
    到处理时刻之后的事实引用生效。回放清单会记录 `late_correction` 与
    `restated=False`，便于核对“事实当时引用的是哪个版本”。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus
from app.store import store

OPEN_END = "9999-12-31T23:59:59.999999"
_EPSILON = timedelta(microseconds=1)
_TYPE1 = {"Type1", "ScdType1", "SCD1", "1"}
_TYPE2 = {"Type2", "ScdType2", "SCD2", "2"}
_TYPE3 = {"Type3", "ScdType3", "SCD3", "3"}


def _parse_dt(value: Any, default: datetime | None = None) -> datetime | None:
    """把时间值解析成不带时区的 datetime，避免 aware/naive 混比较。"""
    if isinstance(value, datetime):
        dt = value
    elif value is None or str(value).strip() == "":
        return default
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            try:
                dt = datetime.fromisoformat(text[:10])
            except ValueError:
                return default
    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="microseconds")


def _digest(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


class ScdHandlerService(BaseModuleService):
    MODULE = "scd_handler"
    ENTITY = "SCD配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置名称", "维度表", "SCD类型", "自然键", "代理键", "生效时间列", "失效时间列"]
    REQUIRED_FIELDS = ["配置名称", "维度表", "SCD类型"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用配置": "草稿", "停用配置": "已启用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    # ------------------------------------------------------------------
    # 变更事件 -> 维度变更事件
    # ------------------------------------------------------------------
    def enrich_event(self, event: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
        """补齐变更事件的来源维度、自然键与字段差异。

        change_event 消费管道事件时调用一次；apply_scd 收到未补齐的事件时也会
        再调一次，保证无论从哪个入口进来，承载的都是同一份维度变更语义。
        """
        enriched = dict(event)
        explicit_dimension = str(enriched.get("来源维度") or "").strip()
        configured_dimension = str((config or {}).get("维度表") or "").strip()
        if explicit_dimension and configured_dimension and explicit_dimension != configured_dimension:
            # 事件声明的维度与配置不一致时绝不改写事件归属，交给调用方判为 unresolved。
            enriched["_dimension_conflict"] = {
                "event_dimension": explicit_dimension,
                "config_dimension": configured_dimension,
            }
            return enriched
        dim_table = (
            configured_dimension
            or explicit_dimension
            or self._table_from_source(str(enriched.get("来源表") or ""))
        )
        if dim_table:
            enriched["来源维度"] = dim_table

        natural_keys = self._natural_keys(config)
        if not enriched.get("自然键"):
            enriched["自然键"] = self._build_dim_key(enriched, natural_keys)

        if "字段差异" not in enriched or enriched.get("字段差异") is None:
            enriched["字段差异"] = self.field_diff(
                enriched.get("变更前镜像"), enriched.get("变更后镜像"))
        return enriched

    def field_diff(self, before: Any, after: Any) -> list[dict[str, Any]]:
        """计算变更前后镜像的字段差异。"""
        before_dict = _json_dict(before)
        after_dict = _json_dict(after)
        changed: list[dict[str, Any]] = []
        for key, new_value in after_dict.items():
            if key not in before_dict or before_dict[key] != new_value:
                changed.append({"field": key, "old": before_dict.get(key), "new": new_value})
        for key, old_value in before_dict.items():
            if key not in after_dict:
                changed.append({"field": key, "old": old_value, "new": None})
        return changed

    # ------------------------------------------------------------------
    # 变更事件入口
    # ------------------------------------------------------------------
    def handle_change_events(self, events: list[dict[str, Any]], config: dict[str, Any] | None = None,
                             override: dict[str, Any] | None = None,
                             apply: bool = True) -> dict[str, Any]:
        """从变更事件入口接收事件，逐个补齐来源维度/自然键/字段差异后落库。

        - `config`：指定唯一的 SCD 配置（适合只处理一张维表）。
        - `override`：按事件自带字段临时指定维度表、自然键、生效/失效时间列。
        - 两者都不给时按事件的 `来源维度`（缺省取来源表名）匹配已启用的 SCD 配置。
          维度无法确定的事件直接标记 unresolved，绝不会借用别的维表配置。

        返回每条事件的处置结果与回放清单，便于核对数据归属。
        """
        results: list[dict[str, Any]] = []
        for event in events:
            enriched = self.enrich_event(event, config)
            resolved = config or self.resolve_config(enriched, override)
            if resolved is not None and not resolved.get("维度表"):
                resolved = None
            if resolved is None:
                results.append({
                    "event_id": enriched.get("事件ID", ""),
                    "source_dimension": enriched.get("来源维度", ""),
                    "natural_key": enriched.get("自然键", ""),
                    "action": "unresolved",
                    "detail": "没有匹配到已启用的SCD配置，未写入维度数据",
                })
                continue
            enriched = self.enrich_event(enriched, resolved)
            outcome = self.apply_scd(
                resolved, enriched, processed_at=enriched.get("_processed_at")) if apply else {
                "dim_table": resolved.get("维度表"),
                "natural_key": enriched.get("自然键"),
                "scd_type": resolved.get("SCD类型", "Type2"),
                "action": "preview",
                "row": None,
            }
            outcome["event_id"] = enriched.get("事件ID", "")
            outcome["source_dimension"] = enriched.get("来源维度", "")
            outcome["config_name"] = resolved.get("配置名称", "")
            results.append(outcome)
        return {
            "results": results,
            "applied": sum(1 for item in results if item.get("action") not in ("unresolved", "preview")),
            "unresolved": sum(1 for item in results if item.get("action") == "unresolved"),
            "manifest": self.query_replay_manifest(dim_table=(
                config or override or {}).get("维度表") if (config or override) else None),
        }

    def resolve_config(self, event: dict[str, Any],
                       override: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """为事件找到对应的 SCD 配置：显式覆盖 > 已启用配置 > 该维表任意配置。

        维度不明确（事件没带来源维度、来源表也推不出来）时返回 None，
        宁可显式标记 unresolved，也不能退回到别的维表配置上串写版本链。
        """
        dimension = str(event.get("来源维度") or "")
        if not dimension:
            return None
        rows = store.rows(self.MODULE)
        candidates = [row for row in rows if str(row.get("维度表", "")) == dimension]
        enabled = [row for row in candidates if row.get("status") == "已启用"]
        chosen = (enabled or candidates or [None])[0]
        if chosen is None:
            chosen = {"配置名称": f"自动识别-{dimension}", "维度表": dimension,
                      "SCD类型": "Type2"}
        config = dict(chosen)
        if override:
            for field in ("维度表", "自然键", "代理键", "生效时间列", "失效时间列", "SCD类型"):
                if override.get(field):
                    config[field] = override[field]
        return config

    # ------------------------------------------------------------------
    # SCD 策略入口
    # ------------------------------------------------------------------
    def apply_scd(self, config: dict[str, Any], event: dict[str, Any],
                  processed_at: Any = None, restate: bool = False) -> dict[str, Any]:
        """根据配置的SCD类型处理变更事件。

        读取事件中的来源维度、自然键、字段差异和变更前后镜像，
        对维度表执行 Type1(覆盖)、Type2(历史)、Type3(前值列) 策略。
        迟到更正不重述已生效区间（见模块 docstring）。
        `restate` 保留为显式开关，当前实现固定不重述，参数不改变区间口径。
        """
        _ = restate  # 取舍：不重述已生效区间
        scd_type = str(config.get("SCD类型", "Type2"))
        dim_table = str(config.get("维度表") or "").strip()
        if not dim_table:
            # 没有来源维度的变更事件宁可不落库，也不能写进别的维表版本链。
            return {
                "dim_table": "",
                "scd_type": scd_type,
                "natural_key": "",
                "action": "unresolved",
                "detail": "事件缺少来源维度，未写入任何维表",
                "late_correction": False,
                "restated": False,
                "row": None,
            }
        natural_keys = self._natural_keys(config)
        surrogate_key = config.get("代理键") or f"sk_{dim_table}"
        processed = _parse_dt(processed_at, default=datetime.now()) or datetime.now()

        enriched = self.enrich_event(event, config)
        if enriched.get("_dimension_conflict"):
            conflict = enriched["_dimension_conflict"]
            return {
                "dim_table": conflict["config_dimension"],
                "scd_type": scd_type,
                "natural_key": "",
                "action": "unresolved",
                "detail": (
                    f"事件来源维度 {conflict['event_dimension']} 与配置维度 "
                    f"{conflict['config_dimension']} 不一致，未写入任何维表"
                ),
                "late_correction": False,
                "restated": False,
                "row": None,
            }
        natural_key = str(enriched.get("自然键") or self._build_dim_key(enriched, natural_keys))
        event_id = str(enriched.get("事件ID") or "")
        changed_fields = list(enriched.get("字段差异") or [])

        duplicate_of = self._find_previous_disposition(dim_table, natural_key, event_id)
        if duplicate_of is not None:
            replay = self._write_manifest(
                dim_table=dim_table,
                natural_key=natural_key,
                surrogate_key=duplicate_of.get("surrogate_key", surrogate_key),
                version=duplicate_of.get("version"),
                event=enriched,
                changed_fields=changed_fields,
                digest=duplicate_of.get("digest", ""),
                disposition="duplicate",
                detail="重复提交，复用首次处置结果，未再次关闭任何版本",
                late_correction=False,
                restated=False,
                effective_from=duplicate_of.get("effective_from"),
                effective_to=duplicate_of.get("effective_to"),
                processed_at=processed,
            )
            duplicate_row = duplicate_of.get("row")
            return {
                "dim_table": dim_table,
                "scd_type": scd_type,
                "natural_key": natural_key,
                "surrogate_key": duplicate_of.get("surrogate_key"),
                "action": "duplicate",
                "duplicate": True,
                "late_correction": bool((duplicate_row or {}).get("late_correction", False)),
                "restated": False,
                "version": duplicate_of.get("version"),
                "effective_from": duplicate_of.get("effective_from"),
                "effective_to": duplicate_of.get("effective_to"),
                "changed_fields": changed_fields,
                "replay_id": replay["replay_id"],
                "row": duplicate_row,
            }

        row = self._extract_dim_row(enriched, dim_table)
        event_time = self._event_time(enriched, config, processed)

        result: dict[str, Any] = {
            "dim_table": dim_table,
            "scd_type": scd_type,
            "natural_key": natural_key,
            "surrogate_key": surrogate_key,
            "duplicate": False,
            "changed_fields": changed_fields,
            "event_time": _iso(event_time),
            "processed_at": _iso(processed),
        }

        if scd_type in _TYPE1:
            row["effective_from"] = _iso(event_time)
            row["effective_to"] = OPEN_END
            updated = pipeline_bus.apply_scd_type1(dim_table, natural_key, row)
            digest = _digest({"dim_table": dim_table, "natural_key": natural_key,
                              "payload": self._digest_payload(row)})
            result.update({"action": "overwrite", "version": None, "row": updated,
                           "late_correction": False, "restated": False,
                           "effective_from": row["effective_from"],
                           "effective_to": row["effective_to"]})
            replay = self._write_manifest(
                dim_table=dim_table, natural_key=natural_key, surrogate_key=surrogate_key,
                version=None, event=enriched, changed_fields=changed_fields, digest=digest,
                disposition="applied", detail="Type1 覆盖当前快照", late_correction=False,
                restated=False, effective_from=row["effective_from"],
                effective_to=row["effective_to"], processed_at=processed,
            )
        elif scd_type in _TYPE2:
            version_row, late_correction, restated, detail = self._append_type2_version(
                config=config, event=enriched, dim_table=dim_table, natural_key=natural_key,
                surrogate_key=surrogate_key, row=row, event_time=event_time,
                processed=processed,
            )
            result.update({
                "action": "append_version",
                "version": version_row["version"],
                "row": version_row,
                "late_correction": late_correction,
                "restated": restated,
                "effective_from": version_row["effective_from"],
                "effective_to": version_row["effective_to"],
                "surrogate_key": version_row["surrogate_key"],
            })
            replay = self._write_manifest(
                dim_table=dim_table, natural_key=natural_key,
                surrogate_key=version_row["surrogate_key"], version=version_row["version"],
                event=enriched, changed_fields=changed_fields, digest=version_row["digest"],
                disposition="applied", detail=detail, late_correction=late_correction,
                restated=restated, effective_from=version_row["effective_from"],
                effective_to=version_row["effective_to"], processed_at=processed,
            )
            version_row["replay_id"] = replay["replay_id"]
        elif scd_type in _TYPE3:
            col_name = config.get("生效时间列") or "status"
            row["effective_from"] = _iso(event_time)
            row["effective_to"] = OPEN_END
            updated = pipeline_bus.apply_scd_type3(dim_table, natural_key, col_name, row)
            digest = _digest({"dim_table": dim_table, "natural_key": natural_key,
                              "payload": self._digest_payload(row)})
            result.update({"action": "add_previous_column", "version": None, "row": updated,
                           "late_correction": False, "restated": False,
                           "effective_from": row["effective_from"],
                           "effective_to": row["effective_to"]})
            replay = self._write_manifest(
                dim_table=dim_table, natural_key=natural_key, surrogate_key=surrogate_key,
                version=None, event=enriched, changed_fields=changed_fields, digest=digest,
                disposition="applied", detail=f"Type3 追加前值列 {col_name}",
                late_correction=False, restated=False,
                effective_from=row["effective_from"], effective_to=row["effective_to"],
                processed_at=processed,
            )
        else:
            result.update({"action": "skip", "version": None, "row": None,
                           "late_correction": False, "restated": False,
                           "reason": "unknown_scd_type"})
            replay = self._write_manifest(
                dim_table=dim_table, natural_key=natural_key, surrogate_key=surrogate_key,
                version=None, event=enriched, changed_fields=changed_fields, digest="",
                disposition="skipped", detail="未知SCD类型，未写入维度数据",
                late_correction=False, restated=False, effective_from=None,
                effective_to=None, processed_at=processed,
            )

        result["replay_id"] = replay["replay_id"]
        pipeline_bus.pipeline_metrics["scd_processed"] += 1
        pipeline_bus.write_audit(
            f"SCD{scd_type}", f"{dim_table}:{natural_key}",
            f"维度表{dim_table}应用{scd_type}策略 {result.get('action')}"
            f"{'（重复提交）' if result.get('duplicate') else ''}",
            "scd_handler", "已处理",
        )
        return result

    def _append_type2_version(
        self, *, config: dict[str, Any], event: dict[str, Any], dim_table: str,
        natural_key: str, surrogate_key: str, row: dict[str, Any], event_time: datetime,
        processed: datetime,
    ) -> tuple[dict[str, Any], bool, bool, str]:
        """追加一个 Type2 版本，返回 (版本行, 是否迟到, 是否重述, 处置说明)。"""
        # 直接操作该维度表、该自然键自己的版本链；别的维表不可能被碰到。
        history = pipeline_bus.scd_type2_store.setdefault(dim_table, {}).setdefault(
            natural_key, [])
        current = history[-1] if history else None
        current_from = _parse_dt(current["effective_from"]) if current else None

        late_correction = current_from is not None and event_time <= current_from
        applied_from = event_time
        detail = "按事件时间追加新版本"
        if late_correction:
            # 已经生效的区间不重述：更正从处理时刻起生效。
            applied_from = processed if processed > current_from else current_from + _EPSILON
            detail = (
                "迟到更正：事件时间早于当前版本生效时间，按不重述策略"
                f"自 {_iso(applied_from)} 生效，已生效区间保持不变"
            )

        if current is not None:
            current["effective_to"] = _iso(applied_from)
            current["is_current"] = False

        version = {
            **row,
            "dim_table": dim_table,
            "natural_key": natural_key,
            "version": len(history) + 1,
            "surrogate_key": f"{surrogate_key}_{dim_table}_{natural_key}_{len(history) + 1}",
            "effective_from": _iso(applied_from),
            "effective_to": OPEN_END,
            "is_current": True,
            "event_id": str(event.get("事件ID") or ""),
            "operation": str(event.get("操作类型") or ""),
            "event_time": _iso(event_time),
            "processed_at": _iso(processed),
            "changed_fields": list(event.get("字段差异") or []),
            "late_correction": late_correction,
            # 显式取舍：迟到更正不追溯改写已经交出去的区间，事实仍按原版本重算。
            "restated": False,
            "replay_id": "",
        }
        version["digest"] = _digest({
            "dim_table": dim_table,
            "natural_key": natural_key,
            "version": version["version"],
            "event_id": version["event_id"],
            "operation": version["operation"],
            "event_time": version["event_time"],
            "changed_fields": version["changed_fields"],
            "payload": self._digest_payload(version),
        })
        history.append(version)
        return version, late_correction, False, detail

    # ------------------------------------------------------------------
    # 回放清单
    # ------------------------------------------------------------------
    def _find_previous_disposition(self, dim_table: str, natural_key: str,
                                   event_id: str) -> dict[str, Any] | None:
        if not event_id:
            return None
        index_key = f"{dim_table}\u0000{natural_key}\u0000{event_id}"
        position = pipeline_bus.scd_replay_index.get(index_key)
        if position is None:
            return None
        entry = pipeline_bus.scd_replay_manifest[position]
        row = self._version_row(entry)
        return {
            "version": entry.get("version"),
            "surrogate_key": entry.get("surrogate_key"),
            "digest": entry.get("digest", ""),
            # 区间取版本链里的当前值：同一事件重复提交时，原版本可能已被后续
            # 变更关闭，回放清单也要展示它现在的真实区间。
            "effective_from": (row or {}).get("effective_from", entry.get("effective_from")),
            "effective_to": (row or {}).get("effective_to", entry.get("effective_to")),
            "row": row,
        }

    def _version_row(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        if entry.get("version") is None:
            return None
        return next(
            (dict(row) for row in pipeline_bus.get_scd_type2_history(
                entry.get("dim_table", ""), entry.get("natural_key", ""))
             if row.get("version") == entry.get("version")),
            None,
        )

    def _write_manifest(
        self, *, dim_table: str, natural_key: str, surrogate_key: str,
        version: int | None, event: dict[str, Any], changed_fields: list[dict[str, Any]],
        digest: str, disposition: str, detail: str, late_correction: bool,
        restated: bool, effective_from: str | None, effective_to: str | None,
        processed_at: datetime,
    ) -> dict[str, Any]:
        replay_id = f"rpl_{len(pipeline_bus.scd_replay_manifest) + 1:06d}"
        entry = {
            "replay_id": replay_id,
            "dim_table": dim_table,
            "natural_key": natural_key,
            "surrogate_key": surrogate_key,
            "version": version,
            "event_id": str(event.get("事件ID") or ""),
            "source_table": str(event.get("来源表") or ""),
            "source_dimension": str(event.get("来源维度") or dim_table),
            "operation": str(event.get("操作类型") or ""),
            "event_time": str(event.get("事件时间") or ""),
            "changed_fields": list(changed_fields),
            "digest": digest,
            "disposition": disposition,
            "detail": detail,
            "late_correction": late_correction,
            "restated": restated,
            "effective_from": effective_from,
            "effective_to": effective_to,
            "processed_at": _iso(processed_at),
        }
        pipeline_bus.scd_replay_manifest.append(entry)
        if entry["event_id"] and disposition != "duplicate":
            index_key = f"{dim_table}\u0000{natural_key}\u0000{entry['event_id']}"
            pipeline_bus.scd_replay_index[index_key] = len(pipeline_bus.scd_replay_manifest) - 1
        return entry

    def query_replay_manifest(self, dim_table: str | None = None,
                              natural_key: str | None = None,
                              disposition: str | None = None) -> list[dict[str, Any]]:
        """查询事件回放清单，可按维度表 / 自然键 / 处置结果过滤。"""
        entries = []
        for entry in pipeline_bus.scd_replay_manifest:
            if dim_table and entry.get("dim_table") != dim_table:
                continue
            if natural_key and entry.get("natural_key") != natural_key:
                continue
            if disposition and entry.get("disposition") != disposition:
                continue
            entries.append(dict(entry))
        return entries

    def reconcile_manifest(self, dim_table: str | None = None) -> dict[str, Any]:
        """核对回放清单与维度历史清单的版本是否一致。

        以 (维度表, 自然键, 版本, 摘要) 为唯一版本标识：每个实际写入的 Type2
        版本必须在回放清单里有且只有一条 applied 记录，反之亦然。
        """
        manifest_versions: dict[tuple[str, str, int], dict[str, Any]] = {}
        manifest_problems: list[dict[str, Any]] = []
        for entry in pipeline_bus.scd_replay_manifest:
            if entry.get("disposition") != "applied" or entry.get("version") is None:
                continue
            if dim_table and entry.get("dim_table") != dim_table:
                continue
            key = (entry["dim_table"], entry["natural_key"], int(entry["version"]))
            if key in manifest_versions:
                manifest_problems.append({
                    "type": "manifest_duplicate_version",
                    "dim_table": key[0], "natural_key": key[1], "version": key[2],
                    "replay_ids": [manifest_versions[key]["replay_id"], entry["replay_id"]],
                })
                continue
            manifest_versions[key] = entry

        history_versions: dict[tuple[str, str, int], dict[str, Any]] = {}
        for table, natural_key, row in pipeline_bus.iter_scd_type2():
            if dim_table and table != dim_table:
                continue
            key = (table, natural_key, int(row.get("version", 0)))
            history_versions[key] = row

        missing_in_manifest = sorted(
            key for key in history_versions if key not in manifest_versions)
        missing_in_history = sorted(
            key for key in manifest_versions if key not in history_versions)
        digest_mismatches = []
        for key in sorted(set(history_versions) & set(manifest_versions)):
            if history_versions[key].get("digest") != manifest_versions[key].get("digest"):
                digest_mismatches.append({
                    "dim_table": key[0], "natural_key": key[1], "version": key[2],
                    "history_digest": history_versions[key].get("digest"),
                    "manifest_digest": manifest_versions[key].get("digest"),
                })
        mismatches = (
            [{"type": "version_missing_in_manifest", "dim_table": k[0],
              "natural_key": k[1], "version": k[2]} for k in missing_in_manifest]
            + [{"type": "version_missing_in_history", "dim_table": k[0],
                "natural_key": k[1], "version": k[2]} for k in missing_in_history]
            + [dict(item, type="digest_mismatch") for item in digest_mismatches]
            + manifest_problems
        )
        return {
            "dim_table": dim_table or "*",
            "consistent": not mismatches,
            "manifest_version_count": len(manifest_versions),
            "history_version_count": len(history_versions),
            "manifest_entries": len([
                entry for entry in pipeline_bus.scd_replay_manifest
                if not dim_table or entry.get("dim_table") == dim_table
            ]),
            "mismatches": mismatches,
            "checked_at": _iso(datetime.now()),
        }

    # ------------------------------------------------------------------
    # 查询：历史 / 当前 / 时点重算
    # ------------------------------------------------------------------
    def query_dimension_history(self, dim_table: str,
                                natural_key: str | None = None) -> list[dict[str, Any]]:
        """返回指定维度表的 Type2 历史版本，可再按自然键收窄。"""
        table_store = pipeline_bus.scd_type2_store.get(dim_table, {})
        if natural_key is not None:
            return [dict(row) for row in table_store.get(natural_key, [])]
        return [
            dict(row)
            for history in table_store.values()
            for row in history
        ]

    def pipeline_tables(self) -> list[str]:
        """已经产生 Type2 版本的维度表。"""
        return list(pipeline_bus.scd_type2_store)

    def dimension_keys(self, dim_table: str) -> list[str]:
        """指定维度表下已有版本的自然键。"""
        return list(pipeline_bus.scd_type2_store.get(dim_table, {}))

    def current_version(self, dim_table: str, natural_key: str) -> dict[str, Any] | None:
        """返回指定维度键当前最新的 Type2 版本。"""
        history = pipeline_bus.get_scd_type2_history(dim_table, natural_key)
        return history[-1] if history else None

    def query_as_of(self, dim_table: str, natural_key: str, as_of: Any) -> dict[str, Any] | None:
        """按指定时点重算当时生效的 Type2 版本。

        每个时点最多命中一条：区间为左闭右开 [effective_from, effective_to)。
        """
        moment = _parse_dt(as_of)
        if moment is None:
            return None
        for row in pipeline_bus.get_scd_type2_history(dim_table, natural_key):
            start = _parse_dt(row.get("effective_from"))
            end = _parse_dt(row.get("effective_to"))
            if start is None:
                continue
            if start <= moment and (end is None or moment < end):
                return dict(row)
        return None

    def query_dimension_snapshot(self, dim_table: str, scd_type: str) -> dict[str, Any]:
        """查询维度表的SCD快照（Type2 只汇总该维表自己的版本链）。"""
        if scd_type in _TYPE1:
            keys = pipeline_bus.scd_type1_store.get(dim_table, {})
            return {"type": "Type1", "dim_table": dim_table, "count": len(keys),
                    "snapshot": {key: dict(row) for key, row in keys.items()}}
        if scd_type in _TYPE2:
            history = pipeline_bus.scd_type2_store.get(dim_table, {})
            current_only = {key: dict(rows[-1]) for key, rows in history.items() if rows}
            return {
                "type": "Type2",
                "dim_table": dim_table,
                "current_count": len(current_only),
                "total_history": sum(len(rows) for rows in history.values()),
                "snapshot": current_only,
            }
        if scd_type in _TYPE3:
            keys = pipeline_bus.scd_type3_store.get(dim_table, {})
            return {"type": "Type3", "dim_table": dim_table, "count": len(keys),
                    "snapshot": {key: dict(row) for key, row in keys.items()}}
        return {"type": scd_type, "dim_table": dim_table, "count": 0, "snapshot": {}}

    def validate_intervals(self, dim_table: str | None = None) -> dict[str, Any]:
        """校验版本区间：无重叠、无重复起点、相邻区间首尾相接、最多一条当前版本。"""
        issues: list[dict[str, Any]] = []
        tables = [dim_table] if dim_table else list(pipeline_bus.scd_type2_store)
        for table in tables:
            for natural_key, history in pipeline_bus.scd_type2_store.get(table, {}).items():
                previous_end: datetime | None = None
                current_versions = 0
                for row in history:
                    if row.get("is_current"):
                        current_versions += 1
                    start = _parse_dt(row.get("effective_from"))
                    end = _parse_dt(row.get("effective_to"))
                    if start is None or end is None:
                        issues.append({"type": "invalid_interval", "dim_table": table,
                                       "natural_key": natural_key, "version": row.get("version")})
                        continue
                    if end <= start:
                        issues.append({"type": "empty_interval", "dim_table": table,
                                       "natural_key": natural_key, "version": row.get("version")})
                    if previous_end is not None and start < previous_end:
                        issues.append({"type": "overlap", "dim_table": table,
                                       "natural_key": natural_key, "version": row.get("version")})
                    elif previous_end is not None and start > previous_end:
                        issues.append({"type": "gap", "dim_table": table,
                                       "natural_key": natural_key, "version": row.get("version")})
                    previous_end = end
                if current_versions != 1:
                    issues.append({"type": "current_version_count", "dim_table": table,
                                   "natural_key": natural_key, "count": current_versions})
        return {"dim_table": dim_table or "*", "valid": not issues, "issues": issues}

    def verify_replay(self, dim_table: str, natural_key: str | None = None) -> dict[str, Any]:
        """重复读取历史与当前版本，确认结果可稳定复算，并核对清单归属。"""
        keys = [natural_key] if natural_key else sorted(
            pipeline_bus.scd_type2_store.get(dim_table, {}))
        checks: list[dict[str, Any]] = []
        for key in keys:
            first = pipeline_bus.get_scd_type2_history(dim_table, key)
            second = pipeline_bus.get_scd_type2_history(dim_table, key)
            current_first = self.current_version(dim_table, key)
            current_second = self.current_version(dim_table, key)
            recomputed = [
                self.query_as_of(dim_table, key, row.get("effective_from"))
                for row in first
            ]
            stable = (
                [row.get("digest") for row in first] == [row.get("digest") for row in second]
                and (current_first or {}).get("digest") == (current_second or {}).get("digest")
                and all(hit is not None and hit.get("version") == row.get("version")
                        for hit, row in zip(recomputed, first))
            )
            manifest = self.query_replay_manifest(dim_table, key)
            applied = [e for e in manifest if e.get("disposition") == "applied"]
            checks.append({
                "natural_key": key,
                "versions": len(first),
                "current_version": (current_first or {}).get("version"),
                "stable": stable,
                "manifest_versions": sorted(
                    int(e["version"]) for e in applied if e.get("version") is not None),
                "history_versions": [int(row.get("version", 0)) for row in first],
            })
        reconciliation = self.reconcile_manifest(dim_table)
        return {
            "dim_table": dim_table,
            "checks": checks,
            "stable": all(check["stable"] for check in checks),
            "reconciliation": reconciliation,
            "checked_at": _iso(datetime.now()),
        }

    # ------------------------------------------------------------------
    # 键与镜像辅助
    # ------------------------------------------------------------------
    def _natural_keys(self, config: dict[str, Any] | None) -> list[str]:
        raw = str((config or {}).get("自然键") or "id")
        keys = [part.strip() for part in raw.split(",") if part.strip()]
        return keys or ["id"]

    def _natural_key_values(self, event: dict[str, Any], natural_keys: list[str]) -> list[str]:
        after = _json_dict(event.get("变更后镜像"))
        before = _json_dict(event.get("变更前镜像"))
        values: list[str] = []
        for key in natural_keys:
            if key in after:
                values.append(str(after[key]))
            elif key in before:
                values.append(str(before[key]))
            elif key in event:
                values.append(str(event[key]))
            else:
                values.append("")
        return values

    def _build_dim_key(self, event: dict[str, Any], natural_keys: list[str]) -> str:
        values = self._natural_key_values(event, natural_keys)
        if all(value == "" for value in values):
            return str(event.get("自然键") or event.get("事件ID") or "unknown")
        return "|".join(values)

    def _table_from_source(self, source_table: str) -> str:
        return source_table.split(".")[-1] if source_table else ""

    def _extract_dim_row(self, event: dict[str, Any], dim_table: str) -> dict[str, Any]:
        after = _json_dict(event.get("变更后镜像"))
        row = dict(after)
        row["_source_table"] = dim_table
        row["_source_dimension"] = str(event.get("来源维度") or dim_table)
        row["_event_id"] = event.get("事件ID", "")
        row["_operation"] = event.get("操作类型", "")
        return row

    def _event_time(self, event: dict[str, Any], config: dict[str, Any],
                    processed: datetime) -> datetime:
        for candidate in (event.get("事件时间"), event.get("生效时间"), event.get("effective_time")):
            parsed = _parse_dt(candidate)
            if parsed is not None:
                return parsed
        effective_col = str(config.get("生效时间列") or "")
        after = _json_dict(event.get("变更后镜像"))
        if effective_col and effective_col in after:
            parsed = _parse_dt(after[effective_col])
            if parsed is not None:
                return parsed
        return processed

    def _digest_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        volatile = {
            "effective_to", "is_current", "replay_id", "digest", "processed_at",
            "effective_from", "version", "surrogate_key", "late_correction", "restated",
        }
        return {key: value for key, value in row.items() if key not in volatile}

    def detect_scd_conflicts(self, natural_keys: list[str],
                             event_batch: list[dict[str, Any]],
                             dim_table: str | None = None) -> list[dict[str, Any]]:
        """检测同一维度表、同一自然键在批次内的冲突更新。"""
        key_counts: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for evt in event_batch:
            table = dim_table or str(evt.get("来源维度") or self._table_from_source(
                str(evt.get("来源表") or "")))
            key = self._build_dim_key(evt, natural_keys)
            key_counts.setdefault((table, key), []).append(evt)
        conflicts = []
        for (table, key), evts in key_counts.items():
            if len(evts) > 1:
                conflicts.append({
                    "dim_table": table,
                    "natural_key": key,
                    "update_count": len(evts),
                    "resolves_to": "last_write_wins",
                    "event_ids": [e.get("事件ID", "") for e in evts],
                })
        return conflicts

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用配置":
            dim_table = entry.get("维度表", "")
            pipeline_bus.write_audit(
                "SCD启用", dim_table,
                f"启用{dim_table}的{entry.get('SCD类型','Type2')}SCD策略",
                "scd_handler", "已启用"
            )
        return entry, msg
