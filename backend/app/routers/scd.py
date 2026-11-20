"""缓慢变化维专用接口：维度变更入口、Type2 历史查询、回放清单核对。

通用资源路由仍负责 SCD 配置的增删查改；这里只补通用接口表达不了的
“维度版本链”语义：
  - 变更事件入口按来源维度提交自然键与字段差异
  - 按维度表读取 Type2 历史 / 当前版本 / 指定时点重算
  - 事件回放清单与维度历史清单的版本核对
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.change_event import ChangeEventService
from app.services.scd_handler import ScdHandlerService

router = APIRouter(prefix="/api/scd", tags=["缓慢变化维"])


class DimensionEvent(BaseModel):
    """变更事件入口载荷：来源维度、自然键、字段差异缺一不可。"""

    event_id: str = ""
    source_dimension: str = ""
    source_table: str = ""
    natural_key: str = ""
    operation: str = "UPDATE"
    before: dict[str, Any] = Field(default_factory=dict)
    after: dict[str, Any] = Field(default_factory=dict)
    changed_fields: list[dict[str, Any]] | None = None
    event_time: str = ""
    processed_at: str = ""


class DimensionEventBatch(BaseModel):
    events: list[DimensionEvent] = Field(default_factory=list)
    dim_table: str = ""
    natural_key: str = ""
    scd_type: str = ""


def _scd() -> ScdHandlerService:
    service = service_for("scd_handler")
    if not isinstance(service, ScdHandlerService):
        raise HTTPException(status_code=500, detail="缓慢变化维服务未注册")
    return service


def _to_event(payload: DimensionEvent) -> dict[str, Any]:
    event: dict[str, Any] = {
        "事件ID": payload.event_id,
        "来源维度": payload.source_dimension,
        "来源表": payload.source_table,
        "操作类型": payload.operation,
        "变更前镜像": payload.before,
        "变更后镜像": payload.after,
        "事件时间": payload.event_time,
    }
    if payload.natural_key:
        event["自然键"] = payload.natural_key
    if payload.changed_fields is not None:
        event["字段差异"] = payload.changed_fields
    if payload.processed_at:
        # 回放/补数时可以显式给出处理时刻，让重算结果可复现。
        event["_processed_at"] = payload.processed_at
    return event


@router.post("/events")
def submit_dimension_events(batch: DimensionEventBatch) -> dict[str, Any]:
    """变更事件入口：为每个维度提交事件，按维度表分别落库为 Type2 历史版本。"""
    service = _scd()
    results: list[dict[str, Any]] = []
    for payload in batch.events:
        event = _to_event(payload)
        override: dict[str, Any] = {}
        if batch.dim_table:
            override["维度表"] = batch.dim_table
        if batch.natural_key:
            override["自然键"] = batch.natural_key
        if batch.scd_type:
            override["SCD类型"] = batch.scd_type
        outcome = service.handle_change_events(
            [event], override=override or None, apply=True)
        results.extend(outcome["results"])
    return {
        "ok": all(item.get("action") not in ("unresolved",) for item in results),
        "results": results,
        "manifest": service.query_replay_manifest(dim_table=batch.dim_table or None),
    }


@router.post("/change-event/{entry_id}")
def dispatch_change_event(
    entry_id: int,
    dim_table: str = Query(default=""),
    natural_key: str = Query(default=""),
    scd_type: str = Query(default=""),
    processing_time: str = Query(default="", description="回放/补数时指定处理时刻，便于复算"),
) -> dict[str, Any]:
    """从变更事件入口把一条已登记事件交给缓慢变化维。"""
    change_service = service_for("change_event")
    if not isinstance(change_service, ChangeEventService):
        raise HTTPException(status_code=500, detail="变更事件服务未注册")
    entry = change_service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"变更记录 {entry_id} 不存在或已归档")
    return change_service.dispatch_to_scd(
        [str(entry.get("事件ID", ""))],
        dimension=dim_table,
        natural_key=natural_key,
        scd_type=scd_type,
        processing_time=processing_time,
    )


@router.get("/history")
def dimension_history(dim_table: str, natural_key: str | None = None) -> dict[str, Any]:
    """Type2 历史版本数据（维度历史清单）。"""
    service = _scd()
    versions = service.query_dimension_history(dim_table, natural_key)
    return {"dim_table": dim_table, "natural_key": natural_key,
            "items": versions, "total": len(versions)}


@router.get("/history/summary")
def history_summary(dim_table: str | None = None) -> dict[str, Any]:
    """维度历史清单的版本汇总：按维度表、自然键列出全部版本与当前版本。"""
    service = _scd()
    tables = [dim_table] if dim_table else sorted(service.pipeline_tables())
    summary: dict[str, Any] = {}
    for table in tables:
        keys: dict[str, Any] = {}
        for natural_key in sorted(service.dimension_keys(table)):
            history = service.query_dimension_history(table, natural_key)
            keys[natural_key] = {
                "versions": [version.get("version") for version in history],
                "current_version": history[-1].get("version") if history else None,
                "digests": [version.get("digest") for version in history],
            }
        summary[table] = {"natural_keys": keys, "total_versions": sum(
            len(item["versions"]) for item in keys.values())}
    return {"dim_table": dim_table or "*", "tables": summary}


@router.get("/current")
def current_versions(dim_table: str, natural_key: str | None = None) -> dict[str, Any]:
    """维度表当前版本；给 natural_key 时只取该键。"""
    service = _scd()
    if natural_key:
        version = service.current_version(dim_table, natural_key)
        return {"dim_table": dim_table, "natural_key": natural_key, "item": version}
    snapshot = service.query_dimension_snapshot(dim_table, "Type2")
    return {"dim_table": dim_table, "items": snapshot["snapshot"],
            "current_count": snapshot["current_count"],
            "total_history": snapshot["total_history"]}


@router.get("/as-of")
def as_of(dim_table: str, natural_key: str, moment: str) -> dict[str, Any]:
    """按当时口径重算指定时点生效的版本。"""
    service = _scd()
    return {"dim_table": dim_table, "natural_key": natural_key, "as_of": moment,
            "item": service.query_as_of(dim_table, natural_key, moment)}


@router.get("/manifest")
def replay_manifest(dim_table: str | None = None, natural_key: str | None = None,
                    disposition: str | None = None) -> dict[str, Any]:
    """事件回放清单：每条变更事件的处置结果与归属。"""
    service = _scd()
    entries = service.query_replay_manifest(dim_table, natural_key, disposition)
    return {"items": entries, "total": len(entries)}


@router.get("/reconcile")
def reconcile(dim_table: str | None = None) -> dict[str, Any]:
    """核对回放清单与维度历史清单的版本是否一一对应。"""
    return _scd().reconcile_manifest(dim_table)


@router.get("/validate")
def validate_intervals(dim_table: str | None = None) -> dict[str, Any]:
    """校验每个维度键的有效区间互补重叠、无空洞。"""
    return _scd().validate_intervals(dim_table)


@router.get("/verify")
def verify(dim_table: str, natural_key: str | None = None) -> dict[str, Any]:
    """重复刷新历史查询与当前版本，确认可稳定复算并核对清单归属。"""
    return _scd().verify_replay(dim_table, natural_key)
