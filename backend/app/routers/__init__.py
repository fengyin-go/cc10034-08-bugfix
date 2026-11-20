"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派；缓慢变化维另有版本链接口。"""
from __future__ import annotations

from app.routers import resource, scd

ROUTERS = [scd, resource]
