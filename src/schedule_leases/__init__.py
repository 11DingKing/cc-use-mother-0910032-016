"""排班资源暂占恢复服务。

- ``store``：基于 SQLite 的原子状态机存储（仅标准库）。
- ``service``：租约业务逻辑（暂占 / 续约 / 确认 / 取消 / 失效 / 清理）。
- ``server``：HTTP API。
- ``cli``：管理命令（python -m schedule_leases ...）。
"""
from __future__ import annotations

from .service import LeaseService, ServiceError
from .store import LeaseStore

__all__ = ["LeaseService", "LeaseStore", "ServiceError"]
