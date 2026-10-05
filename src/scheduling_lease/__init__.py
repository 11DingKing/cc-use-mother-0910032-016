"""排班资源暂占恢复服务。

核心不变式（见 domain/contract.json）：

* 组合资源租约：多个讲解员/场地作为一个原子组占用，要么全部占住要么全部不占；
* 确认版本比较：凭续约令牌与乐观版本号把暂占转换为正式预约；
* 并发清理互斥：多个清理进程并发扫描时，每个租约只可能被释放一次；
* 孤儿占用恢复：超时扫描与管理员回收都能安全终结异常遗留的暂占。
"""
from __future__ import annotations

from .service import LeaseService
from .store import ConflictError, InvalidResourceError, Store

__all__ = ["LeaseService", "Store", "ConflictError", "InvalidResourceError"]
