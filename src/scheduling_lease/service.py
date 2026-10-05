"""业务服务层：租约记录的组合资源暂占、续约、确认与回收。

时间统一使用带时区的 UTC datetime；存储层负责并发互斥，本层负责
令牌/版本/幂等语义与"部分资源失效"的处理。
"""
from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .store import ConflictError, InvalidResourceError, Store, utcnow

DEFAULT_TTL_SECONDS = 15 * 60


def parse_dt(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def hash_token(token: str) -> str:
    """令牌只以 SHA-256 摘要落库；接口返回原文一次，后续全凭摘要比对。"""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class LeaseError(Exception):
    """业务错误。code 供 API 映射为状态码。"""

    def __init__(self, code: str, message: str, extra: dict | None = None) -> None:
        self.code = code
        self.extra = extra or {}
        super().__init__(message)


class LeaseService:
    def __init__(
        self,
        store: Store | str | Path,
        *,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.store = store if isinstance(store, Store) else Store(store)
        self.clock = clock

    # ---------------------------------------------------------------- helpers

    def _normalize_resources(self, resources: list[dict]) -> list[dict]:
        if not resources:
            raise LeaseError("empty_resources", "组合资源不能为空")
        normalized = []
        seen: set[tuple] = set()
        for r in resources:
            try:
                item = {
                    "resource_type": r["resource_type"],
                    "resource_ref": r["resource_ref"],
                    "slot_start": parse_dt(r["slot_start"]),
                    "slot_end": parse_dt(r["slot_end"]),
                }
            except (KeyError, TypeError, ValueError) as exc:
                raise LeaseError("bad_resource", f"资源描述无效：{r}") from exc
            if item["slot_end"] <= item["slot_start"]:
                raise LeaseError("bad_slot", "时段结束必须晚于开始")
            key = (item["resource_type"], item["resource_ref"], item["slot_start"])
            if key in seen:
                raise LeaseError("duplicate_resource", "同一组合内资源不能重复")
            seen.add(key)
            normalized.append(item)
        return normalized

    def _lease_view(self, row: dict, *, token: str | None = None) -> dict:
        view = {
            "lease_id": row["id"],
            "school_id": row["school_id"],
            "contact": row["contact"],
            "source": row["source"],
            "created_by": row["created_by"],
            "status": row["status"],
            "version": row["version"],
            "created_at": row["created_at"],
            "expires_at": row["expires_at"],
            "released_at": row["released_at"],
            "released_reason": row["released_reason"],
            "released_by": row["released_by"],
            "booking_id": row["booking_id"],
            "resources": row["resources"],
        }
        if token is not None:
            view["token"] = token
        return view

    # -------------------------------------------------------------------- hold

    def hold(
        self,
        *,
        school_id: str,
        resources: list[dict],
        contact: str = "",
        source: str = "api",
        created_by: str = "",
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ) -> dict:
        """暂占一组讲解员+场地。成功返回含续约令牌的租约（令牌只返回这一次）。"""
        if ttl_seconds <= 0:
            raise LeaseError("bad_ttl", "ttl_seconds 必须为正")
        resources = self._normalize_resources(resources)
        now = self.clock()
        lease_id = "lease-" + uuid.uuid4().hex
        token = secrets.token_urlsafe(32)
        try:
            self.store.insert_lease(
                lease_id=lease_id,
                school_id=school_id,
                contact=contact,
                source=source,
                created_by=created_by,
                created_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
                token_hash=hash_token(token),
                resources=resources,
            )
        except InvalidResourceError as exc:
            raise LeaseError("invalid_resource", str(exc), {"invalid": exc.invalid}) from exc
        except ConflictError as exc:
            raise LeaseError("conflict", str(exc), {"conflicts": exc.conflicts}) from exc
        row = self.store.get_lease(lease_id)
        return self._lease_view(row, token=token)

    # ------------------------------------------------------------------ renew

    def renew(
        self,
        lease_id: str,
        token: str,
        *,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        resources: list[dict] | None = None,
    ) -> dict:
        """续约并可调整组合资源；版本号自增，旧版本号之后无法用于确认。"""
        if ttl_seconds <= 0:
            raise LeaseError("bad_ttl", "ttl_seconds 必须为正")
        new_resources = (
            self._normalize_resources(resources) if resources is not None else None
        )
        now = self.clock()
        status, version = self.store.renew(
            lease_id, hash_token(token),
            now + timedelta(seconds=ttl_seconds), new_resources,
        )
        if status == "ok":
            return self._lease_view(self.store.get_lease(lease_id))
        message = {
            "not_found": "租约不存在",
            "not_held": "租约已终结，无法续约",
            "bad_token": "续约令牌无效",
            "conflict": "调整后的组合资源存在冲突",
            "invalid_resource": "组合中包含已失效资源",
        }[status]
        raise LeaseError(status, message, {"version": version} if status == "conflict" else None)

    # ----------------------------------------------------------------- confirm

    def confirm(
        self,
        lease_id: str,
        token: str,
        expected_version: int,
        *,
        booking_id: str | None = None,
    ) -> dict:
        """凭令牌+版本号把暂占转为正式预约。

        * 版本不符（期间被续约过）返回 version_conflict；
        * 对同一租约重复确认，原样返回首次预约，不产生第二条预约 —— 幂等；
        * 用同一 booking_id 重复请求也返回同一条预约。
        """
        booking_id = booking_id or "booking-" + uuid.uuid4().hex
        status, info = self.store.confirm(
            lease_id, hash_token(token), int(expected_version),
            booking_id, self.clock(),
        )
        if status in ("confirmed", "already_confirmed"):
            assert info is not None
            result = dict(info)
            if isinstance(result["resources"], str):
                result["resources"] = json.loads(result["resources"])
            result["idempotent"] = status == "already_confirmed"
            return result
        message = {
            "not_found": "租约不存在",
            "not_held": "租约已释放或过期，无法确认",
            "bad_token": "续约令牌无效",
            "version_conflict": "版本已变化（期间发生续约），请按最新版本重新确认",
        }[status]
        extra = info if status == "version_conflict" else None
        raise LeaseError(status, message, extra)

    # ------------------------------------------------------------------ cancel

    def cancel(self, lease_id: str, token: str | None = None, *, actor: str) -> dict:
        """主动取消：持有人带令牌；管理员 token 传 None 强制回收。重复取消幂等。"""
        token_hash = hash_token(token) if token is not None else None
        status, info = self.store.cancel(lease_id, token_hash, self.clock(), actor)
        if status == "cancelled":
            assert info is not None
            return {"lease_id": lease_id, "status": info["status"],
                    "idempotent": False, "reason": info["reason"]}
        if status and status.startswith("already_"):
            return {"lease_id": lease_id,
                    "status": status.removeprefix("already_").upper(),
                    "idempotent": True}
        message = {
            "not_found": "租约不存在",
            "bad_token": "续约令牌无效",
        }.get(status, status)
        raise LeaseError(status, message)

    # ----------------------------------------------------------- housekeeping

    def sweep_expired(self, worker_id: str, *, batch: int = 100) -> dict:
        """超时扫描一轮。多进程并发调用安全：每个租约只被释放一次。"""
        claimed = self.store.claim_expired(self.clock(), worker_id, limit=batch)
        return {"worker_id": worker_id, "released": claimed, "count": len(claimed)}

    def invalidate_resources(
        self, refs: list[dict], *, reason: str, actor: str,
    ) -> dict:
        """部分资源失效（讲解员请假/场地维修）。

        * 幂等登记失效资源（重复登记不产生副作用）；
        * 同事务内释放所有包含失效资源的 HELD 租约（条件更新认领，
          与清理进程并发也不会重复释放）；
        * 此后新暂占/续约若包含失效资源会被拒。
        """
        normalized = self._normalize_refs(refs)
        released = self.store.mark_invalid_and_release(
            normalized, reason=reason, actor=actor, now=self.clock(),
        )
        return {"invalidated": normalized, "released_leases": released,
                "count": len(released)}

    @staticmethod
    def _normalize_refs(refs: list[dict]) -> list[dict]:
        out = []
        seen: set[tuple] = set()
        for r in refs:
            key = (r["resource_type"], r["resource_ref"])
            if key not in seen:
                seen.add(key)
                out.append({"resource_type": r["resource_type"],
                            "resource_ref": r["resource_ref"]})
        return out

    def reclaim_orphans(
        self,
        worker_id: str,
        *,
        lease_ids: list[str] | None = None,
        grace_seconds: int = 0,
    ) -> dict:
        """回收孤儿记录：指定 id 批量回收，或回收已过期并超过宽限期的暂占。

        与超时扫描互斥；对非 HELD 记录返回 claimed=false，不报错 —— 幂等。
        """
        older_than = self.clock() - timedelta(seconds=grace_seconds)
        results = self.store.reclaim_orphans(
            self.clock(), worker_id,
            older_than=None if lease_ids is not None else older_than,
            lease_ids=lease_ids,
        )
        claimed = [r["id"] for r in results if r["claimed"]]
        return {"worker_id": worker_id, "results": results,
                "released": claimed, "count": len(claimed)}

    # ------------------------------------------------------------------ query

    def get_lease(self, lease_id: str) -> dict:
        row = self.store.get_lease(lease_id)
        if row is None:
            raise LeaseError("not_found", "租约不存在")
        return self._lease_view(row)

    def list_leases(self, status: str | None = None) -> list[dict]:
        return [self._lease_view(r) for r in self.store.list_leases(status=status)]

    def get_booking(self, booking_id: str) -> dict:
        info = self.store.get_booking(booking_id)
        if info is None:
            raise LeaseError("not_found", "预约不存在")
        if isinstance(info["resources"], str):
            info["resources"] = json.loads(info["resources"])
        return info
