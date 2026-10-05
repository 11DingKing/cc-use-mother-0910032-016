"""租约业务逻辑：暂占 / 续约 / 确认 / 取消 / 失效 / 超时清理 / 管理回收。

所有写操作都在单条 ``BEGIN IMMEDIATE`` 事务内完成"检查 + 条件更新"，
因此对超时扫描、主动取消、资源失效和重复确认都是幂等的；多个清理
进程通过认领令牌（reclaim_token）互斥，不会重复释放同一条占用。
"""
from __future__ import annotations

import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable

from .store import (
    STATE_CONFIRMED,
    STATE_EXPIRED,
    STATE_HELD,
    STATE_RELEASED,
    LeaseStore,
    parse_iso,
    to_iso,
    utcnow,
)

DEFAULT_TTL_SECONDS = 15 * 60
SWEEP_BATCH = 200


class ServiceError(Exception):
    """业务错误，``code`` 稳定可用于 API 与测试。"""

    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def _normalize_resources(resources: list[dict[str, str]]) -> list[dict[str, str]]:
    if not resources:
        raise ServiceError("empty_resources", "组合资源不能为空", 400)
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in resources:
        try:
            rtype = str(item["resource_type"]).strip()
            rid = str(item["resource_id"]).strip()
            slot = str(item["slot"]).strip()
        except (KeyError, TypeError):
            raise ServiceError("bad_resource", "资源需包含 resource_type/resource_id/slot", 400)
        if not rtype or not rid or not slot:
            raise ServiceError("bad_resource", "资源字段不能为空", 400)
        key = (rtype, rid, slot)
        if key in seen:
            raise ServiceError("dup_resource", f"资源重复：{key}", 400)
        seen.add(key)
        normalized.append(
            {"resource_type": rtype, "resource_id": rid, "slot": slot}
        )
    return normalized


class LeaseService:
    def __init__(
        self,
        store: LeaseStore,
        *,
        clock: Callable[[], datetime] = utcnow,
        default_ttl: int = DEFAULT_TTL_SECONDS,
    ) -> None:
        self.store = store
        self.clock = clock
        self.default_ttl = default_ttl

    def _now(self) -> datetime:
        return self.clock()

    # ── 暂占 ──────────────────────────────────────────────────────────
    def hold(
        self,
        *,
        holder: str,
        holder_kind: str,
        resources: list[dict[str, str]],
        ttl_seconds: int | None = None,
        expected_capacity_version: int = 0,
    ) -> dict[str, Any]:
        """为持有人暂占一组组合资源，返回租约（含续约令牌与版本）。"""
        holder = (holder or "").strip()
        if not holder:
            raise ServiceError("bad_holder", "持有人不能为空", 400)
        resources = _normalize_resources(resources)
        ttl = ttl_seconds or self.default_ttl
        if ttl <= 0:
            raise ServiceError("bad_ttl", "ttl_seconds 必须为正", 400)
        now = self._now()
        lease_id = uuid.uuid4().hex
        token = secrets.token_urlsafe(24)
        with self.store.tx() as conn:
            conflicts = self.store.active_slots_for(conn, resources)
            if conflicts:
                raise ServiceError(
                    "slot_busy",
                    "部分资源槽位已被暂占或已排定",
                    409,
                )
            try:
                row = self.store.insert(
                    conn,
                    lease_id=lease_id,
                    holder=holder,
                    holder_kind=holder_kind or "unknown",
                    resources=resources,
                    renew_token=token,
                    created_at=to_iso(now),
                    expires_at=to_iso(now + timedelta(seconds=ttl)),
                    expected_capacity_version=int(expected_capacity_version),
                )
            except sqlite3.IntegrityError as exc:
                raise ServiceError("slot_busy", "资源竞争，请重试", 409) from exc
            self.store.append_event(
                conn, lease_id, "HOLD", holder,
                {"ttl_seconds": ttl, "resources": resources},
            )
        assert row is not None
        return self.detail(lease_id)

    # ── 续约 ──────────────────────────────────────────────────────────
    def renew(self, lease_id: str, token: str, ttl_seconds: int | None = None) -> dict[str, Any]:
        """凭续约令牌延长到期时间。每次续约 version + 1（使旧确认请求失效）。"""
        ttl = ttl_seconds or self.default_ttl
        if ttl <= 0:
            raise ServiceError("bad_ttl", "ttl_seconds 必须为正", 400)
        new_expiry = to_iso(self._now() + timedelta(seconds=ttl))
        with self.store.tx() as conn:
            ok, reason = self.store.cas_renew(conn, lease_id, token, new_expiry)
            if not ok:
                raise self._error(reason)
            self.store.append_event(
                conn, lease_id, "RENEW", lease_id, {"expires_at": new_expiry}
            )
        return self.detail(lease_id)

    # ── 确认（比较版本 → 正式预约）────────────────────────────────────
    def confirm(
        self,
        lease_id: str,
        *,
        token: str,
        expected_version: int,
        confirmation_id: str | None = None,
    ) -> dict[str, Any]:
        """确认暂占并转换为正式预约。

        - 比较 ``expected_version`` 与租约当前 version：学校看到容量后
          若客服续过约/改过资源，版本不一致即拒绝（乐观锁）。
        - ``confirmation_id`` 是调用方幂等键：网络重试重复确认同键
          返回同一结果，不会二次转换。
        """
        cid = confirmation_id or uuid.uuid4().hex
        with self.store.tx() as conn:
            lease = self.store.get(conn, lease_id)
            if lease is None:
                raise ServiceError("not_found", "租约不存在", 404)

            # 幂等：同一确认键重复提交。
            existing = self.store.get_by_confirmation(conn, cid)
            if existing is not None:
                if existing["lease_id"] != lease_id:
                    raise ServiceError(
                        "confirmation_id_in_use", "确认单号已被其他租约使用", 409
                    )
                if existing["state"] == STATE_CONFIRMED:
                    return self._detail_conn(conn, existing, idempotent=True)

            ok, reason = self.store.cas_confirm(
                conn,
                lease_id,
                token=token,
                expected_version=int(expected_version),
                confirmation_id=cid,
                confirmed_at=to_iso(self._now()),
            )
            if not ok:
                raise self._error(reason)
            self.store.append_event(
                conn, lease_id, "CONFIRM", lease["holder"],
                {"confirmation_id": cid, "expected_version": int(expected_version)},
            )
        return self.detail(lease_id)

    # ── 主动取消 ──────────────────────────────────────────────────────
    def cancel(self, lease_id: str, token: str, *, reason: str = "holder_cancel") -> dict[str, Any]:
        """持有人凭令牌取消；重复取消、对已终结记录取消均幂等成功。"""
        return self._release(
            lease_id,
            token=token,
            new_state=STATE_RELEASED,
            reason=reason,
            actor="holder",
        )

    # ── 资源失效（含部分失效）─────────────────────────────────────────
    def invalidate_resource(
        self,
        resource_type: str,
        resource_id: str,
        slot: str,
        *,
        actor: str = "admin",
    ) -> dict[str, Any]:
        """讲解员请假 / 场地封闭等场景：声明某资源槽位失效。

        - 占用该槽位的 HELD 租约资源行置 INVALID，立即腾出容量。
        - 若租约资源全部失效，租约整体 RELEASED（reason=resource_invalid）。
        - 仅部分失效时租约保留为 HELD，事件中注明失效资源，供客服重配；
          该租约确认时仍可成功（剩余资源有效），由业务侧决定是否重配。
        - CONFIRMED 正式预约不动（走改期流程，不能静默吞掉正式预约）。
        重复声明同一失效是幂等的。
        """
        released: list[str] = []
        partially: list[str] = []
        with self.store.tx() as conn:
            affected = self.store.mark_resource_invalid(
                conn, resource_type, resource_id, slot
            )
            for lease_id in affected:
                self.store.append_event(
                    conn, lease_id, "RESOURCE_INVALID", actor,
                    {"resource_type": resource_type, "resource_id": resource_id, "slot": slot},
                )
                if self.store.active_resource_count(conn, lease_id) == 0:
                    ok, reason = self.store.cas_release(
                        conn, lease_id,
                        new_state=STATE_RELEASED,
                        reason="resource_invalid",
                        actor=actor,
                    )
                    if ok:
                        self.store.append_event(
                            conn, lease_id, "RELEASE", actor,
                            {"reason": "resource_invalid"},
                        )
                        released.append(lease_id)
                else:
                    partially.append(lease_id)
        return {
            "invalidated": [
                {"resource_type": resource_type, "resource_id": resource_id, "slot": slot}
            ],
            "released_leases": released,
            "partially_invalid_leases": partially,
        }

    # ── 超时扫描（多进程互斥）─────────────────────────────────────────
    def sweep(self, worker_id: str, *, batch_size: int = SWEEP_BATCH) -> dict[str, Any]:
        """扫描并回收到期暂占。

        两阶段：先原子 ``claim``（写认领令牌），再按认领所有权释放。
        多个清理进程并发运行时，每条到期记录只可能被一个进程认领和
        释放；崩溃进程遗留的认领略过宽限期后由其他进程接管。
        """
        now = to_iso(self._now())
        expired: list[str] = []
        claimed_stale: list[str] = []
        with self.store.tx() as conn:
            claimed = self.store.claim_expired(conn, now, worker_id, batch_size)
            stale = self.store.reclaim_stale_claims(conn, now, worker_id, batch_size)
        for lease_id in claimed:
            if self._release_claimed(lease_id, worker_id, reason="expired"):
                expired.append(lease_id)
        for lease_id in stale:
            if lease_id in claimed:
                continue
            if self._release_claimed(lease_id, worker_id, reason="claim_stale_expired"):
                claimed_stale.append(lease_id)
        return {
            "worker_id": worker_id,
            "released_expired": expired,
            "reclaimed_stale_claims": claimed_stale,
            "count": len(expired) + len(claimed_stale),
        }

    def _release_claimed(self, lease_id: str, worker_id: str, *, reason: str) -> bool:
        with self.store.tx() as conn:
            ok, code = self.store.cas_release(
                conn, lease_id,
                new_state=STATE_EXPIRED,
                reason=reason,
                actor=f"sweeper:{worker_id}",
                reclaim_owner=worker_id,
            )
            if ok:
                self.store.append_event(
                    conn, lease_id, "EXPIRE", f"sweeper:{worker_id}", {"reason": reason}
                )
                return True
            # already_terminal / confirmed / 认领被接管 → 本进程不释放。
            return False

    # ── 管理命令：查看来源、安全回收孤儿 ──────────────────────────────
    def admin_release(self, lease_id: str, *, reason: str, actor: str = "admin") -> dict[str, Any]:
        """管理员强制回收一条 HELD 暂占（孤儿记录/异常进程遗留）。

        对已终结记录幂等；CONFIRMED 正式预约拒绝回收（必须走取消预约）。
        """
        if not reason:
            raise ServiceError("bad_reason", "回收原因不能为空", 400)
        return self._release(
            lease_id,
            token=None,
            new_state=STATE_RELEASED,
            reason=f"admin:{reason}",
            actor=f"admin:{actor}",
        )

    def _release(
        self,
        lease_id: str,
        *,
        token: str | None,
        new_state: str,
        reason: str,
        actor: str,
    ) -> dict[str, Any]:
        with self.store.tx() as conn:
            ok, code = self.store.cas_release(
                conn, lease_id,
                new_state=new_state,
                reason=reason,
                actor=actor,
                token=token,
            )
            if ok:
                self.store.append_event(
                    conn, lease_id,
                    "EXPIRE" if new_state == STATE_EXPIRED else "RELEASE",
                    actor, {"reason": reason},
                )
            elif code == "already_terminal":
                # 幂等：返回当前状态，不制造新事件。
                pass
            else:
                raise self._error(code)
        return self.detail(lease_id)

    # ── 查询 ──────────────────────────────────────────────────────────
    def detail(self, lease_id: str) -> dict[str, Any]:
        with self.store.tx() as conn:
            lease = self.store.get(conn, lease_id)
            if lease is None:
                raise ServiceError("not_found", "租约不存在", 404)
            return self._detail_conn(conn, lease)

    def _detail_conn(
        self, conn, lease: dict[str, Any], *, idempotent: bool = False
    ) -> dict[str, Any]:
        resources = self.store.resource_states(conn, lease["lease_id"])
        events = self.store.events_conn(conn, lease["lease_id"])
        events_view = [
            {"event": e["event"], "actor": e["actor"], "detail": e["detail"],
             "created_at": e["created_at"]}
            for e in events
        ]
        out = {
            "lease_id": lease["lease_id"],
            "state": lease["state"],
            "version": lease["version"],
            "holder": lease["holder"],
            "holder_kind": lease["holder_kind"],
            "resources": resources,
            "created_at": lease["created_at"],
            "expires_at": lease["expires_at"],
            "renew_token": lease["renew_token"],
            "expected_capacity_version": lease["expected_capacity_version"],
            "confirmation_id": lease["confirmation_id"],
            "confirmed_at": lease["confirmed_at"],
            "cancel_reason": lease["cancel_reason"],
            "reclaimed_by": lease["reclaimed_by"],
            "events": events_view,
            "source": self._source(lease),
        }
        if idempotent:
            out["idempotent_replay"] = True
        return out

    @staticmethod
    def _source(lease: dict[str, Any]) -> dict[str, Any]:
        """占用来源：谁暂占的、因何终结/回收。"""
        return {
            "holder": lease["holder"],
            "holder_kind": lease["holder_kind"],
            "created_at": lease["created_at"],
            "reclaimed_by": lease["reclaimed_by"],
            "cancel_reason": lease["cancel_reason"],
        }

    def list_leases(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.store.list_leases(**kwargs)

    def capacity(self, resources: list[dict[str, str]]) -> dict[str, Any]:
        """容量查询：给定槽位中哪些仍被占用（容量失真排查入口）。"""
        resources = _normalize_resources(resources)
        with self.store.tx() as conn:
            busy = self.store.active_slots_for(conn, resources)
        return {"requested": resources, "busy": busy, "available": len(busy) == 0}

    @staticmethod
    def _error(code: str) -> ServiceError:
        messages = {
            "not_found": ("租约不存在", 404),
            "bad_token": ("续约令牌不匹配", 403),
            "version_conflict": ("版本不一致：资源或续约已变更，请重新读取容量", 409),
            "already_confirmed": ("租约已确认", 409),
            "not_held": ("租约已不在暂占状态", 409),
            "confirmed": ("正式预约不能被释放，请走取消预约流程", 409),
            "not_claimed_by_worker": ("记录未被当前清理进程认领", 409),
        }
        message, status = messages.get(code, (code, 400))
        return ServiceError(code, message, status)
