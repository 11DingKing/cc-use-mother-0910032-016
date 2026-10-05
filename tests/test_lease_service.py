"""租约状态机与幂等性回归测试。"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from schedule_leases.server import build_server
from schedule_leases.service import LeaseService, ServiceError
from schedule_leases.store import (
    STATE_CONFIRMED,
    STATE_EXPIRED,
    STATE_HELD,
    STATE_RELEASED,
    LeaseStore,
    to_iso,
)
from schedule_leases.clock import ManualClock


GUIDE = {"resource_type": "guide", "resource_id": "g1", "slot": "2026-10-12T09:00"}
VENUE = {"resource_type": "venue", "resource_id": "v1", "slot": "2026-10-12T09:00"}
GUIDE2 = {"resource_type": "guide", "resource_id": "g2", "slot": "2026-10-12T09:00"}


def make_service(ttl: int = 900) -> tuple[LeaseService, ManualClock]:
    clock = ManualClock()
    svc = LeaseService(LeaseStore(":memory:"), clock=clock.now, default_ttl=ttl)
    return svc, clock


class HoldConfirmTest(unittest.TestCase):
    def test_hold_then_confirm_with_version(self) -> None:
        svc, _ = make_service()
        lease = svc.hold(holder="学校A/客服小王", holder_kind="agent",
                         resources=[GUIDE, VENUE])
        self.assertEqual(lease["state"], STATE_HELD)
        self.assertEqual(lease["version"], 1)
        self.assertTrue(lease["renew_token"])

        confirmed = svc.confirm(
            lease["lease_id"],
            token=lease["renew_token"],
            expected_version=1,
            confirmation_id="confirm-001",
        )
        self.assertEqual(confirmed["state"], STATE_CONFIRMED)
        self.assertEqual(confirmed["version"], 2)
        self.assertEqual(confirmed["confirmation_id"], "confirm-001")
        # 正式预约仍占用资源
        self.assertTrue(all(r["state"] == "ACTIVE" for r in confirmed["resources"]))

    def test_confirm_version_conflict_after_renew(self) -> None:
        svc, clock = make_service()
        lease = svc.hold(holder="学校A", holder_kind="agent", resources=[GUIDE])
        svc.renew(lease["lease_id"], lease["renew_token"], ttl_seconds=600)
        with self.assertRaises(ServiceError) as ctx:
            svc.confirm(lease["lease_id"], token=lease["renew_token"],
                        expected_version=1, confirmation_id="c1")
        self.assertEqual(ctx.exception.code, "version_conflict")
        # 用新版本可确认
        fresh = svc.detail(lease["lease_id"])
        ok = svc.confirm(lease["lease_id"], token=lease["renew_token"],
                         expected_version=fresh["version"], confirmation_id="c1")
        self.assertEqual(ok["state"], STATE_CONFIRMED)

    def test_duplicate_confirm_is_idempotent(self) -> None:
        svc, _ = make_service()
        lease = svc.hold(holder="学校A", holder_kind="agent", resources=[GUIDE])
        args = dict(token=lease["renew_token"], expected_version=1,
                    confirmation_id="idempotent-1")
        first = svc.confirm(lease["lease_id"], **args)
        second = svc.confirm(lease["lease_id"], **args)  # 网络重试
        self.assertEqual(first["state"], STATE_CONFIRMED)
        self.assertTrue(second.get("idempotent_replay"))
        self.assertEqual(second["confirmation_id"], "idempotent-1")
        events = svc.store.list_events(lease["lease_id"])
        self.assertEqual([e["event"] for e in events].count("CONFIRM"), 1)

    def test_confirmation_id_cannot_cross_leases(self) -> None:
        svc, _ = make_service()
        l1 = svc.hold(holder="A", holder_kind="agent", resources=[GUIDE])
        l2 = svc.hold(holder="B", holder_kind="agent", resources=[GUIDE2])
        svc.confirm(l1["lease_id"], token=l1["renew_token"],
                    expected_version=1, confirmation_id="X")
        with self.assertRaises(ServiceError) as ctx:
            svc.confirm(l2["lease_id"], token=l2["renew_token"],
                        expected_version=1, confirmation_id="X")
        self.assertEqual(ctx.exception.code, "confirmation_id_in_use")

    def test_confirm_bad_token_rejected(self) -> None:
        svc, _ = make_service()
        lease = svc.hold(holder="A", holder_kind="agent", resources=[GUIDE])
        with self.assertRaises(ServiceError) as ctx:
            svc.confirm(lease["lease_id"], token="wrong", expected_version=1)
        self.assertEqual(ctx.exception.code, "bad_token")

    def test_same_slot_cannot_be_double_held(self) -> None:
        svc, _ = make_service()
        svc.hold(holder="A", holder_kind="agent", resources=[GUIDE, VENUE])
        with self.assertRaises(ServiceError) as ctx:
            svc.hold(holder="B", holder_kind="agent", resources=[VENUE])
        self.assertEqual(ctx.exception.code, "slot_busy")


class CancelAndExpireTest(unittest.TestCase):
    def test_cancel_releases_slots_and_is_idempotent(self) -> None:
        svc, _ = make_service()
        lease = svc.hold(holder="A", holder_kind="agent", resources=[GUIDE])
        first = svc.cancel(lease["lease_id"], lease["renew_token"])
        self.assertEqual(first["state"], STATE_RELEASED)
        # 重复取消幂等
        again = svc.cancel(lease["lease_id"], lease["renew_token"])
        self.assertEqual(again["state"], STATE_RELEASED)
        events = svc.store.list_events(lease["lease_id"])
        self.assertEqual([e["event"] for e in events].count("RELEASE"), 1)
        # 槽位已腾出
        cap = svc.capacity([GUIDE])
        self.assertTrue(cap["available"])

    def test_cancel_confirmed_refused(self) -> None:
        svc, _ = make_service()
        lease = svc.hold(holder="A", holder_kind="agent", resources=[GUIDE])
        svc.confirm(lease["lease_id"], token=lease["renew_token"], expected_version=1)
        with self.assertRaises(ServiceError) as ctx:
            svc.cancel(lease["lease_id"], lease["renew_token"])
        self.assertEqual(ctx.exception.code, "confirmed")

    def test_sweep_releases_expired_only(self) -> None:
        svc, clock = make_service(ttl=100)
        live = svc.hold(holder="A", holder_kind="agent", resources=[GUIDE],
                        ttl_seconds=900)
        dead = svc.hold(holder="B", holder_kind="agent", resources=[GUIDE2],
                        ttl_seconds=100)
        clock.advance(seconds=101)
        result = svc.sweep("w1")
        self.assertEqual(result["released_expired"], [dead["lease_id"]])
        self.assertEqual(svc.detail(dead["lease_id"])["state"], STATE_EXPIRED)
        self.assertEqual(svc.detail(live["lease_id"])["state"], STATE_HELD)
        # 再扫一次：零释放，幂等
        again = svc.sweep("w1")
        self.assertEqual(again["count"], 0)
        # 到期槽位容量恢复
        self.assertTrue(svc.capacity([GUIDE2])["available"])


class ResourceInvalidationTest(unittest.TestCase):
    def test_partial_invalidation_keeps_lease(self) -> None:
        svc, _ = make_service()
        lease = svc.hold(holder="A", holder_kind="agent", resources=[GUIDE, VENUE])
        result = svc.invalidate_resource("guide", "g1", "2026-10-12T09:00")
        self.assertEqual(result["partially_invalid_leases"], [lease["lease_id"]])
        self.assertEqual(result["released_leases"], [])
        detail = svc.detail(lease["lease_id"])
        self.assertEqual(detail["state"], STATE_HELD)
        states = {(r["resource_type"], r["resource_id"]): r["state"]
                  for r in detail["resources"]}
        self.assertEqual(states[("guide", "g1")], "INVALID")
        self.assertEqual(states[("venue", "v1")], "ACTIVE")
        # 失效槽位立即可被他人暂占（容量不再失真）
        other = svc.hold(holder="B", holder_kind="agent", resources=[GUIDE])
        self.assertEqual(other["state"], STATE_HELD)

    def test_full_invalidation_releases_lease_idempotently(self) -> None:
        svc, _ = make_service()
        lease = svc.hold(holder="A", holder_kind="agent", resources=[GUIDE, VENUE])
        svc.invalidate_resource("guide", "g1", "2026-10-12T09:00")
        result = svc.invalidate_resource("venue", "v1", "2026-10-12T09:00")
        self.assertEqual(result["released_leases"], [lease["lease_id"]])
        self.assertEqual(svc.detail(lease["lease_id"])["state"], STATE_RELEASED)
        # 重复声明失效不产生新释放
        again = svc.invalidate_resource("venue", "v1", "2026-10-12T09:00")
        self.assertEqual(again["released_leases"], [])

    def test_invalidation_does_not_touch_confirmed(self) -> None:
        svc, _ = make_service()
        lease = svc.hold(holder="A", holder_kind="agent", resources=[GUIDE])
        svc.confirm(lease["lease_id"], token=lease["renew_token"], expected_version=1)
        result = svc.invalidate_resource("guide", "g1", "2026-10-12T09:00")
        self.assertEqual(result["released_leases"], [])
        self.assertEqual(svc.detail(lease["lease_id"])["state"], STATE_CONFIRMED)


class ConcurrentSweepTest(unittest.TestCase):
    """多清理进程（多连接/多线程）不能重复释放同一条占用。"""

    def test_concurrent_sweepers_release_each_lease_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "leases.db")
            seed = LeaseService(LeaseStore(db))
            lease_ids = []
            for i in range(30):
                lease = seed.hold(
                    holder=f"school-{i}", holder_kind="agent",
                    resources=[{"resource_type": "guide", "resource_id": f"g{i}",
                                "slot": "2026-10-12T09:00"}],
                    ttl_seconds=1,
                )
                lease_ids.append(lease["lease_id"])
            time.sleep(1.1)  # 全部到期

            errors: list[BaseException] = []

            def worker(n: int) -> None:
                try:
                    svc = LeaseService(LeaseStore(db))
                    for _ in range(10):
                        r = svc.sweep(f"worker-{n}")
                        if r["count"] == 0:
                            break
                except BaseException as exc:  # noqa: BLE001
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(errors, [])

            checker = LeaseStore(db)
            for lid in lease_ids:
                row = checker.get(checker.conn, lid)
                self.assertIsNotNone(row)
                self.assertEqual(row["state"], STATE_EXPIRED)  # type: ignore[index]
                events = checker.list_events(lid)
                self.assertEqual(
                    [e["event"] for e in events].count("EXPIRE"), 1,
                    f"{lid} 被重复释放",
                )

    def test_stale_claim_is_reclaimed_but_fresh_claim_is_not(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "leases.db")
            clock = ManualClock()
            store = LeaseStore(db)
            svc = LeaseService(store, clock=clock.now)
            stale = svc.hold(holder="old", holder_kind="agent",
                            resources=[GUIDE], ttl_seconds=10)
            fresh = svc.hold(holder="new", holder_kind="agent",
                             resources=[GUIDE2], ttl_seconds=10)
            clock.advance(seconds=11)
            # worker-A 认领两条后"崩溃"（认领时间 = 11s）
            with store.tx() as conn:
                claimed = store.claim_expired(conn, to_iso(clock.now()), "worker-A", 100)
            self.assertEqual(set(claimed), {stale["lease_id"], fresh["lease_id"]})
            # 时间走过宽限期；模拟 worker-A 在崩溃前对 fresh 刚做过心跳
            clock.advance(seconds=400)  # 超过 CLAIM_GRACE=5min
            with store.tx() as conn:
                conn.execute(
                    "UPDATE lease SET reclaim_at = ? WHERE lease_id = ?",
                    (to_iso(clock.now()), fresh["lease_id"]),
                )
            result = svc.sweep("worker-B")
            self.assertEqual(result["reclaimed_stale_claims"], [stale["lease_id"]])
            self.assertEqual(svc.detail(stale["lease_id"])["state"], STATE_EXPIRED)
            # fresh 认领在宽限内（视为 worker-A 仍在处理），worker-B 不得释放
            self.assertEqual(svc.detail(fresh["lease_id"])["state"], STATE_HELD)


class AdminTest(unittest.TestCase):
    def test_admin_release_orphan_is_safe_and_idempotent(self) -> None:
        svc, _ = make_service()
        lease = svc.hold(holder="zombie", holder_kind="agent", resources=[GUIDE])
        out = svc.admin_release(lease["lease_id"], reason="进程异常遗留", actor="ops")
        self.assertEqual(out["state"], STATE_RELEASED)
        self.assertEqual(out["reclaimed_by"], "admin:ops")
        again = svc.admin_release(lease["lease_id"], reason="重复回收", actor="ops")
        self.assertEqual(again["state"], STATE_RELEASED)
        events = svc.store.list_events(lease["lease_id"])
        self.assertEqual([e["event"] for e in events].count("RELEASE"), 1)

    def test_admin_release_requires_reason_and_refuses_confirmed(self) -> None:
        svc, _ = make_service()
        lease = svc.hold(holder="A", holder_kind="agent", resources=[GUIDE])
        with self.assertRaises(ServiceError) as ctx:
            svc.admin_release(lease["lease_id"], reason="")
        self.assertEqual(ctx.exception.code, "bad_reason")
        svc.confirm(lease["lease_id"], token=lease["renew_token"], expected_version=1)
        with self.assertRaises(ServiceError) as ctx:
            svc.admin_release(lease["lease_id"], reason="x")
        self.assertEqual(ctx.exception.code, "confirmed")

    def test_list_shows_source(self) -> None:
        svc, _ = make_service()
        svc.hold(holder="学校A/小王", holder_kind="agent", resources=[GUIDE])
        rows = svc.list_leases(state=STATE_HELD)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["holder"], "学校A/小王")


class HttpApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = build_server(":memory:", "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def _req(self, method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_end_to_end_api(self) -> None:
        status, body = self._req("POST", "/leases", {
            "holder": "学校A/客服", "holder_kind": "agent",
            "resources": [GUIDE, VENUE], "ttl_seconds": 600,
        })
        self.assertEqual(status, 200)
        lid, token, version = body["lease_id"], body["renew_token"], body["version"]

        # 容量被占
        status, cap = self._req("POST", "/capacity", {"resources": [VENUE]})
        self.assertFalse(cap["available"])

        # 重复确认幂等
        for _ in range(2):
            status, body = self._req("POST", f"/leases/{lid}/confirm", {
                "renew_token": token, "expected_version": version,
                "confirmation_id": "api-cid-1",
            })
            self.assertEqual(status, 200)
            self.assertEqual(body["state"], STATE_CONFIRMED)

        # 正式预约不能被管理员回收
        status, body = self._req("POST", f"/admin/leases/{lid}/release",
                                 {"reason": "test"})
        self.assertEqual(status, 409)
        self.assertEqual(body["error"], "confirmed")

        # 详情包含来源与事件
        status, body = self._req("GET", f"/leases/{lid}")
        self.assertEqual(status, 200)
        self.assertEqual(body["source"]["holder"], "学校A/客服")
        self.assertIn("events", body)


if __name__ == "__main__":
    unittest.main()
