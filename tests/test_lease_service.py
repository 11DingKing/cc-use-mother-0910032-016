"""租约核心语义测试：版本比较、幂等、部分失效、并发清理互斥。"""
from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from scheduling_lease.service import LeaseError, LeaseService
from scheduling_lease.store import Store


class MutableClock:
    def __init__(self, start) -> None:
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def resources(guide="g-001", venue="hall-a", hour=10):
    base = "2026-10-10T"
    return [
        {"resource_type": "guide", "resource_ref": guide,
         "slot_start": f"{base}{hour:02d}:00:00+00:00",
         "slot_end": f"{base}{hour:02d}:59:00+00:00"},
        {"resource_type": "venue", "resource_ref": venue,
         "slot_start": f"{base}{hour:02d}:00:00+00:00",
         "slot_end": f"{base}{hour:02d}:59:00+00:00"},
    ]


class LeaseServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "test.sqlite3")
        self.clock = MutableClock(datetime(2026, 10, 10, 9, 0, tzinfo=timezone.utc))
        self.svc = LeaseService(Store(self.db), clock=self.clock)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def hold(self, **kw):
        kw.setdefault("school_id", "school-1")
        kw.setdefault("contact", "王老师")
        kw.setdefault("source", "客服电话 #123")
        kw.setdefault("created_by", "客服-小李")
        kw.setdefault("resources", resources())
        kw.setdefault("ttl_seconds", 900)
        return self.svc.hold(**kw)

    # ------------------------------------------------------- hold & conflict

    def test_group_hold_is_atomic_on_conflict(self) -> None:
        self.hold()  # 占住 g-001 + hall-a
        # 第二个学校用 g-001 + hall-b：因 g-001 冲突，hall-b 也不应被占
        with self.assertRaises(LeaseError) as ctx:
            self.hold(school_id="school-2",
                      resources=resources(guide="g-001", venue="hall-b"))
        self.assertEqual(ctx.exception.code, "conflict")
        # hall-b 仍可被第三所学校与另一位讲解员组合占用
        ok = self.hold(school_id="school-3",
                       resources=resources(guide="g-002", venue="hall-b"))
        self.assertEqual(ok["status"], "HELD")

    def test_overlapping_slots_conflict_adjacent_do_not(self) -> None:
        self.hold(resources=resources(hour=10))
        # 同时段冲突
        with self.assertRaises(LeaseError):
            self.hold(school_id="s2", resources=resources(hour=10))
        # 首尾相接（10:59 结束 / 11:00 开始）不冲突
        ok = self.hold(school_id="s2", resources=resources(hour=11))
        self.assertTrue(ok["lease_id"])

    # ------------------------------------------------------------- versioned

    def test_confirm_requires_token_and_version(self) -> None:
        lease = self.hold()
        # 错令牌
        with self.assertRaises(LeaseError) as ctx:
            self.svc.confirm(lease["lease_id"], "wrong-token", lease["version"])
        self.assertEqual(ctx.exception.code, "bad_token")
        # 错版本
        with self.assertRaises(LeaseError) as ctx:
            self.svc.confirm(lease["lease_id"], lease["token"], lease["version"] + 1)
        self.assertEqual(ctx.exception.code, "version_conflict")
        # 正确：转正式预约
        booking = self.svc.confirm(lease["lease_id"], lease["token"], lease["version"])
        self.assertEqual(booking["idempotent"], False)
        self.assertEqual(len(booking["resources"]), 2)

    def test_renew_bumps_version_and_old_confirm_rejected(self) -> None:
        lease = self.hold()
        v1 = lease["version"]
        renewed = self.svc.renew(lease["lease_id"], lease["token"], ttl_seconds=900)
        self.assertEqual(renewed["version"], v1 + 1)
        # 学校拿着旧版本来确认 -> 拒绝
        with self.assertRaises(LeaseError) as ctx:
            self.svc.confirm(lease["lease_id"], lease["token"], v1)
        self.assertEqual(ctx.exception.code, "version_conflict")
        # 按新版本确认成功
        booking = self.svc.confirm(lease["lease_id"], lease["token"], v1 + 1)
        self.assertFalse(booking["idempotent"])

    def test_duplicate_confirm_is_idempotent(self) -> None:
        lease = self.hold()
        b1 = self.svc.confirm(lease["lease_id"], lease["token"], lease["version"],
                              booking_id="booking-fixed")
        b2 = self.svc.confirm(lease["lease_id"], lease["token"], lease["version"],
                              booking_id="booking-fixed")
        self.assertEqual(b1["booking_id"], b2["booking_id"])
        self.assertTrue(b2["idempotent"])
        self.assertEqual(
            len([l for l in self.svc.list_leases() if l["status"] == "CONFIRMED"]), 1)

    def test_confirmed_booking_keeps_capacity(self) -> None:
        lease = self.hold()
        self.svc.confirm(lease["lease_id"], lease["token"], lease["version"])
        with self.assertRaises(LeaseError):
            self.hold(school_id="s2")

    # -------------------------------------------------------------- lifecycle

    def test_expiry_sweep_releases_and_frees_capacity(self) -> None:
        lease = self.hold(ttl_seconds=100)
        self.clock.advance(101)
        result = self.svc.sweep_expired("worker-1")
        self.assertEqual(result["released"], [lease["lease_id"]])
        view = self.svc.get_lease(lease["lease_id"])
        self.assertEqual(view["status"], "EXPIRED")
        self.assertEqual(view["released_reason"], "TIMEOUT")
        # 容量已释放：别的学校可占同一时段
        again = self.hold(school_id="s2")
        self.assertEqual(again["status"], "HELD")
        # 再次扫描幂等：不会重复释放
        second = self.svc.sweep_expired("worker-1")
        self.assertEqual(second["count"], 0)

    def test_cancel_is_idempotent_and_token_checked(self) -> None:
        lease = self.hold()
        with self.assertRaises(LeaseError) as ctx:
            self.svc.cancel(lease["lease_id"], "bad", actor="x")
        self.assertEqual(ctx.exception.code, "bad_token")
        r1 = self.svc.cancel(lease["lease_id"], lease["token"], actor="王老师")
        self.assertFalse(r1["idempotent"])
        r2 = self.svc.cancel(lease["lease_id"], lease["token"], actor="王老师")
        self.assertTrue(r2["idempotent"])
        self.assertEqual(r2["status"], "CANCELLED")
        # 取消后容量释放
        self.assertEqual(self.hold(school_id="s2")["status"], "HELD")

    def test_admin_force_cancel(self) -> None:
        lease = self.hold()
        r = self.svc.cancel(lease["lease_id"], None, actor="admin-root")
        self.assertFalse(r["idempotent"])
        self.assertEqual(self.svc.get_lease(lease["lease_id"])["status"], "RECLAIMED")

    def test_invalidate_partial_resources(self) -> None:
        l1 = self.hold(resources=resources(guide="g-001", venue="hall-a"))
        l2 = self.hold(school_id="s2",
                       resources=resources(guide="g-002", venue="hall-b"))
        result = self.svc.invalidate_resources(
            [{"resource_type": "guide", "resource_ref": "g-001"}],
            reason="讲解员突发请假", actor="场馆管理员")
        self.assertEqual([l["id"] for l in result["released_leases"]],
                         [l1["lease_id"]])
        self.assertEqual(self.svc.get_lease(l1["lease_id"])["status"], "INVALIDATED")
        self.assertEqual(self.svc.get_lease(l2["lease_id"])["status"], "HELD")
        # 重复登记失效：不重复释放
        again = self.svc.invalidate_resources(
            [{"resource_type": "guide", "resource_ref": "g-001"}],
            reason="讲解员突发请假", actor="场馆管理员")
        self.assertEqual(again["count"], 0)
        # 含失效资源的新暂占被拒；不含的可成功
        with self.assertRaises(LeaseError) as ctx:
            self.hold(school_id="s3", resources=resources(guide="g-001"))
        self.assertEqual(ctx.exception.code, "invalid_resource")
        self.assertEqual(
            self.svc.hold(school_id="s3", resources=resources(guide="g-009"))["status"],
            "HELD")

    def test_reclaim_orphans_by_id_and_grace(self) -> None:
        l1 = self.hold(ttl_seconds=10)
        l2 = self.hold(school_id="s2", ttl_seconds=1000,
                       resources=resources(guide="g-002", venue="hall-b"))
        self.clock.advance(50)
        # 指定 id 回收
        r = self.svc.reclaim_orphans("admin", lease_ids=[l1["lease_id"], "missing-id"])
        by_id = {x["id"]: x for x in r["results"]}
        self.assertTrue(by_id[l1["lease_id"]]["claimed"])
        self.assertIsNone(by_id["missing-id"]["previous_status"])
        # 重复回收已回收的记录：不报错，claimed=false
        r2 = self.svc.reclaim_orphans("admin", lease_ids=[l1["lease_id"]])
        self.assertFalse(r2["results"][0]["claimed"])
        # 宽限期孤儿扫描：l2 未过期，不回收
        self.assertEqual(self.svc.reclaim_orphans("admin", grace_seconds=0)["count"], 0)
        self.clock.advance(2000)
        r3 = self.svc.reclaim_orphans("admin", grace_seconds=60)
        self.assertEqual(r3["released"], [l2["lease_id"]])

    def test_renew_can_replace_group(self) -> None:
        lease = self.hold(resources=resources(guide="g-001", venue="hall-a"))
        renewed = self.svc.renew(
            lease["lease_id"], lease["token"],
            resources=resources(guide="g-002", venue="hall-a"))
        self.assertEqual(renewed["version"], 2)
        # 旧讲解员已释放：g-001 可被他人占用
        self.assertEqual(
            self.svc.hold(school_id="s2",
                          resources=resources(guide="g-001", venue="hall-b")
                          )["status"], "HELD")
        # 换成冲突讲解员则被拒，租约保持原样
        with self.assertRaises(LeaseError):
            self.svc.renew(lease["lease_id"], lease["token"],
                           resources=resources(guide="g-001", venue="hall-a"))
        still = self.svc.get_lease(lease["lease_id"])
        self.assertEqual(still["version"], 2)

    # ----------------------------------------------------------- concurrency

    def test_concurrent_holds_only_one_wins(self) -> None:
        barrier = threading.Barrier(8)
        outcomes: list[str] = []
        lock = threading.Lock()

        def worker(school: str) -> None:
            barrier.wait()
            try:
                self.svc.hold(school_id=school, resources=resources())
                with lock:
                    outcomes.append("ok")
            except LeaseError:
                with lock:
                    outcomes.append("conflict")

        threads = [threading.Thread(target=worker, args=(f"s{i}",)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(outcomes.count("ok"), 1)
        self.assertEqual(outcomes.count("conflict"), 7)

    def test_concurrent_sweeps_claim_exactly_once(self) -> None:
        leases = [
            self.hold(school_id=f"s{i}", ttl_seconds=10,
                      resources=resources(guide=f"g-{i:03d}", venue=f"hall-{i:03d}")
                      )["lease_id"]
            for i in range(20)
        ]
        self.clock.advance(100)
        barrier = threading.Barrier(6)
        released: list[list[str]] = []
        lock = threading.Lock()

        def worker(wid: str) -> None:
            barrier.wait()
            r = self.svc.sweep_expired(wid, batch=50)
            with lock:
                released.append(r["released"])

        threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        flat = [i for batch in released for i in batch]
        self.assertEqual(sorted(flat), sorted(leases))  # 每条恰好释放一次

    def test_concurrent_confirm_and_cancel_only_one_terminal_state(self) -> None:
        lease = self.hold()

        def confirm():
            try:
                self.svc.confirm(lease["lease_id"], lease["token"], lease["version"])
            except LeaseError:
                pass

        def cancel():
            try:
                self.svc.cancel(lease["lease_id"], lease["token"], actor="x")
            except LeaseError:
                pass

        t1 = threading.Thread(target=confirm)
        t2 = threading.Thread(target=cancel)
        t1.start(); t2.start(); t1.join(); t2.join()
        final = self.svc.get_lease(lease["lease_id"])
        self.assertIn(final["status"], ("CONFIRMED", "CANCELLED"))
        if final["status"] == "CONFIRMED":
            # 确认胜出：重复确认仍幂等返回同一预约
            again = self.svc.confirm(
                lease["lease_id"], lease["token"], lease["version"])
            self.assertTrue(again["idempotent"])
            # 取消不能再改终态
            self.assertTrue(self.svc.cancel(
                lease["lease_id"], lease["token"], actor="x")["idempotent"])
        else:
            # 取消胜出：再确认被拒，再取消幂等
            with self.assertRaises(LeaseError) as ctx:
                self.svc.confirm(lease["lease_id"], lease["token"], lease["version"])
            self.assertEqual(ctx.exception.code, "not_held")
            self.assertTrue(self.svc.cancel(
                lease["lease_id"], lease["token"], actor="x")["idempotent"])


if __name__ == "__main__":
    unittest.main()
