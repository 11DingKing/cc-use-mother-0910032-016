"""HTTP API 端到端测试 + 多进程超时扫描互斥测试。"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from scheduling_lease.api import ApiHandler
from scheduling_lease.service import LeaseService
from scheduling_lease.store import Store


def res(guide="g-001", venue="hall-a"):
    return [
        {"resource_type": "guide", "resource_ref": guide,
         "slot_start": "2026-10-10T10:00:00+00:00",
         "slot_end": "2026-10-10T10:59:00+00:00"},
        {"resource_type": "venue", "resource_ref": venue,
         "slot_start": "2026-10-10T10:00:00+00:00",
         "slot_end": "2026-10-10T10:59:00+00:00"},
    ]


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "api.sqlite3")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), ApiHandler)
        self.server.service = LeaseService(Store(self.db))
        self.server.admin_token = "secret"
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.tmp.cleanup()

    def call(self, method: str, path: str, body: dict | None = None,
             admin: bool = False):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if admin:
            headers["X-Admin-Token"] = "secret"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_full_flow_and_idempotency(self) -> None:
        # 1. 暂占
        status, lease = self.call("POST", "/v1/leases", {
            "school_id": "school-1", "contact": "王老师",
            "source": "客服电话 #123", "created_by": "客服-小李",
            "resources": res(), "ttl_seconds": 900})
        self.assertEqual(status, 201)
        lid, token, version = lease["lease_id"], lease["token"], lease["version"]

        # 2. 管理接口需要口令
        s, _ = self.call("POST", "/v1/admin/sweep", {})
        self.assertEqual(s, 403)

        # 3. 错误版本确认被拒
        s, err = self.call("POST", f"/v1/leases/{lid}/confirm",
                           {"token": token, "version": version + 1})
        self.assertEqual(s, 409)
        self.assertEqual(err["error"], "version_conflict")

        # 4. 正确确认 -> 201
        s, booking = self.call("POST", f"/v1/leases/{lid}/confirm",
                               {"token": token, "version": version})
        self.assertEqual(s, 201)
        bid = booking["booking_id"]

        # 5. 重复确认 -> 200 idempotent，同一预约
        s, again = self.call("POST", f"/v1/leases/{lid}/confirm",
                             {"token": token, "version": version})
        self.assertEqual(s, 200)
        self.assertTrue(again["idempotent"])
        self.assertEqual(again["booking_id"], bid)

        # 6. 预约可查
        s, got = self.call("GET", f"/v1/bookings/{bid}")
        self.assertEqual(s, 200)
        self.assertEqual(got["school_id"], "school-1")

    def test_admin_sweep_reclaim_invalidate(self) -> None:
        _, lease = self.call("POST", "/v1/leases",
                             {"school_id": "s1", "source": "线上表单",
                              "resources": res(guide="g-009", venue="hall-z")})
        lid = lease["lease_id"]

        # 列表可见来源
        s, rows = self.call("GET", "/v1/leases?status=HELD")
        self.assertEqual(s, 200)
        self.assertEqual(rows[0]["source"], "线上表单")

        # 管理员强制回收单条
        s, r = self.call("POST", f"/v1/admin/leases/{lid}/reclaim",
                         {"actor": "主管"}, admin=True)
        self.assertEqual(s, 200)
        self.assertEqual(r["status"], "RECLAIMED")

        # 再回收：幂等
        s, r = self.call("POST", f"/v1/admin/leases/{lid}/reclaim",
                         {"actor": "主管"}, admin=True)
        self.assertEqual(s, 200)
        self.assertTrue(r["idempotent"])
        self.assertEqual(r["status"], "RECLAIMED")

        # 资源失效
        s, r = self.call("POST", "/v1/admin/invalid-resources",
                         {"resources": [{"resource_type": "venue",
                                         "resource_ref": "hall-q"}],
                          "reason": "空调维修", "actor": "主管"}, admin=True)
        self.assertEqual(s, 200)
        s, rows = self.call("GET", "/v1/admin/invalid-resources", admin=True)
        self.assertEqual(s, 200)
        self.assertEqual(rows[0]["reason"], "空调维修")
        # 用失效资源暂占被拒
        s, err = self.call("POST", "/v1/leases",
                           {"school_id": "s2",
                            "resources": res(guide="g-1", venue="hall-q")})
        self.assertEqual(s, 422)
        self.assertEqual(err["error"], "invalid_resource")


# 多进程扫描脚本：循环扫描直到一轮无释放，把累计释放数写入文件。
SWEEP_SCRIPT = """
import sys, json
sys.path.insert(0, %(src)r)
from scheduling_lease.service import LeaseService
svc = LeaseService(%(db)r)
total = 0
ids = []
while True:
    r = svc.sweep_expired(%(worker)r, batch=7)
    if not r["released"]:
        break
    total += len(r["released"])
    ids.extend(r["released"])
with open(%(out)r, "w", encoding="utf-8") as f:
    json.dump({"total": total, "ids": ids}, f)
"""


class MultiProcessSweepTest(unittest.TestCase):
    def test_no_double_release_across_processes(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = str(Path(tmp.name) / "mp.sqlite3")
        svc = LeaseService(Store(db))
        n = 30
        for i in range(n):
            svc.hold(school_id=f"s{i}", source=f"客服-{i}",
                     resources=res(guide=f"g-{i:03d}", venue=f"hall-{i:03d}"),
                     ttl_seconds=1)  # 1 秒后到期
        time.sleep(1.2)  # 等全部过期

        procs = []
        out_files = []
        for w in range(6):
            out = str(Path(tmp.name) / f"out{w}.json")
            out_files.append(out)
            code = SWEEP_SCRIPT % {"src": str(ROOT / "src"), "db": db,
                                   "worker": f"proc-{w}", "out": out}
            procs.append(subprocess.Popen(
                [sys.executable, "-c", code],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        for p in procs:
            stdout, stderr = p.communicate(timeout=60)
            self.assertEqual(p.returncode, 0, stderr.decode())

        totals, all_ids = 0, []
        for f in out_files:
            data = json.loads(Path(f).read_text(encoding="utf-8"))
            totals += data["total"]
            all_ids.extend(data["ids"])

        # 所有过期租约被释放，且每条恰好一个进程释放（无重复释放）
        self.assertEqual(totals, n)
        self.assertEqual(len(all_ids), len(set(all_ids)))
        final = svc.list_leases()
        self.assertTrue(all(l["status"] == "EXPIRED" for l in final))
        self.assertEqual(len(final), n)
        # 释放后容量恢复：同一资源同一时段可重新暂占
        ok = svc.hold(school_id="new", resources=res(guide="g-000", venue="hall-0"))
        self.assertEqual(ok["status"], "HELD")


if __name__ == "__main__":
    unittest.main()
