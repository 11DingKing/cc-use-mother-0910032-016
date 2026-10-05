"""SQLite 原子状态机存储（仅标准库）。

状态机::

    HELD ──confirm──▶ CONFIRMED
      │
      ├──cancel / invalid──▶ RELEASED
      └──expire sweep──────▶ EXPIRED

设计要点
--------
1. 租约行通过条件 UPDATE 做 CAS（比较 version/renewal_token/状态），
   所有转换都是单条原子语句，天然幂等。
2. 每个 (resource_type, resource_id, slot) 上有唯一的"活跃占用"
   部分唯一索引：同一时刻同一资源槽位只允许一条 HELD/CONFIRMED 记录。
3. 扫描者认领（claim）用一条 ``UPDATE ... WHERE lease_id IN
   (SELECT ... WHERE state='HELD' AND expires_at <= :now AND
   reclaim_token IS NULL)`` 完成，多个清理进程并发运行时只有一个能
   把 token 写上，避免重复释放。
"""
from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

# ── 状态常量 ─────────────────────────────────────────────────────────
STATE_HELD = "HELD"
STATE_CONFIRMED = "CONFIRMED"
STATE_RELEASED = "RELEASED"
STATE_EXPIRED = "EXPIRED"

ACTIVE_STATES = (STATE_HELD, STATE_CONFIRMED)
TERMINAL_STATES = (STATE_RELEASED, STATE_EXPIRED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS lease (
    lease_id       TEXT PRIMARY KEY,
    version        INTEGER NOT NULL,
    holder         TEXT NOT NULL,
    holder_kind    TEXT NOT NULL,
    resources_json TEXT NOT NULL,
    renew_token    TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    expires_at     TEXT NOT NULL,
    state          TEXT NOT NULL,
    expected_capacity_version INTEGER NOT NULL,
    confirmed_at   TEXT,
    confirmation_id TEXT,
    cancel_reason  TEXT,
    reclaimed_by   TEXT,
    reclaim_token  TEXT,
    reclaim_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_lease_state_expires ON lease(state, expires_at);
CREATE INDEX IF NOT EXISTS idx_lease_holder ON lease(holder);
CREATE INDEX IF NOT EXISTS idx_lease_reclaim ON lease(state, reclaim_at)
    WHERE reclaim_token IS NOT NULL;

CREATE TABLE IF NOT EXISTS lease_resource (
    lease_id      TEXT NOT NULL REFERENCES lease(lease_id),
    resource_type TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    slot          TEXT NOT NULL,
    -- ACTIVE=仍占槽位；RELEASED=随租约释放；INVALID=资源自身失效
    state         TEXT NOT NULL DEFAULT 'ACTIVE',
    PRIMARY KEY (lease_id, resource_type, resource_id, slot)
);
CREATE INDEX IF NOT EXISTS idx_lr_slot ON lease_resource(resource_type, resource_id, slot);

-- 同一资源槽位至多一条"仍占槽位"的记录：暂占与正式预约互斥，
-- 已释放/失效的行不参与唯一约束，可保留作审计。
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_slot
    ON lease_resource(resource_type, resource_id, slot)
    WHERE state = 'ACTIVE';

CREATE TABLE IF NOT EXISTS lease_event (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_id   TEXT NOT NULL,
    event      TEXT NOT NULL,
    actor      TEXT NOT NULL,
    detail     TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_lease ON lease_event(lease_id, seq);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


class LeaseStore:
    """线程安全的 SQLite 封装。

    每个工作线程持有自己的连接（``check_same_thread=False`` + 连接锁），
    写事务使用 ``BEGIN IMMEDIATE`` 获取写锁，保证多进程/多线程下的
    条件更新不会漏判。
    """

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        # URI 形式让多连接共享同一个内存库（仅测试用）。
        self._uri = self.path
        self._lock = threading.RLock()
        self._conn = self._connect()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self._uri, timeout=30, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)

    # ── 低层辅助 ──────────────────────────────────────────────────────
    @contextlib.contextmanager
    def tx(self):
        """立即写事务上下文。多进程依赖 SQLite 写锁，多线程依赖 RLock。"""
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        data = dict(row)
        data["resources"] = json.loads(data.pop("resources_json"))
        return data

    def get(self, conn: sqlite3.Connection, lease_id: str) -> dict[str, Any] | None:
        row = conn.execute("SELECT * FROM lease WHERE lease_id = ?", (lease_id,)).fetchone()
        return self._row_to_dict(row)

    def get_by_confirmation(
        self, conn: sqlite3.Connection, confirmation_id: str
    ) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT * FROM lease WHERE confirmation_id = ?", (confirmation_id,)
        ).fetchone()
        return self._row_to_dict(row)

    def insert(
        self,
        conn: sqlite3.Connection,
        *,
        lease_id: str,
        holder: str,
        holder_kind: str,
        resources: list[dict[str, str]],
        renew_token: str,
        created_at: str,
        expires_at: str,
        expected_capacity_version: int,
    ) -> dict[str, Any]:
        conn.execute(
            """INSERT INTO lease(lease_id, version, holder, holder_kind,
                  resources_json, renew_token, created_at, expires_at, state,
                  expected_capacity_version)
               VALUES(?, 1, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                lease_id,
                holder,
                holder_kind,
                json.dumps(resources, ensure_ascii=False, sort_keys=True),
                renew_token,
                created_at,
                expires_at,
                STATE_HELD,
                expected_capacity_version,
            ),
        )
        for r in resources:
            conn.execute(
                """INSERT INTO lease_resource(lease_id, resource_type, resource_id, slot)
                   VALUES(?, ?, ?, ?)""",
                (lease_id, r["resource_type"], r["resource_id"], r["slot"]),
            )
        return self.get(conn, lease_id)  # type: ignore[return-value]

    def append_event(
        self,
        conn: sqlite3.Connection,
        lease_id: str,
        event: str,
        actor: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            """INSERT INTO lease_event(lease_id, event, actor, detail, created_at)
               VALUES(?, ?, ?, ?, ?)""",
            (lease_id, event, actor, json.dumps(detail or {}, ensure_ascii=False), to_iso(utcnow())),
        )

    # ── 原子状态转换（CAS）────────────────────────────────────────────
    def cas_renew(
        self,
        conn: sqlite3.Connection,
        lease_id: str,
        token: str,
        new_expires_at: str,
    ) -> tuple[bool, str]:
        row = conn.execute(
            "SELECT state FROM lease WHERE lease_id = ? AND renew_token = ?",
            (lease_id, token),
        ).fetchone()
        if row is None:
            exists = conn.execute(
                "SELECT 1 FROM lease WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            return False, "not_found" if exists is None else "bad_token"
        if row["state"] != STATE_HELD:
            return False, "not_held"
        cur = conn.execute(
            """UPDATE lease
                  SET version = version + 1, expires_at = ?
                WHERE lease_id = ? AND state = 'HELD' AND renew_token = ?""",
            (new_expires_at, lease_id, token),
        )
        return (cur.rowcount == 1), "ok"

    def cas_confirm(
        self,
        conn: sqlite3.Connection,
        lease_id: str,
        *,
        token: str,
        expected_version: int,
        confirmation_id: str,
        confirmed_at: str,
    ) -> tuple[bool, str]:
        """原子确认转换，比较 version 与续约令牌。

        返回 ``(是否本次发生转换, 原因码)``：
        ``ok`` / ``not_found`` / ``bad_token`` / ``version_conflict`` /
        ``already_confirmed`` / ``not_held``。

        重复确认的幂等返回由 service 层依据 confirmation_id 判别；
        本方法只做单次原子 CAS。
        """
        row = conn.execute(
            "SELECT state, version, renew_token FROM lease WHERE lease_id = ?",
            (lease_id,),
        ).fetchone()
        if row is None:
            return False, "not_found"
        state, version, current_token = row
        if state == STATE_CONFIRMED:
            return False, "already_confirmed"
        if state != STATE_HELD:
            return False, "not_held"
        if current_token != token:
            return False, "bad_token"
        if version != expected_version:
            return False, "version_conflict"
        cur = conn.execute(
            """UPDATE lease
                  SET state = 'CONFIRMED', confirmation_id = ?, confirmed_at = ?,
                      version = version + 1
                WHERE lease_id = ? AND state = 'HELD'
                  AND renew_token = ? AND version = ?""",
            (confirmation_id, confirmed_at, lease_id, token, expected_version),
        )
        if cur.rowcount != 1:
            return False, "not_held"
        # 正式预约继续占用资源槽位（资源行保持 ACTIVE）。
        return True, "ok"

    def cas_release(
        self,
        conn: sqlite3.Connection,
        lease_id: str,
        *,
        new_state: str,
        reason: str,
        actor: str,
        token: str | None = None,
        reclaim_owner: str | None = None,
    ) -> tuple[bool, str]:
        """通用释放转换（主动取消 / 资源失效 / 超时回收）。

        - ``token`` 非空时必须匹配续约令牌（持有人主动取消）。
        - ``reclaim_owner`` 非空时该行必须已由该清理者认领（超时回收），
          防止一个清理进程释放另一个进程已认领的记录。
        - 已终结 / 已确认返回 ``already_terminal`` / ``confirmed``，
          调用方按幂等成功处理。
        释放时所有仍 ACTIVE 的资源行一并置 RELEASED，腾出槽位。
        """
        row = conn.execute(
            "SELECT state, renew_token, reclaim_token FROM lease WHERE lease_id = ?",
            (lease_id,),
        ).fetchone()
        if row is None:
            return False, "not_found"
        state, current_token, claimed_by = row
        if state in TERMINAL_STATES:
            return False, "already_terminal"
        if state == STATE_CONFIRMED:
            return False, "confirmed"
        if token is not None and current_token != token:
            return False, "bad_token"
        if reclaim_owner is not None and claimed_by != reclaim_owner:
            return False, "not_claimed_by_worker"
        cur = conn.execute(
            """UPDATE lease
                  SET state = ?, cancel_reason = ?, reclaimed_by = ?,
                      reclaim_token = NULL, version = version + 1
                WHERE lease_id = ? AND state = 'HELD'""",
            (new_state, reason, actor, lease_id),
        )
        if cur.rowcount != 1:
            return False, "not_held"
        conn.execute(
            "UPDATE lease_resource SET state = 'RELEASED' WHERE lease_id = ? AND state = 'ACTIVE'",
            (lease_id,),
        )
        return True, "ok"

    def claim_expired(
        self, conn: sqlite3.Connection, now: str, worker_id: str, limit: int
    ) -> list[str]:
        """原子认领一批到期租约。

        只有 reclaim_token 仍为 NULL 的 HELD 到期行会被写上本进程的
        worker token；多个清理进程并发执行时认领集合互不重叠。
        认领时间使用调用方传入的 ``now``（与业务时钟一致，便于测试）。
        """
        conn.execute(
            """UPDATE lease
                  SET reclaim_token = ?, reclaim_at = ?
                WHERE lease_id IN (
                    SELECT lease_id FROM lease
                     WHERE state = 'HELD'
                       AND expires_at <= ?
                       AND reclaim_token IS NULL
                     LIMIT ?
                )""",
            (worker_id, now, now, limit),
        )
        rows = conn.execute(
            "SELECT lease_id FROM lease WHERE reclaim_token = ? AND state = 'HELD'",
            (worker_id,),
        ).fetchall()
        return [r["lease_id"] for r in rows]

    CLAIM_GRACE = timedelta(minutes=5)

    def reclaim_stale_claims(
        self, conn: sqlite3.Connection, now: str, worker_id: str, limit: int
    ) -> list[str]:
        """接管崩溃清理者遗留的认领导约（孤儿认领恢复）。

        认领时间早于 ``now - CLAIM_GRACE`` 仍未终结的记录，改由当前
        worker 认领；宽限期内的不碰，避免与尚在运行的清理者竞争。
        """
        cutoff_iso = to_iso(parse_iso(now) - self.CLAIM_GRACE)
        conn.execute(
            """UPDATE lease
                  SET reclaim_token = ?, reclaim_at = ?
                WHERE lease_id IN (
                    SELECT lease_id FROM lease
                     WHERE state = 'HELD'
                       AND reclaim_token IS NOT NULL
                       AND reclaim_token <> ?
                       AND reclaim_at < ?
                     LIMIT ?
                )""",
            (worker_id, now, worker_id, cutoff_iso, limit),
        )
        rows = conn.execute(
            "SELECT lease_id FROM lease WHERE reclaim_token = ? AND state = 'HELD'",
            (worker_id,),
        ).fetchall()
        return [r["lease_id"] for r in rows]

    def mark_resource_invalid(
        self,
        conn: sqlite3.Connection,
        resource_type: str,
        resource_id: str,
        slot: str,
    ) -> list[str]:
        """标记某失效资源槽位，并找出受影响的 HELD 租约。

        资源行置 INVALID（释放唯一槽位）。若租约的全部资源都已
        RELEASED/INVALID，则整个暂占无法成立，由 service 层释放该租约；
        仍有有效资源的租约保留（部分失效，等待重配）。
        CONFIRMED 正式预约不受影响（走改期而非静默失效）。
        """
        conn.execute(
            """UPDATE lease_resource
                  SET state = 'INVALID'
                WHERE resource_type = ? AND resource_id = ? AND slot = ?
                  AND state = 'ACTIVE'
                  AND lease_id IN (SELECT lease_id FROM lease WHERE state = 'HELD')""",
            (resource_type, resource_id, slot),
        )
        rows = conn.execute(
            """SELECT DISTINCT lease_id FROM lease_resource
                WHERE resource_type = ? AND resource_id = ? AND slot = ?
                  AND state = 'INVALID'""",
            (resource_type, resource_id, slot),
        ).fetchall()
        return [r["lease_id"] for r in rows]

    def active_resource_count(self, conn: sqlite3.Connection, lease_id: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM lease_resource WHERE lease_id = ? AND state = 'ACTIVE'",
            (lease_id,),
        ).fetchone()
        return int(row["n"])

    def resource_states(self, conn: sqlite3.Connection, lease_id: str) -> list[dict[str, str]]:
        rows = conn.execute(
            """SELECT resource_type, resource_id, slot, state
                 FROM lease_resource WHERE lease_id = ?
             ORDER BY resource_type, resource_id, slot""",
            (lease_id,),
        ).fetchall()
        return [dict(r) for r in rows]


    # ── 查询（管理视图）──────────────────────────────────────────────
    def list_leases(
        self,
        *,
        state: str | None = None,
        holder: str | None = None,
        include_terminal: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM lease WHERE 1=1"
        args: list[Any] = []
        if state:
            sql += " AND state = ?"
            args.append(state)
        if holder:
            sql += " AND holder = ?"
            args.append(holder)
        if not include_terminal and not state:
            sql += " AND state NOT IN ('RELEASED','EXPIRED')"
        sql += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_dict(r) for r in rows if r is not None]  # type: ignore[misc]

    def events_conn(self, conn: sqlite3.Connection, lease_id: str) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT event, actor, detail, created_at FROM lease_event"
            " WHERE lease_id = ? ORDER BY seq",
            (lease_id,),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for r in rows:
            item = dict(r)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def list_events(self, lease_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return self.events_conn(self._conn, lease_id)

    def active_slots_for(
        self, conn: sqlite3.Connection, resources: Iterable[dict[str, str]]
    ) -> list[dict[str, str]]:
        """返回给定资源槽位中当前仍被活跃占用的部分（用于容量视图）。"""
        out: list[dict[str, str]] = []
        for r in resources:
            row = conn.execute(
                """SELECT l.lease_id, l.holder, l.state, l.expires_at
                     FROM lease_resource lr
                     JOIN lease l ON l.lease_id = lr.lease_id
                    WHERE lr.resource_type = ? AND lr.resource_id = ? AND lr.slot = ?
                      AND lr.state = 'ACTIVE'
                      AND l.state IN ('HELD','CONFIRMED') LIMIT 1""",
                (r["resource_type"], r["resource_id"], r["slot"]),
            ).fetchone()
            if row:
                out.append(dict(row))
        return out

    def lock(self):
        return self._lock

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn
