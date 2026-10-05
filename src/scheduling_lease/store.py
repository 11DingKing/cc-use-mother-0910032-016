"""SQLite 存储层。

所有状态转换都在单条 ``BEGIN IMMEDIATE`` 事务内完成，配合 WAL 模式保证
多个 API 进程与多个清理进程并发时的正确性：

* ``resource_hold`` 上的 ``(resource_type, resource_ref, slot_start)`` 唯一索引
  使同一资源同一时段的第二次占用在数据库层直接失败；
* 释放租约时以 ``WHERE status='HELD'`` 条件更新做"认领"，只有一个进程能把
  租约从 HELD 改成终结态（EXPIRED/CANCELLED/RECLAIMED/INVALIDATED/CONFIRMED）
  —— 这就是并发清理互斥；
* 每次成功修改 version 自增，作为确认时的乐观锁。
"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS lease (
    id              TEXT PRIMARY KEY,
    school_id       TEXT NOT NULL,
    contact         TEXT NOT NULL,
    source          TEXT NOT NULL,
    created_by      TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    expires_at      TEXT NOT NULL,
    token_hash      TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    status          TEXT NOT NULL DEFAULT 'HELD',
    resources_snapshot TEXT,
    released_at     TEXT,
    released_reason TEXT,
    released_by     TEXT,
    booking_id      TEXT
);

CREATE TABLE IF NOT EXISTS resource_hold (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_id     TEXT NOT NULL REFERENCES lease(id),
    resource_type TEXT NOT NULL,
    resource_ref  TEXT NOT NULL,
    slot_start   TEXT NOT NULL,
    slot_end     TEXT NOT NULL,
    idx          INTEGER NOT NULL,
    UNIQUE(resource_type, resource_ref, slot_start)
);

CREATE INDEX IF NOT EXISTS idx_hold_lease ON resource_hold(lease_id);
CREATE INDEX IF NOT EXISTS idx_lease_status_expires ON lease(status, expires_at);

CREATE TABLE IF NOT EXISTS confirmed_booking (
    booking_id   TEXT PRIMARY KEY,
    lease_id     TEXT NOT NULL UNIQUE,
    school_id    TEXT NOT NULL,
    resources    TEXT NOT NULL,
    confirmed_at TEXT NOT NULL,
    version      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS invalid_resource (
    resource_type TEXT NOT NULL,
    resource_ref  TEXT NOT NULL,
    reason        TEXT NOT NULL,
    invalidated_by TEXT NOT NULL,
    invalidated_at TEXT NOT NULL,
    PRIMARY KEY (resource_type, resource_ref)
);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    """统一的 UTC ISO8601 文本，按字符串排序即按时间排序。"""
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


class ConflictError(Exception):
    """组合资源中至少有一项已被其他有效租约/预约占用。"""

    def __init__(self, conflicts: list[dict]) -> None:
        self.conflicts = conflicts
        super().__init__("资源冲突：" + "、".join(c["resource_ref"] for c in conflicts))


class InvalidResourceError(Exception):
    """组合资源中包含已失效（请假/维修）的资源。"""

    def __init__(self, invalid: list[dict]) -> None:
        self.invalid = invalid
        super().__init__("资源已失效：" + "、".join(i["resource_ref"] for i in invalid))


class Store:
    """数据库操作集合。每个方法自行开启并提交 IMMEDIATE 事务。"""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------ holds

    def insert_lease(
        self,
        *,
        lease_id: str,
        school_id: str,
        contact: str,
        source: str,
        created_by: str,
        created_at: datetime,
        expires_at: datetime,
        token_hash: str,
        resources: list[dict],
    ) -> str:
        """原子地写入租约及其全部资源占用；任一资源冲突则整体回滚。"""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            invalid = self._find_invalid(conn, resources)
            if invalid:
                conn.rollback()
                raise InvalidResourceError(invalid)
            conflicts = self._find_conflicts(conn, resources, exclude_lease=None)
            if conflicts:
                conn.rollback()
                raise ConflictError(conflicts)
            conn.execute(
                """INSERT INTO lease
                   (id, school_id, contact, source, created_by, created_at,
                    expires_at, token_hash, version, status)
                   VALUES (?,?,?,?,?,?,?,?,1,'HELD')""",
                (lease_id, school_id, contact, source, created_by,
                 iso(created_at), iso(expires_at), token_hash),
            )
            try:
                conn.executemany(
                    """INSERT INTO resource_hold
                       (lease_id, resource_type, resource_ref, slot_start, slot_end, idx)
                       VALUES (?,?,?,?,?,?)""",
                    [
                        (lease_id, r["resource_type"], r["resource_ref"],
                         iso(r["slot_start"]), iso(r["slot_end"]), i)
                        for i, r in enumerate(resources)
                    ],
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                # 并发对同一资源同一整点时段的两个暂占，由唯一索引裁决。
                conn.rollback()
                raise ConflictError([{"resource_ref": "并发占用冲突"}]) from exc
            return lease_id
        finally:
            conn.close()

    @staticmethod
    def _find_conflicts(
        conn: sqlite3.Connection, resources: Iterable[dict], exclude_lease: str | None
    ) -> list[dict]:
        """找出与有效占用重叠的资源（同类型+同资源+时段相交）。

        有效占用包括 HELD 暂占与 CONFIRMED 正式预约；已释放租约的占用行
        已在终结时删除，不会误伤后续出租。
        """
        conflicts: list[dict] = []
        for r in resources:
            sql = """
                SELECT h.lease_id, h.resource_type, h.resource_ref,
                       h.slot_start, h.slot_end, l.school_id, l.source, l.status
                FROM resource_hold h
                JOIN lease l ON l.id = h.lease_id
                WHERE l.status IN ('HELD','CONFIRMED')
                  AND h.resource_type = ?
                  AND h.resource_ref = ?
                  AND h.slot_start < ? AND h.slot_end > ?
            """
            params: list = [r["resource_type"], r["resource_ref"],
                            iso(r["slot_end"]), iso(r["slot_start"])]
            if exclude_lease is not None:
                sql += " AND h.lease_id != ?"
                params.append(exclude_lease)
            row = conn.execute(sql, params).fetchone()
            if row is not None:
                conflicts.append(dict(row))
        return conflicts

    @staticmethod
    def _find_invalid(
        conn: sqlite3.Connection, resources: Iterable[dict]
    ) -> list[dict]:
        """返回组合中已被标记失效的资源（去重）。"""
        invalid: list[dict] = []
        seen: set[tuple] = set()
        for r in resources:
            key = (r["resource_type"], r["resource_ref"])
            if key in seen:
                continue
            row = conn.execute(
                "SELECT resource_type, resource_ref, reason, invalidated_by, "
                "invalidated_at FROM invalid_resource WHERE resource_type=? "
                "AND resource_ref=?",
                key,
            ).fetchone()
            if row is not None:
                seen.add(key)
                invalid.append(dict(row))
        return invalid

    def mark_invalid_and_release(
        self, refs: list[dict], *, reason: str, actor: str, now: datetime
    ) -> list[dict]:
        """登记失效资源并释放所有受影响的 HELD 租约（部分资源失效）。

        一个 IMMEDIATE 事务内完成：登记（INSERT OR IGNORE 保证重复登记幂等）
        + 条件更新认领受影响租约 + 快照删除占用。多进程/重复调用安全。
        返回被释放租约的摘要（含来源信息，供审计/通知）。
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                """INSERT OR IGNORE INTO invalid_resource
                   (resource_type, resource_ref, reason, invalidated_by, invalidated_at)
                   VALUES (?,?,?,?,?)""",
                [(r["resource_type"], r["resource_ref"], reason, actor, iso(now))
                 for r in refs],
            )
            placeholders = " OR ".join(
                ["(h.resource_type=? AND h.resource_ref=?)"] * len(refs)
            )
            rows = conn.execute(
                f"""SELECT DISTINCT l.id FROM lease l
                    JOIN resource_hold h ON h.lease_id = l.id
                    WHERE l.status='HELD' AND ({placeholders})""",
                [v for r in refs for v in (r["resource_type"], r["resource_ref"])],
            ).fetchall()
            released: list[dict] = []
            for row in rows:
                lease_id = row["id"]
                cur = conn.execute(
                    """UPDATE lease
                       SET status='INVALIDATED', released_at=?,
                           released_reason=?, released_by=?, version=version+1
                       WHERE id=? AND status='HELD'""",
                    (iso(now), "RESOURCE_INVALID:" + reason, actor, lease_id),
                )
                if cur.rowcount == 1:
                    lease = self.get_lease_row(conn, lease_id)
                    self._snapshot_and_release_holds(conn, lease_id)
                    released.append({
                        "id": lease_id,
                        "school_id": lease["school_id"],
                        "contact": lease["contact"],
                        "source": lease["source"],
                    })
            conn.commit()
            return released
        finally:
            conn.close()

    def list_invalid(self) -> list[dict]:
        conn = self._connect()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM invalid_resource ORDER BY invalidated_at"
            )]
        finally:
            conn.close()

    def get_lease_row(self, conn: sqlite3.Connection, lease_id: str) -> sqlite3.Row | None:
        return conn.execute("SELECT * FROM lease WHERE id=?", (lease_id,)).fetchone()

    @staticmethod
    def _snapshot_and_release_holds(conn: sqlite3.Connection, lease_id: str) -> None:
        """把组合资源快照写入租约后删除占用行，释放容量供他人预约。

        仅用于非正常终结（过期/取消/回收）；CONFIRMED 的占用行保留，
        继续代表正式预约占住资源。
        """
        rows = conn.execute(
            "SELECT resource_type, resource_ref, slot_start, slot_end "
            "FROM resource_hold WHERE lease_id=? ORDER BY idx",
            (lease_id,),
        ).fetchall()
        conn.execute(
            "UPDATE lease SET resources_snapshot=? WHERE id=?",
            (json.dumps([dict(r) for r in rows], ensure_ascii=False), lease_id),
        )
        conn.execute("DELETE FROM resource_hold WHERE lease_id=?", (lease_id,))

    def _lease_detail(self, conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
        result = dict(row)
        holds = [
            dict(r) for r in conn.execute(
                "SELECT resource_type, resource_ref, slot_start, slot_end "
                "FROM resource_hold WHERE lease_id=? ORDER BY idx",
                (row["id"],),
            )
        ]
        if holds:
            result["resources"] = holds
        elif result.get("resources_snapshot"):
            result["resources"] = json.loads(result["resources_snapshot"])
        else:
            result["resources"] = []
        return result

    def get_lease(self, lease_id: str) -> dict | None:
        conn = self._connect()
        try:
            row = self.get_lease_row(conn, lease_id)
            if row is None:
                return None
            return self._lease_detail(conn, row)
        finally:
            conn.close()

    def list_leases(self, *, status: str | None = None) -> list[dict]:
        conn = self._connect()
        try:
            if status:
                rows = conn.execute(
                    "SELECT * FROM lease WHERE status=? ORDER BY created_at", (status,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM lease ORDER BY created_at").fetchall()
            return [self._lease_detail(conn, row) for row in rows]
        finally:
            conn.close()

    # ------------------------------------------------------------- transitions

    def renew(
        self,
        lease_id: str,
        token_hash: str,
        new_expires_at: datetime,
        new_resources: list[dict] | None,
    ) -> tuple[str, int]:
        """续约（可顺带调整组合资源）。

        返回 (status, version)。status 为 ``ok`` / ``bad_token`` /
        ``not_held`` / ``conflict`` / ``not_found``。
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self.get_lease_row(conn, lease_id)
            if row is None:
                conn.rollback()
                return "not_found", 0
            if row["status"] != "HELD":
                conn.rollback()
                return "not_held", row["version"]
            if row["token_hash"] != token_hash:
                conn.rollback()
                return "bad_token", row["version"]
            if new_resources is not None:
                invalid = self._find_invalid(conn, new_resources)
                if invalid:
                    conn.rollback()
                    return "invalid_resource", row["version"]
                conflicts = self._find_conflicts(conn, new_resources, exclude_lease=lease_id)
                if conflicts:
                    conn.rollback()
                    return "conflict", row["version"]
                conn.execute("DELETE FROM resource_hold WHERE lease_id=?", (lease_id,))
                conn.executemany(
                    """INSERT INTO resource_hold
                       (lease_id, resource_type, resource_ref, slot_start, slot_end, idx)
                       VALUES (?,?,?,?,?,?)""",
                    [
                        (lease_id, r["resource_type"], r["resource_ref"],
                         iso(r["slot_start"]), iso(r["slot_end"]), i)
                        for i, r in enumerate(new_resources)
                    ],
                )
            conn.execute(
                "UPDATE lease SET expires_at=?, version=version+1 WHERE id=?",
                (iso(new_expires_at), lease_id),
            )
            conn.commit()
            return "ok", row["version"] + 1
        finally:
            conn.close()

    def confirm(
        self,
        lease_id: str,
        token_hash: str,
        expected_version: int,
        booking_id: str,
        confirmed_at: datetime,
    ) -> tuple[str, dict | None]:
        """暂占 -> 正式预约。

        必须同时持有正确令牌与版本号（版本比较）。重复确认返回首次结果，
        天然幂等。返回 (status, booking_info)。
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self.get_lease_row(conn, lease_id)
            if row is None:
                conn.rollback()
                return "not_found", None

            existing = conn.execute(
                "SELECT * FROM confirmed_booking WHERE lease_id=?", (lease_id,)
            ).fetchone()
            if existing is not None:
                # 重复确认：原样返回首次结果，不做任何修改。
                info = dict(existing)
                conn.rollback()
                return "already_confirmed", info

            if row["status"] != "HELD":
                conn.rollback()
                return "not_held", None
            if row["token_hash"] != token_hash:
                conn.rollback()
                return "bad_token", None
            if row["version"] != expected_version:
                conn.rollback()
                return "version_conflict", {"current_version": row["version"]}

            resources = [
                dict(r) for r in conn.execute(
                    "SELECT resource_type, resource_ref, slot_start, slot_end "
                    "FROM resource_hold WHERE lease_id=? ORDER BY idx",
                    (lease_id,),
                )
            ]
            cur = conn.execute(
                """UPDATE lease
                   SET status='CONFIRMED', version=version+1, booking_id=?,
                       released_at=?, released_reason='CONFIRMED', released_by='holder'
                   WHERE id=? AND status='HELD'""",
                (booking_id, iso(confirmed_at), lease_id),
            )
            # 唯一条件更新兜底：理论上前面已检查，这里确保并发下只有一方成功。
            if cur.rowcount != 1:
                conn.rollback()
                return "not_held", None
            conn.execute(
                """INSERT INTO confirmed_booking
                   (booking_id, lease_id, school_id, resources, confirmed_at, version)
                   VALUES (?,?,?,?,?,?)""",
                (booking_id, lease_id, row["school_id"],
                 json.dumps(resources, ensure_ascii=False),
                 iso(confirmed_at), row["version"] + 1),
            )
            conn.commit()
            info = {
                "booking_id": booking_id,
                "lease_id": lease_id,
                "school_id": row["school_id"],
                "resources": json.dumps(resources, ensure_ascii=False),
                "confirmed_at": iso(confirmed_at),
                "version": row["version"] + 1,
            }
            return "confirmed", info
        finally:
            conn.close()

    def claim_expired(self, now: datetime, worker_id: str, limit: int = 100) -> list[str]:
        """超时扫描：原子认领一批已过期且仍 HELD 的租约并释放。

        ``UPDATE ... WHERE status='HELD' AND expires_at<=?`` 的条件更新是
        互斥点：两个清理进程同时扫描，只有一个 UPDATE 能命中每一行
        （行锁串行化后第二个的 WHERE 不再成立），因此不会重复释放。
        返回被本进程实际释放的租约 id。
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """SELECT id FROM lease
                   WHERE status='HELD' AND expires_at<=?
                   ORDER BY expires_at LIMIT ?""",
                (iso(now), limit),
            ).fetchall()
            ids = [r["id"] for r in rows]
            claimed: list[str] = []
            for i in ids:
                cur = conn.execute(
                    """UPDATE lease
                       SET status='EXPIRED', released_at=?,
                           released_reason='TIMEOUT', released_by=?
                       WHERE id=? AND status='HELD'""",
                    (iso(now), worker_id, i),
                )
                if cur.rowcount == 1:
                    self._snapshot_and_release_holds(conn, i)
                    claimed.append(i)
            conn.commit()
            return claimed
        finally:
            conn.close()

    def cancel(
        self, lease_id: str, token_hash: str | None, now: datetime, actor: str
    ) -> tuple[str, dict | None]:
        """主动取消。

        持有人凭令牌取消；管理员（token_hash 传 None）可强制回收。
        对已终结租约再次取消返回当前状态，保持幂等。
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self.get_lease_row(conn, lease_id)
            if row is None:
                conn.rollback()
                return "not_found", None
            if row["status"] != "HELD":
                conn.rollback()
                return "already_" + row["status"].lower(), dict(row)
            if token_hash is not None and row["token_hash"] != token_hash:
                conn.rollback()
                return "bad_token", None
            reason = "CANCELLED" if token_hash is not None else "ADMIN_RECLAIM"
            cur = conn.execute(
                """UPDATE lease
                   SET status=?, released_at=?, released_reason=?,
                       released_by=?, version=version+1
                   WHERE id=? AND status='HELD'""",
                ("CANCELLED" if token_hash is not None else "RECLAIMED",
                 iso(now), reason, actor, lease_id),
            )
            assert cur.rowcount == 1
            self._snapshot_and_release_holds(conn, lease_id)
            conn.commit()
            new_status = "CANCELLED" if token_hash is not None else "RECLAIMED"
            return "cancelled", {"id": lease_id, "status": new_status, "reason": reason}
        finally:
            conn.close()

    def reclaim_orphans(
        self, now: datetime, worker_id: str, *, older_than: datetime | None = None,
        lease_ids: list[str] | None = None,
    ) -> list[dict]:
        """孤儿回收：把异常遗留的 HELD 记录安全终结。

        可指定具体租约 id（管理回收），或回收"过期且宽限期已过"的全部记录
        （孤儿扫描）。与 claim_expired 走同一条条件更新，多进程安全。
        返回 [{id, claimed: bool, previous_status}]。
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if lease_ids is not None:
                rows = [self.get_lease_row(conn, i) for i in lease_ids]
                targets = [r for r in rows if r is not None]
                missing = [i for i, r in zip(lease_ids, rows) if r is None]
            else:
                cutoff = older_than or now
                targets = conn.execute(
                    """SELECT * FROM lease
                       WHERE status='HELD' AND expires_at<=?
                       ORDER BY expires_at""",
                    (iso(cutoff),),
                ).fetchall()
                missing = []
            results: list[dict] = []
            for r in targets:
                cur = conn.execute(
                    """UPDATE lease
                       SET status='RECLAIMED', released_at=?,
                           released_reason='ORPHAN_RECLAIM', released_by=?
                       WHERE id=? AND status='HELD'""",
                    (iso(now), worker_id, r["id"]),
                )
                if cur.rowcount == 1:
                    self._snapshot_and_release_holds(conn, r["id"])
                results.append({
                    "id": r["id"],
                    "claimed": cur.rowcount == 1,
                    "previous_status": r["status"],
                })
            for m in missing:
                results.append({"id": m, "claimed": False, "previous_status": None})
            conn.commit()
            return results
        finally:
            conn.close()

    def get_booking(self, booking_id: str) -> dict | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM confirmed_booking WHERE booking_id=?", (booking_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
