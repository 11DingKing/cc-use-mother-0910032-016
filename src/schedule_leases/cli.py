"""管理命令：python -m schedule_leases <command>。

子命令
------
- ``serve``            启动 HTTP API（--db --host --port --sweep-interval）
- ``list``             查看占用（--state --holder --all），显示占用来源
- ``show <lease_id>``  查看单条占用来源、资源与事件流水
- ``sweep``            执行一轮超时回收（--worker-id；可多进程并发）
- ``release <id>``     安全回收孤儿暂占（--reason 必填；不动正式预约）
- ``invalidate``       声明资源槽位失效（--type --id --slot）
- ``capacity``         查询槽位占用（JSON 资源列表文件或 --resources）
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from typing import Any, Sequence

from .server import SweeperThread, build_server
from .service import LeaseService, ServiceError
from .store import LeaseStore


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _lease_row(lease: dict[str, Any]) -> dict[str, Any]:
    return {
        "lease_id": lease["lease_id"],
        "state": lease["state"],
        "version": lease["version"],
        "holder": lease["holder"],
        "holder_kind": lease["holder_kind"],
        "expires_at": lease["expires_at"],
        "reclaimed_by": lease["reclaimed_by"],
        "cancel_reason": lease["cancel_reason"],
        "resources": lease["resources"],
    }


def cmd_serve(args: argparse.Namespace) -> int:
    server = build_server(args.db, args.host, args.port)
    sweeper = None
    if args.sweep_interval:
        sweeper = SweeperThread(
            server.service, args.sweep_interval, f"bg-{uuid.uuid4().hex[:8]}"
        )
        sweeper.start()
    print(f"listening on http://{args.host}:{args.port} (db={args.db})", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if sweeper:
            sweeper.stop()
        server.server_close()
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    store = LeaseStore(args.db)
    svc = LeaseService(store)
    leases = svc.list_leases(
        state=args.state, holder=args.holder, include_terminal=args.all
    )
    _print({"count": len(leases), "leases": [_lease_row(x) for x in leases]})
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    svc = LeaseService(LeaseStore(args.db))
    _print(svc.detail(args.lease_id))
    return 0


def cmd_sweep(args: argparse.Namespace) -> int:
    svc = LeaseService(LeaseStore(args.db))
    result = svc.sweep(args.worker_id or f"cli-{uuid.uuid4().hex[:8]}")
    _print(result)
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    svc = LeaseService(LeaseStore(args.db))
    try:
        lease = svc.admin_release(args.lease_id, reason=args.reason, actor=args.actor)
    except ServiceError as exc:
        print(f"error: {exc.code} {exc.message}", file=sys.stderr)
        return 2
    _print(_lease_row(lease))
    return 0


def cmd_invalidate(args: argparse.Namespace) -> int:
    svc = LeaseService(LeaseStore(args.db))
    result = svc.invalidate_resource(args.type, args.id, args.slot, actor=args.actor)
    _print(result)
    return 0


def cmd_capacity(args: argparse.Namespace) -> int:
    svc = LeaseService(LeaseStore(args.db))
    if args.resources:
        resources = json.loads(args.resources)
    else:
        resources = json.loads(sys.stdin.read())
    _print(svc.capacity(resources))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="schedule_leases", description="排班资源暂占恢复管理命令")
    parser.add_argument("--db", default="leases.db", help="SQLite 数据库路径")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("serve", help="启动 HTTP API")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--sweep-interval", type=float, default=0.0, help="后台扫描间隔秒，0=不启用")
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("list", help="查看占用与来源")
    p.add_argument("--state")
    p.add_argument("--holder")
    p.add_argument("--all", action="store_true", help="包含已终结记录")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("show", help="查看单条占用详情")
    p.add_argument("lease_id")
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("sweep", help="执行一轮超时回收（可并发多进程）")
    p.add_argument("--worker-id")
    p.set_defaults(func=cmd_sweep)

    p = sub.add_parser("release", help="安全回收孤儿暂占")
    p.add_argument("lease_id")
    p.add_argument("--reason", required=True, help="回收原因（审计）")
    p.add_argument("--actor", default="cli-admin")
    p.set_defaults(func=cmd_release)

    p = sub.add_parser("invalidate", help="声明资源槽位失效")
    p.add_argument("--type", required=True, dest="type")
    p.add_argument("--id", required=True, dest="id")
    p.add_argument("--slot", required=True)
    p.add_argument("--actor", default="cli-admin")
    p.set_defaults(func=cmd_invalidate)

    p = sub.add_parser("capacity", help="查询槽位占用")
    p.add_argument("--resources", help="JSON 数组；省略则从 stdin 读取")
    p.set_defaults(func=cmd_capacity)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except ServiceError as exc:
        print(f"error: {exc.code} {exc.message}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
