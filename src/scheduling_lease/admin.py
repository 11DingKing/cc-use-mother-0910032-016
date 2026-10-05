"""管理命令：查看占用来源、超时扫描、安全回收孤儿记录。

用法（见 tools/lease_admin.py 包装脚本）：

    python -m scheduling_lease.admin list [--status HELD] [--json]
    python -m scheduling_lease.admin show <lease_id> [--json]
    python -m scheduling_lease.admin sweep
    python -m scheduling_lease.admin reclaim [--id ID ...] [--grace 秒]
    python -m scheduling_lease.admin cancel <lease_id> [--actor 名称]
    python -m scheduling_lease.admin invalidate <类型:引用> ... [--reason 原因]
    python -m scheduling_lease.admin list-invalid
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from datetime import datetime

from .service import LeaseError, LeaseService

STATUS_LABEL = {
    "HELD": "暂占中",
    "CONFIRMED": "已确认",
    "EXPIRED": "已超时释放",
    "CANCELLED": "已取消",
    "RECLAIMED": "已回收",
    "INVALIDATED": "因资源失效释放",
}


def _fmt_dt(value: str | None) -> str:
    if not value:
        return "-"
    return datetime.fromisoformat(value).strftime("%m-%d %H:%M:%S")


def _print_table(leases: list[dict]) -> None:
    if not leases:
        print("（无记录）")
        return
    headers = ["租约", "学校", "来源", "经手", "状态", "到期", "释放原因", "资源数"]
    rows = []
    for l in leases:
        rows.append([
            l["lease_id"][:18],
            l["school_id"],
            l["source"],
            l["created_by"],
            STATUS_LABEL.get(l["status"], l["status"]),
            _fmt_dt(l["expires_at"]),
            l["released_reason"] or "-",
            str(len(l["resources"])),
        ])
    widths = [
        max(len(headers[i]), max((len(r[i]) for r in rows), default=0))
        for i in range(len(headers))
    ]
    print("  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(r[i].ljust(widths[i]) for i in range(len(headers))))


def _print_detail(lease: dict) -> None:
    print(f"租约 {lease['lease_id']}")
    print(f"  学校/联系人 : {lease['school_id']} / {lease['contact']}")
    print(f"  占用来源    : {lease['source']}（经手人 {lease['created_by']}）")
    print(f"  状态        : {STATUS_LABEL.get(lease['status'], lease['status'])}")
    print(f"  版本        : {lease['version']}")
    print(f"  创建/到期   : {_fmt_dt(lease['created_at'])} / {_fmt_dt(lease['expires_at'])}")
    if lease["released_at"]:
        print(f"  释放        : {_fmt_dt(lease['released_at'])} "
              f"{lease['released_reason']} by {lease['released_by']}")
    print("  组合资源:")
    for r in lease["resources"]:
        print(f"    - [{r['resource_type']}] {r['resource_ref']} "
              f"{_fmt_dt(r['slot_start'])}~{_fmt_dt(r['slot_end'])}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="排班暂占管理命令")
    parser.add_argument("--db", default=os.environ.get("LEASE_DB", "./data/scheduling.sqlite3"))
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="列出暂占记录（默认全部状态）").add_argument(
        "--status", default=None, help="按状态过滤，如 HELD/EXPIRED")

    p_show = sub.add_parser("show", help="查看单条记录详情")
    p_show.add_argument("lease_id")

    p_sweep = sub.add_parser("sweep", help="超时扫描一轮")
    p_sweep.add_argument("--worker", default=None)

    p_rec = sub.add_parser("reclaim", help="回收孤儿记录")
    p_rec.add_argument("--id", dest="ids", nargs="*", help="指定租约 id")
    p_rec.add_argument("--grace", type=int, default=0, help="仅回收过期超过 N 秒者")
    p_rec.add_argument("--worker", default=None)

    p_cancel = sub.add_parser("cancel", help="管理员强制取消单条暂占")
    p_cancel.add_argument("lease_id")
    p_cancel.add_argument("--actor", default="admin-cli")

    p_inv = sub.add_parser("invalidate", help="标记资源失效并释放受影响暂占")
    p_inv.add_argument("refs", nargs="+", help="格式 类型:引用，如 guide:g-007")
    p_inv.add_argument("--reason", default="")
    p_inv.add_argument("--actor", default="admin-cli")

    sub.add_parser("list-invalid", help="列出已失效资源")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    service = LeaseService(args.db)
    worker = getattr(args, "worker", None) or f"{socket.gethostname()}:admin:{os.getpid()}"

    try:
        if args.cmd == "list":
            leases = service.list_leases(args.status)
            if args.json:
                print(json.dumps(leases, ensure_ascii=False, indent=2))
            else:
                _print_table(leases)

        elif args.cmd == "show":
            lease = service.get_lease(args.lease_id)
            if args.json:
                print(json.dumps(lease, ensure_ascii=False, indent=2))
            else:
                _print_detail(lease)

        elif args.cmd == "sweep":
            result = service.sweep_expired(worker)
            print(json.dumps(result, ensure_ascii=False, indent=2) if args.json
                  else f"本轮释放 {result['count']} 条：{', '.join(result['released']) or '无'}")

        elif args.cmd == "reclaim":
            result = service.reclaim_orphans(
                worker, lease_ids=args.ids, grace_seconds=args.grace)
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                print(f"回收 {result['count']} 条：{', '.join(result['released']) or '无'}")
                for r in result["results"]:
                    if not r["claimed"]:
                        print(f"  跳过 {r['id']}（当前状态 {r['previous_status']}，无需回收）")

        elif args.cmd == "cancel":
            result = service.cancel(args.lease_id, None, actor=args.actor)
            print(json.dumps(result, ensure_ascii=False, indent=2) if args.json
                  else f"{args.lease_id} -> {result['status']}"
                       f"{'（重复操作，幂等）' if result.get('idempotent') else ''}")

        elif args.cmd == "invalidate":
            refs = []
            for item in args.refs:
                try:
                    rtype, ref = item.split(":", 1)
                except ValueError:
                    print(f"忽略格式错误的资源：{item}（应为 类型:引用）", file=sys.stderr)
                    continue
                refs.append({"resource_type": rtype, "resource_ref": ref})
            result = service.invalidate_resources(
                refs, reason=args.reason, actor=args.actor)
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                print(f"登记失效资源 {len(refs)} 项，释放暂占 {result['count']} 条：")
                for l in result["released_leases"]:
                    print(f"  - {l['id']} 学校={l['school_id']} 来源={l['source']}")

        elif args.cmd == "list-invalid":
            rows = service.store.list_invalid()
            print(json.dumps(rows, ensure_ascii=False, indent=2) if args.json
                  else "\n".join(f"[{r['resource_type']}] {r['resource_ref']} "
                                 f"原因={r['reason']} 登记人={r['invalidated_by']}"
                                 for r in rows) or "（无）")
    except LeaseError as exc:
        print(f"错误[{exc.code}]：{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
