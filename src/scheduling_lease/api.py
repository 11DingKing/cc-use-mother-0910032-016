"""HTTP API（仅依赖标准库）。

持有人接口（请求体携带租约 token）：

* ``POST /v1/leases``                 暂占组合资源
* ``GET  /v1/leases``                 查看占用（含来源 source/created_by）
* ``GET  /v1/leases/{id}``            查看单条暂占
* ``POST /v1/leases/{id}/renew``      续约（可调整组合资源）
* ``POST /v1/leases/{id}/confirm``    版本比较后转正式预约（幂等）
* ``POST /v1/leases/{id}/cancel``     持有人主动取消（幂等）
* ``GET  /v1/bookings/{id}``          查询正式预约

管理接口（需 ``X-Admin-Token``，值为启动时环境变量 SCHEDULING_ADMIN_TOKEN）：

* ``POST /v1/admin/sweep``            超时扫描一轮
* ``POST /v1/admin/reclaim``          安全回收孤儿记录
* ``POST /v1/admin/invalid-resources`` 标记部分资源失效并释放受影响暂占
* ``GET  /v1/admin/invalid-resources`` 查看失效资源

所有终结类操作在服务端均为幂等，重复调用返回同样的最终状态。
"""
from __future__ import annotations

import json
import os
import secrets
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .service import LeaseError, LeaseService

ERROR_STATUS = {
    "not_found": 404,
    "not_found_route": 404,
    "bad_token": 403,
    "forbidden": 403,
    "conflict": 409,
    "version_conflict": 409,
    "not_held": 409,
    "invalid_resource": 422,
    "empty_resources": 400,
    "bad_resource": 400,
    "bad_slot": 400,
    "duplicate_resource": 400,
    "bad_ttl": 400,
    "bad_json": 400,
    "missing_field": 400,
}


def worker_default() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{secrets.token_hex(3)}"


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "SchedulingLease/1.0"

    # 注入点：ThreadingHTTPServer 会把下列属性挂在 server 上。
    @property
    def service(self) -> LeaseService:
        return self.server.service  # type: ignore[attr-defined]

    @property
    def admin_token(self) -> str:
        return self.server.admin_token  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args) -> None:  # 安静一点
        if os.environ.get("LEASE_API_LOG"):
            super().log_message(fmt, *args)

    # ------------------------------------------------------------- plumbing

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise LeaseError("bad_json", "请求体不是合法 JSON") from exc
        if not isinstance(value, dict):
            raise LeaseError("bad_json", "请求体必须是 JSON 对象")
        return value

    def _send(self, status: int, payload: dict | list) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _require_admin(self) -> None:
        if not self.admin_token:
            return  # 未配置管理口令（如本地测试），不拦
        provided = self.headers.get("X-Admin-Token", "")
        if not secrets.compare_digest(provided, self.admin_token):
            raise LeaseError("forbidden", "缺少或错误的 X-Admin-Token")

    def _handle(self, fn) -> None:
        try:
            fn()
        except LeaseError as exc:
            status = ERROR_STATUS.get(exc.code, 403 if exc.code == "forbidden" else 400)
            self._send(status, {"error": exc.code, "message": str(exc), **exc.extra})
        except KeyError as exc:
            self._send(400, {"error": "missing_field",
                             "message": f"缺少必填字段：{exc.args[0]}"})
        except (ValueError, TypeError) as exc:
            self._send(400, {"error": "bad_request", "message": f"参数无效：{exc}"})

    # --------------------------------------------------------------- routing

    def do_GET(self) -> None:  # noqa: N802
        self._handle(self._route_get)

    def do_POST(self) -> None:  # noqa: N802
        self._handle(self._route_post)

    def _route_get(self) -> None:
        path = urlparse(self.path).path.strip("/").split("/")
        # ['v1', 'leases', ...]
        if path[:2] == ["v1", "leases"]:
            if len(path) == 2:
                query = urlparse(self.path).query
                status = None
                for pair in query.split("&"):
                    if pair.startswith("status="):
                        from urllib.parse import unquote

                        status = unquote(pair.split("=", 1)[1])
                self._send(200, self.service.list_leases(status))
                return
            if len(path) == 3:
                self._send(200, self.service.get_lease(path[2]))
                return
        if path[:2] == ["v1", "bookings"] and len(path) == 3:
            self._send(200, self.service.get_booking(path[2]))
            return
        if path == ["v1", "admin", "invalid-resources"]:
            self._require_admin()
            self._send(200, self.service.store.list_invalid())
            return
        raise LeaseError("not_found_route", "未知接口")

    def _route_post(self) -> None:
        path = urlparse(self.path).path.strip("/").split("/")
        body = self._read_json()

        if path == ["v1", "leases"]:
            result = self.service.hold(
                school_id=body["school_id"],
                contact=body.get("contact", ""),
                source=body.get("source", "api"),
                created_by=body.get("created_by", body.get("contact", "")),
                resources=body["resources"],
                ttl_seconds=int(body.get("ttl_seconds", 900)),
            )
            self._send(201, result)
            return

        if path[:2] == ["v1", "leases"] and len(path) == 4:
            lease_id, action = path[2], path[3]
            if action == "renew":
                result = self.service.renew(
                    lease_id, body["token"],
                    ttl_seconds=int(body.get("ttl_seconds", 900)),
                    resources=body.get("resources"),
                )
                self._send(200, result)
                return
            if action == "confirm":
                result = self.service.confirm(
                    lease_id, body["token"], body["version"],
                    booking_id=body.get("booking_id"),
                )
                # 首次确认 201；重复确认 200 且 idempotent=true
                self._send(200 if result.get("idempotent") else 201, result)
                return
            if action == "cancel":
                result = self.service.cancel(
                    lease_id, body.get("token"),
                    actor=body.get("actor", "holder"),
                )
                self._send(200, result)
                return

        if path == ["v1", "admin", "sweep"]:
            self._require_admin()
            result = self.service.sweep_expired(
                body.get("worker_id") or worker_default(),
                batch=int(body.get("batch", 100)),
            )
            self._send(200, result)
            return

        if path == ["v1", "admin", "reclaim"]:
            self._require_admin()
            result = self.service.reclaim_orphans(
                body.get("worker_id") or worker_default(),
                lease_ids=body.get("lease_ids"),
                grace_seconds=int(body.get("grace_seconds", 0)),
            )
            self._send(200, result)
            return

        if path == ["v1", "admin", "invalid-resources"]:
            self._require_admin()
            result = self.service.invalidate_resources(
                body["resources"], reason=body.get("reason", ""),
                actor=body.get("actor", "admin"),
            )
            self._send(200, result)
            return

        # 管理员强制回收单条暂占
        if path[:3] == ["v1", "admin", "leases"] and len(path) == 5 and path[4] == "reclaim":
            self._require_admin()
            result = self.service.cancel(
                path[3], None, actor=body.get("actor", "admin"),
            )
            self._send(200, result)
            return

        raise LeaseError("not_found_route", "未知接口")


def build_server(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), ApiHandler)
    server.service = LeaseService(db_path)  # type: ignore[attr-defined]
    server.admin_token = os.environ.get("SCHEDULING_ADMIN_TOKEN", "")  # type: ignore[attr-defined]
    return server


def serve_forever(db_path: str, host: str, port: int) -> None:
    server = build_server(db_path, host, port)
    print(f"排班暂占服务监听 http://{host}:{port}（DB={db_path}）", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="排班资源暂占恢复 HTTP 服务")
    parser.add_argument("--db", default=os.environ.get("LEASE_DB", "./data/scheduling.sqlite3"))
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("LEASE_PORT", "8080")))
    args = parser.parse_args()
    serve_forever(args.db, args.host, args.port)
