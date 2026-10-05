"""HTTP API（仅标准库 http.server）。

路由
----
- ``POST /leases``                 暂占组合资源
- ``GET  /leases/{id}``            查看占用（含来源与事件流水）
- ``GET  /leases``                 列表（?state=&holder=&all=）
- ``POST /leases/{id}/renew``      续约
- ``POST /leases/{id}/confirm``    比较版本并确认
- ``POST /leases/{id}/cancel``     持有人主动取消
- ``POST /capacity``               容量查询（排查失真）
- ``POST /admin/invalidate``       声明资源槽位失效
- ``POST /admin/sweep``            触发一轮超时回收（?worker_id=）
- ``POST /admin/leases/{id}/release``  安全回收孤儿记录

所有响应为 JSON；业务错误返回 ``{"error": code, "message": ...}``。
"""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from .service import LeaseService, ServiceError
from .store import LeaseStore


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class _Handler(BaseHTTPRequestHandler):
    service: LeaseService  # 由工厂注入到类上

    def log_message(self, fmt: str, *args: Any) -> None:  # 安静日志
        return

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ServiceError("bad_json", "请求体不是合法 JSON", 400)
        if not isinstance(data, dict):
            raise ServiceError("bad_json", "请求体必须是 JSON 对象", 400)
        return data

    def _handle(self, fn: Callable[[], Any]) -> None:
        try:
            result, status = fn()
        except ServiceError as exc:
            _json_response(self, exc.status, {"error": exc.code, "message": exc.message})
        except Exception as exc:  # noqa: BLE001
            _json_response(self, 500, {"error": "internal", "message": str(exc)})
        else:
            _json_response(self, status, result)

    # ── GET ───────────────────────────────────────────────────────────
    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/health":
            return self._handle(lambda: ({"ok": True}, 200))
        if path == "/leases":
            return self._handle(self._list)
        m = re.fullmatch(r"/leases/([0-9a-fA-F]{32})", path)
        if m:
            lease_id = m.group(1)
            return self._handle(lambda: (self.service.detail(lease_id), 200))
        _json_response(self, 404, {"error": "not_found", "message": "未知路径"})

    def _list(self) -> tuple[Any, int]:
        from urllib.parse import parse_qs, urlsplit

        qs = parse_qs(urlsplit(self.path).query)
        leases = self.service.list_leases(
            state=qs.get("state", [None])[0],
            holder=qs.get("holder", [None])[0],
            include_terminal=qs.get("all", ["0"])[0] in ("1", "true"),
            limit=int(qs.get("limit", ["100"])[0]),
        )
        return {"leases": leases, "count": len(leases)}, 200

    # ── POST ──────────────────────────────────────────────────────────
    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        routes: dict[str, Callable[[dict[str, Any]], Any]] = {
            "/leases": self._hold,
            "/capacity": self._capacity,
            "/admin/invalidate": self._invalidate,
            "/admin/sweep": self._sweep,
        }
        if path in routes:
            return self._handle(lambda: (routes[path](self._read_json()), 200))

        m = re.fullmatch(r"/leases/([0-9a-fA-F]{32})/(renew|confirm|cancel)", path)
        if m:
            lease_id, action = m.group(1), m.group(2)
            return self._handle(
                lambda: (self._lease_action(action, lease_id, self._read_json()), 200)
            )
        m = re.fullmatch(r"/admin/leases/([0-9a-fA-F]{32})/release", path)
        if m:
            lease_id = m.group(1)
            return self._handle(
                lambda: (self._admin_release(lease_id, self._read_json()), 200)
            )
        _json_response(self, 404, {"error": "not_found", "message": "未知路径"})

    def _hold(self, data: dict[str, Any]) -> Any:
        return self.service.hold(
            holder=str(data.get("holder", "")),
            holder_kind=str(data.get("holder_kind", "service")),
            resources=data.get("resources", []),
            ttl_seconds=data.get("ttl_seconds"),
            expected_capacity_version=int(data.get("expected_capacity_version", 0)),
        )

    def _lease_action(self, action: str, lease_id: str, data: dict[str, Any]) -> Any:
        token = str(data.get("renew_token", ""))
        if action == "renew":
            return self.service.renew(
                lease_id, token, ttl_seconds=data.get("ttl_seconds")
            )
        if action == "confirm":
            if "expected_version" not in data:
                raise ServiceError("bad_request", "缺少 expected_version", 400)
            return self.service.confirm(
                lease_id,
                token=token,
                expected_version=int(data["expected_version"]),
                confirmation_id=data.get("confirmation_id"),
            )
        return self.service.cancel(
            lease_id, token, reason=str(data.get("reason", "holder_cancel"))
        )

    def _capacity(self, data: dict[str, Any]) -> Any:
        return self.service.capacity(data.get("resources", []))

    def _invalidate(self, data: dict[str, Any]) -> Any:
        for key in ("resource_type", "resource_id", "slot"):
            if not data.get(key):
                raise ServiceError("bad_request", f"缺少 {key}", 400)
        return self.service.invalidate_resource(
            str(data["resource_type"]),
            str(data["resource_id"]),
            str(data["slot"]),
            actor=str(data.get("actor", "admin")),
        )

    def _sweep(self, data: dict[str, Any]) -> Any:
        import uuid

        worker_id = str(data.get("worker_id") or uuid.uuid4().hex)
        return self.service.sweep(worker_id)

    def _admin_release(self, lease_id: str, data: dict[str, Any]) -> Any:
        return self.service.admin_release(
            lease_id,
            reason=str(data.get("reason", "")),
            actor=str(data.get("actor", "admin")),
        )


def build_server(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    store = LeaseStore(db_path)
    service = LeaseService(store)
    handler = type("Handler", (_Handler,), {"service": service})
    server = ThreadingHTTPServer((host, port), handler)
    server.service = service  # type: ignore[attr-defined]
    server.store = store  # type: ignore[attr-defined]
    return server


class SweeperThread(threading.Thread):
    """可选的后台超时扫描线程（生产中也可由 cron 多进程执行）。"""

    def __init__(self, service: LeaseService, interval_seconds: float, worker_id: str) -> None:
        super().__init__(daemon=True, name=f"sweeper-{worker_id}")
        self.service = service
        self.interval = interval_seconds
        self.worker_id = worker_id
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.service.sweep(self.worker_id)
            except Exception:  # noqa: BLE001
                pass
