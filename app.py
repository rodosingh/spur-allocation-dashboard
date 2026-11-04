#!/usr/bin/env python3
"""Dependency-free localhost control plane for AMD SPUR allocations."""

from __future__ import annotations

import argparse
import errno
import json
import os
import secrets
import socket
import sys
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

import node_holder_bridge as holder
import scheduler
from request_store import RequestStore


ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
CSRF_TOKEN = secrets.token_urlsafe(32)
STORE = RequestStore()
MUTATION_LOCK = threading.Lock()
READ_ONLY = os.environ.get("SPUR_DASHBOARD_READ_ONLY", "") == "1"

PREFERRED_PORT = 8876
PORT_SCAN_SPAN = 64
MAX_PORT = 65535


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def capabilities() -> dict[str, Any]:
    return {
        "schemaVersion": 1,
        "chainPrefix": holder.CHAIN_PREFIX,
        "normalPrefix": holder.NORMAL_PREFIX,
        "minPriority": holder.MIN_PRIORITY,
        "partition": scheduler.PARTITION,
        "user": scheduler.USERNAME,
        "requestModes": ["chain", "normal"],
        "chainStrategies": ["start", "adopt", "race"],
        "chainActions": sorted(holder.SAFE_ACTIONS),
        "limits": {
            "gpus": {"min": 1, "max": 8},
            "nodes": {"min": 1, "max": 32},
            "chainDepth": {"min": 0, "max": 64},
        },
        "cliOnly": ["shell", "exec", "tick", "stop", "release --all"],
    }


def build_status() -> dict[str, Any]:
    errors: dict[str, str] = {}
    try:
        jobs = scheduler.get_queue("mine")
    except scheduler.SchedulerCommandError as error:
        jobs = []
        errors["queue"] = str(error)
    try:
        chain_status = holder.get_status(jobs)
    except (scheduler.SchedulerCommandError, holder.BridgeError) as error:
        chain_status = {"chains": []}
        errors["chains"] = str(error)
    errors.update(chain_status.get("errors", {}))

    chain_names = holder.chain_names(chain_status)
    for job in jobs:
        job["kind"] = "chain" if job["name"] in chain_names else "normal"
        job["isMine"] = True
    chains = chain_status.get("chains", [])
    for chain in chains:
        name = str(chain.get("name", ""))
        chain["copyShellCommand"] = " ".join(
            [
                f"NODEHOLD_NAME={name}",
                str(holder.NODE_HOLDER),
                "shell",
            ]
        )
        chain["copyExecPrefix"] = " ".join(
            [
                f"NODEHOLD_NAME={name}",
                str(holder.NODE_HOLDER),
                "exec",
            ]
        )
        chain["activeJobIds"] = [
            job["id"] for job in jobs if job["name"] == name
        ]

    running = [job for job in jobs if job["state"] == "RUNNING"]
    pending = [job for job in jobs if job["state"] == "PENDING"]
    return {
        "schemaVersion": 1,
        "updatedAt": utc_now(),
        "summary": {
            "runningJobs": len(running),
            "pendingJobs": len(pending),
            "runningNodes": sum(job["nodes"] for job in running),
            "chains": len(chains),
        },
        "chains": chains,
        "jobs": jobs,
        "errors": errors,
    }


def build_pools() -> dict[str, Any]:
    pools = holder.get_pools()
    jobs = scheduler.get_queue("all")
    for job in jobs:
        job["isMine"] = job["user"] == scheduler.USERNAME
    by_qos: dict[str, list[dict[str, Any]]] = {}
    for job in jobs:
        by_qos.setdefault(job["qos"], []).append(job)
    for pool in pools.get("pools", []):
        pool["jobs"] = by_qos.get(str(pool.get("qos", "")), [])
    pools["updatedAt"] = utc_now()
    return pools


def build_top_users(account: str) -> dict[str, Any]:
    data = holder.get_top_users(account)
    data["updatedAt"] = utc_now()
    return data


def probe_pool_seats(payload: dict[str, Any]) -> dict[str, Any]:
    account = str(payload.get("account", ""))
    qos = str(payload.get("qos", ""))
    data = holder.get_pool_seats(account, qos)
    data["updatedAt"] = utc_now()
    return data


def build_history() -> dict[str, Any]:
    history_error = None
    try:
        jobs = scheduler.get_recent_jobs()
    except scheduler.SchedulerCommandError as error:
        jobs = []
        history_error = str(error)
    return {
        "updatedAt": utc_now(),
        "requests": STORE.list_requests(),
        "events": STORE.list_events(),
        "jobs": jobs,
        "error": history_error,
    }


def create_request(payload: dict[str, Any]) -> dict[str, Any]:
    if not MUTATION_LOCK.acquire(blocking=False):
        raise FileExistsError("Another dashboard operation is already in progress")
    try:
        pools = holder.get_pools()
        request = holder.validate_request(payload, pools)
        if request["mode"] == "chain":
            result = holder.run_chain_request(request)
        else:
            job_id = scheduler.submit_normal(request)
            result = {"jobId": job_id}
        STORE.record_request(
            {
                "type": "request",
                "mode": request["mode"],
                "request": request,
                "result": result,
                "submittedAt": utc_now(),
            }
        )
        return {"request": request, "result": result}
    finally:
        MUTATION_LOCK.release()


def cancel_one_job(job_id: str) -> dict[str, Any]:
    if not MUTATION_LOCK.acquire(blocking=False):
        raise FileExistsError("Another dashboard operation is already in progress")
    try:
        result = scheduler.cancel_job(job_id)
        STORE.record_event(
            {"type": "job-cancel", "target": job_id, "result": result}
        )
        return result
    finally:
        MUTATION_LOCK.release()


def run_chain_action(payload: dict[str, Any]) -> dict[str, Any]:
    if not MUTATION_LOCK.acquire(blocking=False):
        raise FileExistsError("Another dashboard operation is already in progress")
    try:
        name = str(payload.get("chainName", ""))
        action = str(payload.get("action", ""))
        result = holder.run_chain_action(name, action, payload.get("value"))
        STORE.record_event(
            {
                "type": "chain-action",
                "target": name,
                "action": action,
                "result": result,
            }
        )
        return result
    finally:
        MUTATION_LOCK.release()


class DashboardHandler(SimpleHTTPRequestHandler):
    server_version = "SpurDashboard/2.0"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        print(f"{self.address_string()} - {format % args}")

    def send_json(
        self, status: HTTPStatus | int, payload: dict[str, Any]
    ) -> None:
        body = json.dumps(payload).encode()
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'")
        self.end_headers()
        self.wfile.write(body)

    def _origin_allowed(self) -> bool:
        origin = self.headers.get("Origin")
        if not origin:
            return True
        parsed = urlparse(origin)
        return parsed.hostname in {"127.0.0.1", "localhost", "::1"}

    def _read_payload(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length < 1 or content_length > 65_536:
            raise ValueError("Request body must be between 1 byte and 64 KiB")
        payload = json.loads(self.rfile.read(content_length))
        if not isinstance(payload, dict):
            raise ValueError("Request body must be an object")
        return payload

    def _api_get(self, path: str, query: dict[str, list[str]]) -> bool:
        routes: dict[str, Callable[[], dict[str, Any]]] = {
            "/api/capabilities": capabilities,
            "/api/status": build_status,
            "/api/pools": build_pools,
            "/api/history": build_history,
            "/api/diagnostics": holder.get_doctor,
            "/api/csrf-token": lambda: {"token": CSRF_TOKEN},
        }
        if path == "/api/queue":
            scope = query.get("scope", ["mine"])[0]
            jobs = scheduler.get_queue(scope)
            for job in jobs:
                job["isMine"] = job["user"] == scheduler.USERNAME
            self.send_json(
                HTTPStatus.OK,
                {"updatedAt": utc_now(), "scope": scope, "jobs": jobs},
            )
            return True
        if path == "/api/logs":
            chain = query.get("chain", [""])[0]
            kind = query.get("kind", ["tick"])[0]
            self.send_json(HTTPStatus.OK, holder.read_log(chain, kind))
            return True
        if path == "/api/top-users":
            account = query.get("account", [""])[0]
            self.send_json(HTTPStatus.OK, build_top_users(account))
            return True
        function = routes.get(path)
        if not function:
            return False
        self.send_json(HTTPStatus.OK, function())
        return True

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            try:
                if not self._api_get(parsed.path, parse_qs(parsed.query)):
                    self.send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
            except (ValueError, FileNotFoundError) as error:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
            except (scheduler.SchedulerCommandError, holder.BridgeError) as error:
                self.send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE, {"error": str(error)}
                )
            return
        if parsed.path in {"/", "/index.html"}:
            page = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
            body = page.replace("__CSRF_TOKEN__", CSRF_TOKEN).encode()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Security-Policy", "default-src 'self'")
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()

    def do_POST(self) -> None:
        if READ_ONLY:
            self.send_json(
                HTTPStatus.FORBIDDEN,
                {"error": "Dashboard is running in read-only mode"},
            )
            return
        routes: dict[str, tuple[Callable[[dict[str, Any]], dict[str, Any]], int]] = {
            "/api/requests": (create_request, HTTPStatus.CREATED),
            "/api/jobs/cancel": (
                lambda payload: cancel_one_job(str(payload.get("jobId", ""))),
                HTTPStatus.OK,
            ),
            "/api/chains/action": (run_chain_action, HTTPStatus.OK),
            "/api/pool-seats": (probe_pool_seats, HTTPStatus.OK),
        }
        route = routes.get(urlparse(self.path).path)
        if not route:
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
            return
        if not self._origin_allowed():
            self.send_json(HTTPStatus.FORBIDDEN, {"error": "Untrusted origin"})
            return
        if self.headers.get("X-CSRF-Token") != CSRF_TOKEN:
            self.send_json(HTTPStatus.FORBIDDEN, {"error": "Invalid CSRF token"})
            return
        function, success_status = route
        try:
            result = function(self._read_payload())
            self.send_json(success_status, result)
        except (json.JSONDecodeError, TypeError, ValueError) as error:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
        except FileNotFoundError as error:
            self.send_json(HTTPStatus.NOT_FOUND, {"error": str(error)})
        except PermissionError as error:
            self.send_json(HTTPStatus.FORBIDDEN, {"error": str(error)})
        except FileExistsError as error:
            self.send_json(HTTPStatus.CONFLICT, {"error": str(error)})
        except (scheduler.SchedulerCommandError, holder.BridgeError) as error:
            self.send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": str(error)})


def _port_candidates(preferred: int, span: int) -> list[int]:
    """Return a deterministic, bounded port order for a shared login node.

    The list is ``preferred`` first, then ``preferred + 1 .. preferred + span``
    (truncated at :data:`MAX_PORT`), then ``0`` to request an OS-assigned
    ephemeral port as a last resort. An explicit ``preferred`` of ``0`` collapses
    to ``[0]`` (grab an ephemeral port immediately). The order is predictable by
    startup order on the host; it is not a stable per-user assignment.
    """
    if not 0 <= preferred <= MAX_PORT:
        raise ValueError(f"port must be between 0 and {MAX_PORT}, got {preferred}")
    if span < 0:
        raise ValueError(f"span must be non-negative, got {span}")
    candidates: list[int] = []
    if preferred:
        upper = min(preferred + span, MAX_PORT)
        candidates.extend(range(preferred, upper + 1))
    candidates.append(0)  # final resort: let the OS assign an ephemeral port
    return candidates


def _format_host(host: str) -> str:
    """Bracket IPv6 literals so the printed URL stays valid (``::1`` -> ``[::1]``)."""
    return f"[{host}]" if ":" in host else host


class _ThreadingHTTPServerV6(ThreadingHTTPServer):
    """IPv6 variant so an explicit ``--host ::1`` actually binds (AF_INET6)."""

    address_family = socket.AF_INET6


def bind_server(
    host: str,
    preferred: int,
    *,
    strict: bool = False,
    span: int = PORT_SCAN_SPAN,
    handler: Any = DashboardHandler,
) -> tuple[ThreadingHTTPServer, int, bool]:
    """Bind the real dashboard server, side-stepping the check-then-bind race.

    The listening server is constructed directly for each candidate port and the
    first one that binds is kept, so the socket we selected is the socket we
    serve on -- no other local process can claim it in between. Only
    ``errno.EADDRINUSE`` advances to the next candidate; permission errors,
    invalid addresses, and anything else surface immediately. With
    ``strict=True`` only ``preferred`` is attempted and ``OSError`` propagates
    (for fixed SSH tunnels or scripts that need an exact port).

    Returns ``(server, actual_port, moved)`` where ``actual_port`` is read back
    from ``server.server_address`` and ``moved`` is True when a non-zero
    preferred port was unavailable and a different one was chosen.
    """
    if not 0 <= preferred <= MAX_PORT:
        raise ValueError(f"port must be between 0 and {MAX_PORT}, got {preferred}")
    candidates = [preferred] if strict else _port_candidates(preferred, span)
    server_cls = _ThreadingHTTPServerV6 if ":" in host else ThreadingHTTPServer
    last_error: "OSError | None" = None
    for candidate in candidates:
        try:
            server = server_cls((host, candidate), handler)
        except OSError as error:
            last_error = error
            if error.errno == errno.EADDRINUSE:
                continue
            raise
        actual_port = server.server_address[1]
        moved = bool(preferred) and actual_port != preferred
        return server, actual_port, moved
    # Reachable only when every candidate failed -- strict mode on a busy port,
    # or the OS refusing even an ephemeral port. Surface the real cause.
    assert last_error is not None
    raise last_error


def resolve_port(
    host: str,
    preferred: int,
    *,
    strict: bool = False,
    span: int = PORT_SCAN_SPAN,
) -> int:
    """Return a bindable port for ``host`` using the same order as ``bind_server``.

    Convenience/reporting and test helper: it binds and then immediately
    releases, so it does *not* hold the port (there is a tiny window before a
    real bind). ``main()`` never uses this for the live server -- it calls
    ``bind_server`` so the selected socket is retained and there is no
    check-then-bind race.
    """
    server, port, _moved = bind_server(host, preferred, strict=strict, span=span)
    server.server_close()
    return port


def _port_arg(value: str) -> int:
    """argparse type for --port: an integer within 0..65535 (clean CLI errors)."""
    try:
        port = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not an integer")
    if not 0 <= port <= MAX_PORT:
        raise argparse.ArgumentTypeError(f"port must be between 0 and {MAX_PORT}")
    return port


def main() -> None:
    global READ_ONLY
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--port",
        type=_port_arg,
        default=PREFERRED_PORT,
        help=(
            "Preferred port (default %(default)s). If it is already in use "
            "(for example a colleague on this shared login node holds it), the "
            "dashboard automatically falls back to the next free port and prints "
            "the actual URL. Use --strict-port to force an exact port."
        ),
    )
    parser.add_argument(
        "--strict-port",
        action="store_true",
        help=(
            "Bind exactly --port and fail instead of auto-moving when it is "
            "unavailable (useful for fixed SSH tunnels or scripts)."
        ),
    )
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Allow binding outside loopback (not recommended; there is no authentication)",
    )
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Disable every POST/mutation endpoint",
    )
    args = parser.parse_args()
    if args.host not in {"127.0.0.1", "localhost", "::1"} and not args.allow_remote:
        parser.error("non-loopback binding requires --allow-remote")
    READ_ONLY = READ_ONLY or args.read_only
    try:
        server, actual_port, moved = bind_server(
            args.host, args.port, strict=args.strict_port
        )
    except OSError as error:
        detail = error.strerror or error
        if args.strict_port:
            parser.error(
                f"--strict-port could not bind {args.host}:{args.port} "
                f"({detail}); omit --strict-port to auto-pick a free port"
            )
        parser.error(f"could not bind {args.host}:{args.port} ({detail})")
    host_url = _format_host(args.host)
    if moved:
        print(
            f"Note: preferred port {args.port} is in use; "
            f"using {actual_port} instead.",
            file=sys.stderr,
        )
    print(f"Spur dashboard: http://{host_url}:{actual_port}")
    if args.host in {"127.0.0.1", "localhost", "::1"}:
        login_host = os.uname().nodename if hasattr(os, "uname") else "<login-host>"
        print(
            f"SSH tunnel:     "
            f"ssh -L {actual_port}:{host_url}:{actual_port} {login_host}"
        )
    print("Press Ctrl-C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
