#!/usr/bin/env python3
"""Dependency-free localhost control plane for AMD SPUR allocations."""

from __future__ import annotations

import argparse
import json
import os
import secrets
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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def capabilities() -> dict[str, Any]:
    return {
        "schemaVersion": 1,
        "chainPrefix": holder.CHAIN_PREFIX,
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


def cancel_normal_job(job_id: str) -> dict[str, Any]:
    if not MUTATION_LOCK.acquire(blocking=False):
        raise FileExistsError("Another dashboard operation is already in progress")
    try:
        status = holder.get_status()
        scheduler.cancel_owned_job(job_id, holder.chain_names(status))
        result = {"jobId": job_id, "state": "CANCELLED"}
        STORE.record_event(
            {"type": "normal-cancel", "target": job_id, "result": result}
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
                lambda payload: cancel_normal_job(str(payload.get("jobId", ""))),
                HTTPStatus.OK,
            ),
            "/api/chains/action": (run_chain_action, HTTPStatus.OK),
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


def main() -> None:
    global READ_ONLY
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8876)
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
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"Spur dashboard: http://{args.host}:{args.port}")
    print("Press Ctrl-C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
