#!/usr/bin/env python3
"""Dependency-free localhost control plane for AMD SPUR allocations."""

from __future__ import annotations

import argparse
import copy
import errno
import json
import os
import re
import secrets
import socket
import sys
import threading
import time
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
# Shared deployments serve many browsers from one login node: cache scheduler
# reads so viewer count does not multiply squeue load.
CACHE_SECONDS = max(0, int(os.environ.get("SPUR_DASHBOARD_CACHE_SECONDS") or 0))
REFRESH_SECONDS = max(15, int(os.environ.get("SPUR_DASHBOARD_REFRESH_SECONDS") or 30))
TEAM_FILE = Path(
    os.environ.get("SPUR_DASHBOARD_TEAM_FILE")
    or Path.home() / ".spur-dashboard" / "team.json"
).expanduser()
TEAM_NAME_PATTERN = re.compile(r"[A-Za-z0-9_.-]{1,64}")
GPU_UTIL_DIR = Path(os.environ.get("SPUR_GPU_UTIL_DIR") or "/shared_nfs/spur-gpu-util")
UTILIZATION_DEFAULTS = {"lowUtilPercent": 10.0, "minLowHours": 2.0, "windowHours": 24.0}
SAMPLE_SECONDS = 300
# A gap longer than this means the sampler was not running; don't count it.
MAX_SAMPLE_GAP = 900

PREFERRED_PORT = 8876
PORT_SCAN_SPAN = 64
MAX_PORT = 65535


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


_CACHE: dict[str, tuple[float, Any]] = {}
_CACHE_LOCKS: dict[str, threading.Lock] = {}
_CACHE_GUARD = threading.Lock()


def cached(key: str, producer: Callable[[], Any]) -> Any:
    """Return ``producer()``, reusing a result younger than CACHE_SECONDS.

    A per-key lock makes concurrent viewers wait for one scheduler call instead
    of each issuing their own.
    """
    if not CACHE_SECONDS:
        return producer()
    with _CACHE_GUARD:
        lock = _CACHE_LOCKS.setdefault(key, threading.Lock())
    with lock:
        hit = _CACHE.get(key)
        if hit and time.monotonic() - hit[0] < CACHE_SECONDS:
            return copy.deepcopy(hit[1])
        value = producer()
        _CACHE[key] = (time.monotonic(), value)
        return copy.deepcopy(value)


def load_team() -> dict[str, Any]:
    """Read the optional team file: {"users": [...], "focusQos": [...]}.

    Re-read on every call so edits apply without a restart.
    """
    config: dict[str, Any] = {
        "users": [],
        "focusQos": [],
        "utilization": dict(UTILIZATION_DEFAULTS),
        "error": None,
    }
    try:
        raw = json.loads(TEAM_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return config
    except (OSError, ValueError) as error:
        config["error"] = f"{TEAM_FILE}: {error}"
        return config
    if not isinstance(raw, dict):
        config["error"] = f"{TEAM_FILE}: expected a JSON object"
        return config
    for key in ("users", "focusQos"):
        values = raw.get(key) or []
        if not isinstance(values, list):
            continue
        config[key] = list(
            dict.fromkeys(
                str(value).strip()
                for value in values
                if TEAM_NAME_PATTERN.fullmatch(str(value).strip())
            )
        )
    thresholds = raw.get("utilization")
    if isinstance(thresholds, dict):
        for key, default in UTILIZATION_DEFAULTS.items():
            try:
                config["utilization"][key] = float(thresholds.get(key, default))
            except (TypeError, ValueError):
                pass
    return config


def capabilities() -> dict[str, Any]:
    return {
        "schemaVersion": 1,
        "chainPrefix": holder.CHAIN_PREFIX,
        "normalPrefix": holder.NORMAL_PREFIX,
        "minPriority": holder.MIN_PRIORITY,
        "partition": scheduler.PARTITION,
        "user": scheduler.USERNAME,
        "readOnly": READ_ONLY,
        "refreshSeconds": REFRESH_SECONDS,
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
    focus = load_team()["focusQos"]
    if focus:
        rank = {qos: index for index, qos in enumerate(focus)}
        for pool in pools.get("pools", []):
            pool["focus"] = pool.get("qos") in rank
        pools["pools"] = sorted(
            pools.get("pools", []),
            key=lambda pool: rank.get(str(pool.get("qos", "")), len(rank)),
        )
    pools["updatedAt"] = utc_now()
    return pools


def summarize_samples(
    samples: list[dict[str, Any]], low_percent: float, now: float
) -> dict[str, Any]:
    """Weight each sample by the time until the next one (capped at MAX_SAMPLE_GAP)."""
    samples = sorted(samples, key=lambda sample: sample["ts"])
    gpu_seconds = util_seconds = idle_gpu_seconds = 0.0
    low_run = longest_low = 0.0
    last_error = None
    measured = 0
    for index, sample in enumerate(samples):
        if index + 1 < len(samples):
            gap = samples[index + 1]["ts"] - sample["ts"]
        else:
            gap = min(SAMPLE_SECONDS, max(0.0, now - sample["ts"]))
        weight = max(0.0, min(MAX_SAMPLE_GAP, gap))
        utils = [
            float(gpu["util"])
            for gpu in sample.get("gpus") or []
            if isinstance(gpu, dict) and isinstance(gpu.get("util"), (int, float))
        ]
        if not utils:
            last_error = sample.get("error") or "no GPU readings"
            continue
        measured += 1
        gpu_seconds += len(utils) * weight
        util_seconds += sum(utils) * weight
        idle_gpu_seconds += sum(1 for util in utils if util < low_percent) * weight
        if sum(utils) / len(utils) < low_percent:
            low_run += weight
            longest_low = max(longest_low, low_run)
        else:
            low_run = 0.0
    return {
        "samples": measured,
        "gpuHours": round(gpu_seconds / 3600, 2),
        "avgUtil": round(util_seconds / gpu_seconds, 1) if gpu_seconds else None,
        "idleGpuHours": round(idle_gpu_seconds / 3600, 2),
        "longestLowHours": round(longest_low / 3600, 2),
        "currentLowHours": round(low_run / 3600, 2),
        "firstSampleAt": samples[0]["ts"] if samples else None,
        "lastSampleAt": samples[-1]["ts"] if samples else None,
        "lastError": last_error,
    }


def read_samples(path: Path, cutoff: float) -> list[dict[str, Any]]:
    samples = []
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                try:
                    sample = json.loads(line)
                except ValueError:
                    continue
                if isinstance(sample, dict) and isinstance(sample.get("ts"), (int, float)) and sample["ts"] >= cutoff:
                    samples.append(sample)
    except OSError:
        return []
    return samples


def build_utilization() -> dict[str, Any]:
    """Per-job and per-user GPU utilization for team users over the window.

    Samples come from gpu_sampler.py running inside each job; running team jobs
    with no samples are listed as not reporting.
    """
    team = load_team()
    limits = team["utilization"]
    low_percent = limits["lowUtilPercent"]
    min_low_hours = limits["minLowHours"]
    now = time.time()
    cutoff = now - limits["windowHours"] * 3600
    users = team["users"]
    running = {
        job["id"]: job
        for job in scheduler.get_queue("all")
        if job["user"] in users and job["state"] == "RUNNING"
    }
    rows: list[dict[str, Any]] = []
    reported: set[str] = set()
    for user in users:
        user_dir = GPU_UTIL_DIR / user
        try:
            paths = sorted(user_dir.glob("*.jsonl"))
        except OSError:
            paths = []
        for path in paths:
            try:
                if path.stat().st_mtime < cutoff:
                    continue
            except OSError:
                continue
            samples = read_samples(path, cutoff)
            if not samples:
                continue
            job_id, _, node = path.stem.partition(".")
            job = running.get(job_id, {})
            stats = summarize_samples(samples, low_percent, now)
            reported.add(job_id)
            rows.append(
                {
                    "user": user,
                    "id": job_id,
                    "node": node,
                    "name": job.get("name", ""),
                    "qos": job.get("qos", ""),
                    "state": "RUNNING" if job else "ENDED",
                    "reporting": True,
                    "flagged": stats["longestLowHours"] >= min_low_hours,
                    **stats,
                }
            )
    for job_id, job in running.items():
        if job_id in reported:
            continue
        rows.append(
            {
                "user": job["user"],
                "id": job_id,
                "node": job["nodeListOrReason"],
                "name": job["name"],
                "qos": job["qos"],
                "state": "RUNNING",
                "reporting": False,
                "flagged": False,
                "elapsed": job["elapsed"],
            }
        )
    rows.sort(key=lambda row: (users.index(row["user"]), not row["flagged"], row["state"] != "RUNNING", row["id"]))
    members = []
    for user in users:
        mine = [row for row in rows if row["user"] == user]
        measured = [row for row in mine if row["reporting"]]
        gpu_hours = sum(row["gpuHours"] for row in measured)
        weighted = sum(row["avgUtil"] * row["gpuHours"] for row in measured if row["avgUtil"] is not None)
        members.append(
            {
                "user": user,
                "gpuHours": round(gpu_hours, 2),
                "avgUtil": round(weighted / gpu_hours, 1) if gpu_hours else None,
                "idleGpuHours": round(sum(row["idleGpuHours"] for row in measured), 2),
                "flaggedJobs": sum(1 for row in mine if row["flagged"]),
                "unreportedJobs": sum(1 for row in mine if not row["reporting"]),
            }
        )
    return {
        "updatedAt": utc_now(),
        "dataDir": str(GPU_UTIL_DIR),
        "thresholds": limits,
        "members": members,
        "jobs": rows,
    }


def build_team() -> dict[str, Any]:
    """Active jobs for the configured team plus pool-wide focus-QoS totals."""
    team = load_team()
    jobs = scheduler.get_queue("all")
    users = set(team["users"])
    focus = team["focusQos"]
    members: dict[str, dict[str, Any]] = {
        user: {"user": user, "running": 0, "pending": 0, "runningNodes": 0}
        for user in team["users"]
    }
    qos_totals: dict[str, dict[str, Any]] = {
        qos: {"qos": qos, "running": 0, "pending": 0, "runningNodes": 0, "teamRunningNodes": 0}
        for qos in focus
    }
    team_jobs = []
    for job in jobs:
        job["isMine"] = job["user"] == scheduler.USERNAME
        running = job["state"] == "RUNNING"
        totals = qos_totals.get(job["qos"])
        if totals:
            totals["running" if running else "pending"] += 1
            if running:
                totals["runningNodes"] += job["nodes"]
                if job["user"] in users:
                    totals["teamRunningNodes"] += job["nodes"]
        if job["user"] not in users:
            continue
        team_jobs.append(job)
        member = members[job["user"]]
        member["running" if running else "pending"] += 1
        if running:
            member["runningNodes"] += job["nodes"]
    team_jobs.sort(
        key=lambda job: (
            team["users"].index(job["user"]),
            job["state"] != "RUNNING",
            int(job["id"]) if job["id"].isdigit() else 0,
        )
    )
    return {
        "updatedAt": utc_now(),
        "configFile": str(TEAM_FILE),
        "configError": team["error"],
        "members": list(members.values()),
        "focusQos": [qos_totals[qos] for qos in focus],
        "jobs": team_jobs,
    }


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
            "/api/status": lambda: cached("status", build_status),
            "/api/pools": lambda: cached("pools", build_pools),
            "/api/team": lambda: cached("team", build_team),
            "/api/utilization": lambda: cached("utilization", build_utilization),
            "/api/history": lambda: cached("history", build_history),
            "/api/diagnostics": lambda: cached("diagnostics", holder.get_doctor),
            "/api/csrf-token": lambda: {"token": CSRF_TOKEN},
        }
        if path == "/api/queue":
            scope = query.get("scope", ["mine"])[0]
            if scope not in {"mine", "all"}:
                raise ValueError("Queue scope must be 'mine' or 'all'")
            jobs = cached(f"queue:{scope}", lambda: scheduler.get_queue(scope))
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
            self.send_json(
                HTTPStatus.OK,
                cached(f"top-users:{account}", lambda: build_top_users(account)),
            )
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
