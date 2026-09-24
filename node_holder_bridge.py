"""Validated bridge between the dashboard and node_holder.sh."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import scheduler


ROOT = Path(__file__).resolve().parent
NODE_HOLDER = Path(
    os.environ.get("SPUR_DASHBOARD_NODE_HOLDER", ROOT / "node_holder.sh")
).resolve()
STATE_DIR = Path(
    os.environ.get("NODEHOLD_DIR", Path.home() / ".node_holder")
).expanduser()
# Chains take the "hold-" family by default (they hold a node); one-off jobs take
# the "interactive-" family. Both are overridable, and the chain prefix still
# tracks NODEHOLD_NAME so the dashboard and the shell agree on new-chain names.
CHAIN_PREFIX = (
    os.environ.get("SPUR_DASHBOARD_CHAIN_PREFIX")
    or os.environ.get("NODEHOLD_NAME")
    or "hold"
)
NORMAL_PREFIX = os.environ.get("SPUR_DASHBOARD_NORMAL_PREFIX") or "interactive"
# node_holder refuses pools under NODEHOLD_MIN_PRIO; read the same knob so the
# dashboard cannot disagree with the script it drives.
MIN_PRIORITY = int(os.environ.get("NODEHOLD_MIN_PRIO") or 10_000)
# Full scheduler names carry a prefix, so allow a little more room than the base.
CHAIN_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
BASE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,48}$")
NODE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
JOB_ID_RE = re.compile(r"^\d+$")
SAFE_ACTIONS = {"topup", "arm", "clear", "tend", "untend", "release", "shrink"}


class BridgeError(RuntimeError):
    """node_holder returned malformed output."""


def _environment(*, prefix: str | None = None, **overrides: str) -> dict[str, str]:
    environment = dict(os.environ)
    environment["NODEHOLD_NAME"] = prefix or CHAIN_PREFIX
    environment.update(overrides)
    return environment


def _run_json(command: str, *, prefix: str | None = None) -> dict[str, Any]:
    output = scheduler.run_command(
        [str(NODE_HOLDER), command],
        timeout=60,
        env=_environment(prefix=prefix),
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise BridgeError(f"{command} returned invalid JSON: {error}") from error
    if not isinstance(payload, dict):
        raise BridgeError(f"{command} did not return an object")
    return payload


def get_pools() -> dict[str, Any]:
    return _run_json("pools-json")


def _active_managed_names(jobs: list[dict[str, Any]]) -> list[str]:
    """Find active node_holder names without assuming one naming prefix."""
    active = {
        str(job.get("name", ""))
        for job in jobs
        if CHAIN_NAME_RE.fullmatch(str(job.get("name", "")))
    }
    managed = {
        name for name in active if (STATE_DIR / f"{name}.conf").is_file()
    }
    # A race member normally has a profile, but include its explicit membership
    # as a recovery path for a partially-created or older chain.
    for race in STATE_DIR.glob("*.race"):
        try:
            lines = race.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in lines:
            name = line.split("|", 1)[0]
            if name in active:
                managed.add(name)
    return sorted(managed)


def get_status(jobs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Return every active maintained chain, regardless of its original prefix."""
    current_jobs = jobs if jobs is not None else scheduler.get_queue("mine")
    chains: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    for name in _active_managed_names(current_jobs):
        try:
            payload = _run_json("status-json", prefix=name)
        except (scheduler.SchedulerCommandError, BridgeError) as error:
            errors[name] = str(error)
            continue
        exact = next(
            (chain for chain in payload.get("chains", []) if chain.get("name") == name),
            None,
        )
        if exact:
            chains.append(exact)
    return {
        "schemaVersion": 1,
        "user": scheduler.USERNAME,
        "prefix": CHAIN_PREFIX,
        "chains": chains,
        "errors": errors,
    }


def get_doctor() -> dict[str, Any]:
    return _run_json("doctor-json")


def parse_time_limit(value: object) -> tuple[str, int]:
    text = str(value or "24:00:00").strip()
    match = re.fullmatch(r"(?:(\d+)-)?(\d+):(\d{2}):(\d{2})", text)
    if not match:
        raise ValueError("Time must be HH:MM:SS or D-HH:MM:SS")
    days, hours, minutes, seconds = (int(part or 0) for part in match.groups())
    if minutes > 59 or seconds > 59:
        raise ValueError("Minutes and seconds must be below 60")
    total = days * 86_400 + hours * 3_600 + minutes * 60 + seconds
    if total <= 180:
        raise ValueError("Time must exceed three minutes")
    normalized = (
        f"{days}-{hours:02d}:{minutes:02d}:{seconds:02d}"
        if days
        else f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    )
    return normalized, total


def _integer(
    value: object,
    field: str,
    minimum: int,
    maximum: int,
    *,
    default: int,
) -> int:
    if value in {None, ""}:
        return default
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a number")
    try:
        number = int(str(value))
    except ValueError as error:
        raise ValueError(f"{field} must be a number") from error
    if not minimum <= number <= maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return number


def _pool_for(
    pools_payload: dict[str, Any], account: str, qos: str
) -> dict[str, Any]:
    for pool in pools_payload.get("pools", []):
        if pool.get("account") == account and pool.get("qos") == qos:
            return pool
    raise ValueError(f"{account} is not associated with {qos}")


def _derive_cpus(gpus: int, node: str | None) -> int | None:
    if gpus >= 8:
        return None
    target = node
    if not target:
        output = scheduler.run_command(
            ["sinfo", "-h", "-N", "-p", scheduler.PARTITION, "-o", "%N"],
            timeout=20,
        )
        target = next((line.strip() for line in output.splitlines() if line.strip()), "")
    if not target:
        raise ValueError("Could not determine node CPU capacity; provide CPUs explicitly")
    details = scheduler.run_command(["spur", "show", "node", target], timeout=20)
    match = re.search(r"\bCPUTot=(\d+)", details)
    if not match:
        raise ValueError("Could not determine node CPU capacity; provide CPUs explicitly")
    return max(1, int(match.group(1)) * gpus // 8)


def validate_request(
    payload: dict[str, Any],
    pools_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Request body must be an object")
    mode = str(payload.get("mode", "")).strip().lower()
    if mode not in {"chain", "normal"}:
        raise ValueError("Mode must be chain or normal")
    strategy = str(payload.get("strategy", "start")).strip().lower()
    if mode == "normal":
        strategy = "start"
    if strategy not in {"start", "adopt", "race"}:
        raise ValueError("Chain strategy must be start, adopt, or race")

    raw_name = str(payload.get("jobName", "")).strip()
    if not BASE_NAME_RE.fullmatch(raw_name):
        raise ValueError(
            "Job name must be 1-48 letters, digits, dots, underscores, or dashes"
        )
    # Reduce whatever was typed to a bare base so an explicitly typed prefix is
    # not doubled ("hold-run" stays "hold-run", not "hold-hold-run").
    base = raw_name
    if base.startswith(f"{CHAIN_PREFIX}-"):
        base = base[len(CHAIN_PREFIX) + 1 :]
    elif base.startswith(f"{NORMAL_PREFIX}-"):
        base = base[len(NORMAL_PREFIX) + 1 :]
    elif base in {CHAIN_PREFIX, NORMAL_PREFIX}:
        base = ""
    if mode == "normal":
        if not base:
            raise ValueError("Enter a job name")
        tag = base
        job_name = f"{NORMAL_PREFIX}-{base}"
    else:
        tag = base
        job_name = CHAIN_PREFIX if not base else f"{CHAIN_PREFIX}-{base}"

    pools_payload = pools_payload or get_pools()
    account = str(payload.get("account", "")).strip()
    qos = str(payload.get("qos", "")).strip()
    if strategy == "race":
        pool = None
        account = ""
        qos = ""
    else:
        if not account or not qos:
            best = pools_payload.get("bestPool") or {}
            account = account or str(best.get("account", ""))
            qos = qos or str(best.get("qos", ""))
        pool = _pool_for(pools_payload, account, qos)

    time_limit, time_seconds = parse_time_limit(payload.get("timeLimit"))
    if pool and pool.get("maxWallMinutes"):
        if time_seconds > int(pool["maxWallMinutes"]) * 60:
            raise ValueError(
                f"{qos} permits at most {pool['maxWallMinutes']} minutes per job"
            )

    gpus = _integer(payload.get("gpus"), "GPUs", 1, 8, default=8)
    nodes = _integer(payload.get("nodes"), "Nodes", 1, 32, default=1)
    node = str(payload.get("node", "")).strip() or None
    if node and not NODE_RE.fullmatch(node):
        raise ValueError("Node name contains unsupported characters")
    if node and nodes != 1:
        raise ValueError("An exact node cannot be combined with multiple nodes")
    exclusive = bool(payload.get("exclusive", gpus == 8))
    if nodes > 1 and gpus < 8 and not exclusive:
        raise ValueError("Partial-GPU multi-node requests must be exclusive")

    cpus_value = payload.get("cpus")
    cpus = (
        _derive_cpus(gpus, node)
        if cpus_value in {None, "", 0, "0"}
        else _integer(cpus_value, "CPUs", 1, 4096, default=1)
    )
    chain_depth = _integer(
        payload.get("chainDepth"), "Chain depth", 0, 64, default=7
    )
    runway_hours = _integer(
        payload.get("runwayHours"), "Runway hours", 0, 24 * 365, default=0
    )
    if payload.get("chainDepth") not in {None, ""} and runway_hours:
        raise ValueError("Choose either chain depth or runway hours, not both")
    expiry_hours = _integer(
        payload.get("expiryHours"), "Expiry hours", 0, 24 * 365, default=0
    )
    if mode == "normal":
        chain_depth = 0
        runway_hours = 0
        expiry_hours = 0

    if pool:
        max_submit = pool.get("maxSubmitPerUser")
        if mode == "chain" and max_submit and not runway_hours:
            if chain_depth + 1 > int(max_submit):
                raise ValueError(
                    f"{qos} permits at most {max_submit} submitted jobs; "
                    f"chain depth can be at most {int(max_submit) - 1}"
                )
        if pool.get("preemptMode") == "cancel" and not payload.get(
            "acceptPreemption", False
        ):
            raise ValueError(
                f"{qos} cancels jobs when preempted; acknowledge preemption to submit"
            )
        priority = pool.get("priority")
        if priority is not None and int(priority) < MIN_PRIORITY and not payload.get(
            "anyQos", False
        ):
            raise ValueError(
                f"{qos} priority {priority} is below {MIN_PRIORITY}; "
                "enable the safety override"
            )

    adopt_job_id = str(payload.get("adoptJobId", "")).strip()
    if strategy == "adopt" and not JOB_ID_RE.fullmatch(adopt_job_id):
        raise ValueError("Adopt requires a numeric running job ID")

    return {
        "mode": mode,
        "strategy": strategy,
        "jobName": job_name,
        "tag": tag,
        "account": account,
        "qos": qos,
        "timeLimit": time_limit,
        "timeSeconds": time_seconds,
        "gpus": gpus,
        "cpus": cpus,
        "nodes": nodes,
        "node": node,
        "exclusive": exclusive,
        "chainDepth": chain_depth,
        "runwayHours": runway_hours,
        "expiryHours": expiry_hours,
        "repin": bool(payload.get("repin", True)),
        "anyQos": bool(payload.get("anyQos", False)),
        "acceptPreemption": bool(payload.get("acceptPreemption", False)),
        "adoptJobId": adopt_job_id or None,
    }


def run_chain_request(request: dict[str, Any]) -> dict[str, Any]:
    command = [str(NODE_HOLDER)]
    if request["tag"]:
        command.extend(["-n", request["tag"]])
    if request["strategy"] != "race":
        command.extend(["-A", request["account"], "-q", request["qos"]])
    command.extend(
        [
            "-g",
            str(request["gpus"]),
            "-c",
            str(request["cpus"] or 0),
            "-N",
            str(request["nodes"]),
            "--time",
            request["timeLimit"],
        ]
    )
    command.append("--exclusive" if request["exclusive"] else "--no-exclusive")
    if request["node"]:
        command.extend(["-w", request["node"]])
    if request["runwayHours"]:
        command.extend(["--hours", str(request["runwayHours"])])
    else:
        command.extend(["--chain", str(request["chainDepth"])])
    if request["expiryHours"]:
        command.extend(["--for-hours", str(request["expiryHours"])])
    if request["anyQos"]:
        command.append("--any-qos")
    command.append(request["strategy"])
    if request["strategy"] == "adopt":
        command.append(request["adoptJobId"])
    output = scheduler.run_command(
        command,
        timeout=180,
        env=_environment(NODEHOLD_REPIN="1" if request["repin"] else "0"),
    )
    job_ids = re.findall(r"\bholder (\d+) submitted\b", output)
    full_name = CHAIN_PREFIX if not request["tag"] else f"{CHAIN_PREFIX}-{request['tag']}"
    return {
        "chainName": full_name,
        "jobIds": job_ids,
        "output": output,
    }


def run_chain_action(name: str, action: str, value: object = None) -> dict[str, Any]:
    if action not in SAFE_ACTIONS:
        raise ValueError("Unsupported chain action")
    if not CHAIN_NAME_RE.fullmatch(name):
        raise ValueError("Invalid chain name")
    if name not in chain_names(get_status()):
        raise ValueError(f"'{name}' is not an active maintained chain")

    # Treat the full scheduler name as node_holder's default chain. This avoids
    # needing to reconstruct the prefix/tag split used when the chain began.
    command = [str(NODE_HOLDER)]
    command.append(action)
    if action == "shrink":
        command.append(str(_integer(value, "Depth", 0, 64, default=0)))
    output = scheduler.run_command(
        command,
        timeout=180,
        env=_environment(prefix=name, NODEHOLD_CHAIN_FULL_NAME=name),
    )
    return {"chainName": name, "action": action, "output": output}


def chain_names(status: dict[str, Any] | None = None) -> set[str]:
    payload = status or get_status()
    return {
        str(chain.get("name"))
        for chain in payload.get("chains", [])
        if chain.get("name")
    }


def read_log(chain_name: str, kind: str, limit: int = 200) -> dict[str, Any]:
    if not CHAIN_NAME_RE.fullmatch(chain_name):
        raise ValueError("Invalid chain name")
    suffixes = {
        "tick": f"{chain_name}.tick.log",
        "arm": f"{chain_name}.arm.log",
        "on-start": f"{chain_name}.on_start.log",
    }
    filename = suffixes.get(kind)
    if not filename:
        raise ValueError("Unsupported log type")
    path = Path.home() / "logs" / filename
    if not path.exists():
        return {"chainName": chain_name, "kind": kind, "lines": []}
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return {
        "chainName": chain_name,
        "kind": kind,
        "lines": lines[-max(1, min(limit, 500)) :],
    }
