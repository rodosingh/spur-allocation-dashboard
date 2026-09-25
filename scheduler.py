"""Read-only Slurm views and finite one-off job operations for the dashboard."""

from __future__ import annotations

import getpass
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


PARTITION = os.environ.get("SPUR_DASHBOARD_PARTITION", "amd-spur")
# An empty username turns `squeue --user ""` into a confusing empty queue rather
# than an error, so fall back to the password database.
try:
    USERNAME = os.environ.get("USER") or os.environ.get("LOGNAME") or getpass.getuser()
except OSError:  # pragma: no cover - only when the name service is unavailable
    USERNAME = ""
HOME = Path.home()
LOG_DIR = HOME / "logs"
ACTIVE_STATES = {
    "PENDING",
    "RUNNING",
    "CONFIGURING",
    "COMPLETING",
    "SUSPENDED",
}
QUEUE_FORMAT = "%i|%j|%u|%a|%q|%p|%T|%M|%L|%D|%R|%V|%S|%b|%C"


class SchedulerCommandError(RuntimeError):
    """A scheduler command could not be executed or returned a failure."""


def run_command(
    arguments: list[str],
    *,
    timeout: int = 30,
    env: Mapping[str, str] | None = None,
) -> str:
    try:
        result = subprocess.run(
            arguments,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=dict(env) if env is not None else None,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise SchedulerCommandError(f"Could not run {arguments[0]}: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
        raise SchedulerCommandError(f"{arguments[0]} failed: {detail}")
    return result.stdout.strip()


def parse_timestamp(value: str) -> datetime | None:
    if not value or value in {"N/A", "Unknown"}:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def format_duration(seconds: int) -> str:
    seconds = max(0, seconds)
    days, remainder = divmod(seconds, 86_400)
    hours, remainder = divmod(remainder, 3_600)
    minutes, secs = divmod(remainder, 60)
    if days:
        return f"{days}d {hours:02d}h {minutes:02d}m"
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m {secs:02d}s"


def get_queue(scope: str = "mine") -> list[dict[str, Any]]:
    if scope not in {"mine", "all"}:
        raise ValueError("Queue scope must be 'mine' or 'all'")
    command = ["squeue"]
    if scope == "mine":
        command.extend(["--user", USERNAME])
    command.extend(["--noheader", "--format", QUEUE_FORMAT])
    output = run_command(command)
    now = datetime.now(timezone.utc)
    jobs: list[dict[str, Any]] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.split("|", 14)
        if len(fields) != 15:
            continue
        (
            job_id,
            name,
            user,
            account,
            qos,
            priority,
            state,
            elapsed,
            time_left,
            nodes,
            node_or_reason,
            submitted_at,
            estimated_start,
            gres,
            cpus,
        ) = fields
        submitted = parse_timestamp(submitted_at)
        wait_seconds = (
            max(0, int((now - submitted).total_seconds())) if submitted else None
        )
        jobs.append(
            {
                "id": job_id.strip(),
                "name": name.strip(),
                "user": user.strip(),
                "account": account.strip(),
                "qos": qos.strip(),
                "priority": int(priority) if priority.strip().isdigit() else None,
                "state": state.strip(),
                "elapsed": elapsed.strip(),
                "timeLeft": time_left.strip() if state.strip() == "RUNNING" else None,
                "nodes": int(nodes) if nodes.strip().isdigit() else 0,
                "nodeListOrReason": node_or_reason.strip(),
                "submittedAt": submitted_at.strip(),
                "estimatedStart": (
                    estimated_start.strip()
                    if estimated_start.strip() not in {"", "N/A"}
                    else None
                ),
                "gres": gres.strip() if gres.strip() not in {"", "N/A", "(null)"} else None,
                "cpus": int(cpus) if cpus.strip().isdigit() else None,
                "waitSeconds": wait_seconds,
                "waitDisplay": (
                    format_duration(wait_seconds) if wait_seconds is not None else "N/A"
                ),
            }
        )
    return jobs


def get_recent_jobs(limit: int = 50) -> list[dict[str, Any]]:
    limit = max(1, min(limit, 200))
    output = run_command(
        [
            "sacct",
            "--user",
            USERNAME,
            "--starttime",
            "now-7days",
            "--limit",
            str(limit),
            "--noheader",
            "--format",
            "JobID,JobName,User,Account,State,Elapsed,Start,End,ExitCode",
        ]
    )
    recent: list[dict[str, Any]] = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) != 9:
            continue
        (
            job_id,
            name,
            user,
            account,
            state,
            elapsed,
            started,
            ended,
            exit_code,
        ) = fields
        if "." in job_id or state in ACTIVE_STATES:
            continue
        display_state = state
        if state == "COMPLETED" and elapsed in {"00:00:00", "0:00"} and exit_code == "-1:0":
            display_state = "LAUNCH_FAILED"
        recent.append(
            {
                "id": job_id,
                "name": name,
                "user": user,
                "account": account,
                "qos": "",
                "state": display_state,
                "schedulerState": state,
                "elapsed": elapsed,
                "startedAt": started if started not in {"", "Unknown"} else None,
                "endedAt": ended if ended not in {"", "Unknown"} else None,
                "exitCode": exit_code,
                "nodes": None,
            }
        )
        if len(recent) >= limit:
            break
    return recent


def submit_normal(request: dict[str, Any]) -> str:
    """Submit a finite sleep holder. Request validation happens in the bridge."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    command = [
        "sbatch",
        "--parsable",
        "--job-name",
        request["jobName"],
        "--partition",
        PARTITION,
        "--account",
        request["account"],
        "--nodes",
        str(request["nodes"]),
        "--time",
        request["timeLimit"],
        "--gres",
        f"gpu:{request['gpus']}",
        "--chdir",
        "/tmp",
        "--output",
        str(LOG_DIR / f"dashboard-{request['jobName']}.%j.out"),
        "--error",
        str(LOG_DIR / f"dashboard-{request['jobName']}.%j.err"),
    ]
    if request.get("qos"):
        command.extend(["--qos", request["qos"]])
    if request.get("cpus"):
        command.extend(["--cpus-per-task", str(request["cpus"])])
    if request.get("node"):
        command.extend(["--nodelist", request["node"]])
    if request["exclusive"]:
        command.append("--exclusive")
    hold_seconds = max(1, request["timeSeconds"] - 60)
    command.extend(["--wrap", f"exec sleep {hold_seconds}"])
    output = run_command(command, timeout=60)
    job_id = output.split(";", 1)[0].strip()
    if not re.fullmatch(r"\d+", job_id):
        raise SchedulerCommandError("sbatch returned no numeric job ID")
    return job_id


def cancel_job(job_id: str) -> dict[str, Any]:
    """Cancel one of the current user's jobs by ID (chain member or not)."""
    if not re.fullmatch(r"\d+", job_id):
        raise ValueError("Invalid job ID")
    job = next((item for item in get_queue("mine") if item["id"] == job_id), None)
    if not job:
        raise FileNotFoundError("Active job not found")
    run_command(["scancel", job_id], timeout=60)
    return {"jobId": job_id, "name": job["name"], "state": "CANCELLED"}
