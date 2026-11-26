#!/usr/bin/env python3
"""Record GPU utilization from inside a running job for the shared dashboard.

Start it in the background near the top of a job script:

    python3 /path/to/spur-allocation-dashboard/gpu_sampler.py &

Every --interval seconds it appends one JSON line per sample to
$SPUR_GPU_UTIL_DIR/<user>/<jobid>.<node>.jsonl (default /shared_nfs/spur-gpu-util),
and exits once the process that started it is gone, so it never outlives the
job. Multi-node jobs need one sampler per node, e.g.
    srun --overlap --ntasks-per-node=1 python3 gpu_sampler.py &
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

DATA_DIR = Path(os.environ.get("SPUR_GPU_UTIL_DIR") or "/shared_nfs/spur-gpu-util")
RETENTION_SECONDS = 7 * 86_400


def number(value: Any) -> float | None:
    """amd-smi reports scalars, {"value": x, "unit": u}, or per-partition lists."""
    if isinstance(value, dict):
        value = value.get("value")
    if isinstance(value, list):
        values = [item for item in (number(entry) for entry in value) if item is not None]
        return sum(values) / len(values) if values else None
    if isinstance(value, str):
        value = value.strip().rstrip("%").split()[0] if value.strip() else ""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _amd_smi_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        for key in ("gpu_data", "gpus", "data"):
            if isinstance(payload.get(key), list):
                return payload[key]
        return [payload]
    return payload if isinstance(payload, list) else []


def parse_amd_smi(payload: Any) -> list[dict[str, Any]]:
    gpus = []
    for index, record in enumerate(_amd_smi_records(payload)):
        if not isinstance(record, dict):
            continue
        usage = record.get("usage") or {}
        power = record.get("power") or {}
        memory = record.get("mem_usage") or {}
        used, total = number(memory.get("used_vram")), number(memory.get("total_vram"))
        gpu_id = number(record.get("gpu"))
        gpus.append(
            {
                "gpu": int(gpu_id) if gpu_id is not None else index,
                "util": number(usage.get("gfx_activity")),
                "vramUsedPct": round(100 * used / total, 1) if used is not None and total else None,
                "powerW": number(power.get("socket_power") or power.get("current_socket_power")),
            }
        )
    return gpus


def parse_rocm_smi(payload: Any) -> list[dict[str, Any]]:
    gpus = []
    for card, fields in sorted((payload or {}).items()):
        if not card.startswith("card") or not isinstance(fields, dict):
            continue
        power = next((value for key, value in fields.items() if "Power" in key and "(W)" in key), None)
        gpus.append(
            {
                "gpu": int(card[4:]) if card[4:].isdigit() else len(gpus),
                "util": number(fields.get("GPU use (%)")),
                "vramUsedPct": number(fields.get("GPU Memory Allocated (VRAM%)")),
                "powerW": number(power),
            }
        )
    return gpus


def read_gpus() -> tuple[str, list[dict[str, Any]]]:
    attempts = [
        ("amd-smi", ["amd-smi", "metric", "--usage", "--power", "--mem-usage", "--json"], parse_amd_smi),
        ("rocm-smi", ["rocm-smi", "--showuse", "--showmemuse", "--showpower", "--json"], parse_rocm_smi),
    ]
    errors = []
    for source, command, parser in attempts:
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
        except (OSError, subprocess.TimeoutExpired) as error:
            errors.append(f"{source}: {error}")
            continue
        if result.returncode != 0:
            errors.append(f"{source}: exit {result.returncode}: {result.stderr.strip()[:200]}")
            continue
        try:
            gpus = [gpu for gpu in parser(json.loads(result.stdout)) if gpu["util"] is not None]
        except ValueError as error:
            errors.append(f"{source}: bad JSON: {error}")
            continue
        if gpus:
            return source, gpus
        errors.append(f"{source}: no GPU utilization in output")
    raise RuntimeError("; ".join(errors))


def prune(user_dir: Path) -> None:
    cutoff = time.time() - RETENTION_SECONDS
    for path in user_dir.glob("*.jsonl"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
        except OSError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--interval", type=int, default=300, help="Seconds between samples (default 300)")
    parser.add_argument("--once", action="store_true", help="Take one sample, print it, and exit")
    args = parser.parse_args()

    user = getpass.getuser()
    job = os.environ.get("SLURM_JOB_ID") or os.environ.get("SPUR_JOB_ID") or "nojob"
    node = socket.gethostname().split(".")[0]
    parent = os.getppid()

    user_dir = DATA_DIR / user
    if not args.once:
        if not DATA_DIR.exists():
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            os.chmod(DATA_DIR, 0o1777)
        user_dir.mkdir(exist_ok=True)
        prune(user_dir)
    out = user_dir / f"{job}.{node}.jsonl"

    while True:
        sample: dict[str, Any] = {"ts": int(time.time()), "job": job, "user": user, "node": node}
        try:
            sample["source"], sample["gpus"] = read_gpus()
        except RuntimeError as error:
            sample["error"] = str(error)[:500]
        line = json.dumps(sample, separators=(",", ":"))
        if args.once:
            print(line)
            return
        with open(out, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        deadline = time.monotonic() + max(30, args.interval)
        while time.monotonic() < deadline:
            if os.getppid() != parent:
                return
            time.sleep(15)


if __name__ == "__main__":
    main()
