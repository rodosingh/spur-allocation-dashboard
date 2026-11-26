#!/usr/bin/env python3
"""Alert job owners in Teams when their watched-QoS job starts running.

Polls the shared dashboard's /api/team, detects PENDING -> RUNNING transitions
for the configured QoS, and posts an Adaptive Card that @mentions the owner to
a Teams Workflows "post to a channel when a webhook request is received" URL.
A single reminder follows once the job has held its node for reminderHours.

Without a webhookUrl it only logs what it would send (dry run).
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CONFIG_FILE = Path(
    os.environ.get("SPUR_NOTIFY_CONFIG")
    or Path.home() / ".config" / "spur-dashboard" / "notify.json"
).expanduser()
STATE_FILE = Path(
    os.environ.get("SPUR_NOTIFY_STATE")
    or Path.home() / ".local" / "state" / "spur-dashboard" / "notify-state.json"
).expanduser()
DEFAULTS: dict[str, Any] = {
    "dashboardUrl": "http://127.0.0.1:8876",
    "publicDashboardUrl": "",
    "webhookUrl": "",
    "qos": ["amd-brain-models-qos"],
    "reminderHours": 2,
    "pollSeconds": 60,
    "emails": {},
    "digestTime": "11:00",
}


def log(message: str) -> None:
    print(f"{datetime.now().isoformat(timespec='seconds')} {message}", flush=True)


def load_config() -> dict[str, Any]:
    config = dict(DEFAULTS)
    try:
        config.update(json.loads(CONFIG_FILE.read_text(encoding="utf-8")))
    except FileNotFoundError:
        pass
    return config


def load_state() -> dict[str, Any] | None:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_FILE.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2), encoding="utf-8")
    temporary.replace(STATE_FILE)


def elapsed_seconds(value: str) -> int:
    """Parse Slurm elapsed time: [D-]HH:MM:SS, MM:SS, or SS."""
    match = re.fullmatch(r"(?:(\d+)-)?(?:(\d+):)?(?:(\d+):)?(\d+)", value.strip())
    if not match:
        return 0
    days, first, second, seconds = match.groups()
    if second is None:
        hours, minutes = 0, int(first or 0)
    else:
        hours, minutes = int(first or 0), int(second)
    return int(days or 0) * 86_400 + hours * 3_600 + minutes * 60 + int(seconds)


def fetch_team(config: dict[str, Any]) -> dict[str, Any]:
    url = config["dashboardUrl"].rstrip("/") + "/api/team"
    with urllib.request.urlopen(url, timeout=120) as response:
        return json.load(response)


def build_summary(job: dict[str, Any], config: dict[str, Any], reminder: bool) -> str:
    """HTML for a plain Teams message that the flow prefixes with an @mention token."""
    if reminder:
        lead = f"job <b>{job['id']}</b> has held its node for {elapsed_seconds(job.get('elapsed', '')) // 3600}h. Still using it?"
    else:
        lead = (
            f"your <b>{html.escape(job['qos'])}</b> job <b>{job['id']}</b> "
            f"({html.escape(job['name'])}) is now <b>RUNNING</b>."
        )
    parts = [
        lead,
        f"Node: {html.escape(job.get('nodeListOrReason') or '?')} · "
        f"time left: {html.escape(job.get('timeLeft') or '?')}",
        f"Please start using it, or release it with <code>scancel {job['id']}</code>.",
    ]
    if config.get("publicDashboardUrl"):
        parts.append(f"<a href=\"{config['publicDashboardUrl']}\">Open dashboard</a>")
    return "<br>".join(parts)


def owner_email(user: str, config: dict[str, Any]) -> str:
    """Explicit mapping first, else <username>@<emailDomain> (Teams resolves NTID UPNs)."""
    domain = config.get("emailDomain")
    return config["emails"].get(user) or (f"{user}@{domain}" if domain else "")


def build_card(job: dict[str, Any], config: dict[str, Any], reminder: bool) -> dict[str, Any]:
    user = job["user"]
    email = owner_email(user, config)
    card_mentions = bool(email) and config.get("cardMentions", True)
    who = f"<at>{user}</at>" if card_mentions else user
    held = elapsed_seconds(job.get("elapsed", ""))
    if reminder:
        headline = f"{who}: job {job['id']} has held {job['nodes']} node(s) for {held // 3600}h."
        body = "Still using it? If not, please scancel it so a teammate can have the node."
    else:
        headline = f"{who}: your {job['qos']} job {job['id']} is now RUNNING."
        body = "The node is allocated to you. Please start using it, or scancel it if you no longer need it."
    facts = [
        {"title": "Job", "value": f"{job['id']} · {job['name']}"},
        {"title": "Node", "value": job.get("nodeListOrReason") or "?"},
        {"title": "Time left", "value": job.get("timeLeft") or "?"},
        {"title": "Release", "value": f"scancel {job['id']}"},
    ]
    content: dict[str, Any] = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": [
            {"type": "TextBlock", "text": headline, "weight": "Bolder", "wrap": True},
            {"type": "TextBlock", "text": body, "wrap": True},
            {"type": "FactSet", "facts": facts},
        ],
    }
    if config.get("publicDashboardUrl"):
        content["actions"] = [
            {"type": "Action.OpenUrl", "title": "Open dashboard", "url": config["publicDashboardUrl"]}
        ]
    if card_mentions:
        content["msteams"] = {
            "entities": [
                {
                    "type": "mention",
                    "text": f"<at>{user}</at>",
                    "mentioned": {"id": email, "name": user},
                }
            ]
        }
    # mention/summary feed flows that ping via "Get an @mention token for a
    # user" + "Post message in a chat or channel", since Workflows drops
    # Adaptive Card mentions in group chats.
    return {
        "type": "message",
        "attachments": [
            {"contentType": "application/vnd.microsoft.card.adaptive", "content": content}
        ],
        "mention": {"email": email or "", "name": user},
        "summary": build_summary(job, config, reminder),
    }


def send(card: dict[str, Any], config: dict[str, Any], summary: str) -> bool:
    if not config.get("webhookUrl"):
        log(f"DRY RUN (no webhookUrl): {summary}")
        return True
    request = urllib.request.Request(
        config["webhookUrl"],
        data=json.dumps(card).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            log(f"sent ({response.status}): {summary}")
            return True
    except (urllib.error.URLError, OSError) as error:
        log(f"send failed, will retry next poll: {summary}: {error}")
        return False


def poll_once(config: dict[str, Any], state: dict[str, Any] | None) -> dict[str, Any]:
    team = fetch_team(config)
    watched = set(config["qos"])
    jobs = [job for job in team.get("jobs", []) if job.get("qos") in watched]
    running = {job["id"]: job for job in jobs if job.get("state") == "RUNNING"}
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if state is None:
        log(f"first run: recording {len(running)} already-running job(s) without alerting")
        return {
            "jobs": {
                job_id: {"user": job["user"], "startNotified": now, "reminded": True}
                for job_id, job in running.items()
            }
        }

    tracked: dict[str, Any] = state.setdefault("jobs", {})
    reminder_seconds = float(config["reminderHours"]) * 3_600
    for job_id, job in running.items():
        entry = tracked.get(job_id)
        if entry is None:
            summary = f"start {job_id} {job['user']} {job['qos']} on {job.get('nodeListOrReason')}"
            if send(build_card(job, config, reminder=False), config, summary):
                tracked[job_id] = {"user": job["user"], "startNotified": now, "reminded": False}
            continue
        if (
            reminder_seconds
            and not entry.get("reminded")
            and elapsed_seconds(job.get("elapsed", "")) >= reminder_seconds
        ):
            summary = f"reminder {job_id} {job['user']} elapsed {job.get('elapsed')}"
            if send(build_card(job, config, reminder=True), config, summary):
                entry["reminded"] = True
    for job_id in [job_id for job_id in tracked if job_id not in running]:
        del tracked[job_id]
    return state


def build_digest(user: str, jobs: list[dict[str, Any]], data: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    limits = data.get("thresholds", {})
    lines = [
        f"<b>Daily GPU utilization check</b>: {len(jobs)} of your jobs averaged below "
        f"{limits.get('lowUtilPercent', '?')}% GPU utilization for {limits.get('minLowHours', '?')}h+ "
        f"in the last {limits.get('windowHours', 24):g}h."
    ]
    for job in jobs:
        lines.append(
            f"• job <b>{html.escape(str(job['id']))}</b> on {html.escape(str(job.get('node') or '?'))} "
            f"({html.escape(str(job.get('state')))}): avg {job.get('avgUtil')}%, "
            f"low for {job.get('longestLowHours')}h, {job.get('idleGpuHours')} idle GPU-hours"
        )
    lines.append("Please put idle nodes to work or release them with <code>scancel &lt;job&gt;</code>.")
    if config.get("publicDashboardUrl"):
        lines.append(f"<a href=\"{config['publicDashboardUrl']}\">Open dashboard → Utilization</a>")
    summary = "<br>".join(lines)
    content = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": [{"type": "TextBlock", "text": f"{user}: {len(jobs)} low-utilization job(s)", "wrap": True}],
    }
    return {
        "type": "message",
        "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": content}],
        "mention": {"email": owner_email(user, config), "name": user},
        "summary": summary,
    }


def digest_due(config: dict[str, Any], state: dict[str, Any], now: datetime) -> bool:
    try:
        hour, minute = (int(part) for part in str(config.get("digestTime") or "").split(":"))
    except ValueError:
        return False
    today = now.date().isoformat()
    return state.get("lastDigestDate") != today and (now.hour, now.minute) >= (hour, minute)


def send_digest(config: dict[str, Any], state: dict[str, Any]) -> None:
    url = config["dashboardUrl"].rstrip("/") + "/api/utilization"
    with urllib.request.urlopen(url, timeout=180) as response:
        data = json.load(response)
    flagged: dict[str, list[dict[str, Any]]] = {}
    for job in data.get("jobs", []):
        if job.get("flagged"):
            flagged.setdefault(job["user"], []).append(job)
    delivered = True
    for user, jobs in flagged.items():
        delivered &= send(build_digest(user, jobs, data, config), config, f"digest {user}: {len(jobs)} low-util job(s)")
    if delivered:
        state["lastDigestDate"] = datetime.now().date().isoformat()
        log(f"daily digest done: {len(flagged)} user(s) flagged")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Poll a single time and exit")
    args = parser.parse_args()
    state = load_state()
    while True:
        config = load_config()
        try:
            state = poll_once(config, state)
            save_state(state)
        except (urllib.error.URLError, OSError, ValueError) as error:
            log(f"poll failed: {error}")
        if state is not None and digest_due(config, state, datetime.now()):
            try:
                send_digest(config, state)
                save_state(state)
            except (urllib.error.URLError, OSError, ValueError) as error:
                log(f"digest failed, will retry next poll: {error}")
        if args.once:
            return
        time.sleep(max(30, int(config["pollSeconds"])))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
