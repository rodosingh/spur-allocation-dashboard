#!/usr/bin/env bash
# Read-only shared dashboard, launched on a SPUR login node by the tunnel
# service in spur-dashboard-tunnel.service. Binds loopback only; the tunnel
# host publishes it.
set -euo pipefail

PORT="${SPUR_DASHBOARD_SHARED_PORT:-18876}"
cd "$(dirname "$0")/.."

# A dropped tunnel can orphan the previous server on the fixed port.
pkill -u "$(id -u)" -f "app.py --read-only --strict-port --port ${PORT}" 2>/dev/null || true
sleep 1

export SPUR_DASHBOARD_READ_ONLY=1
export SPUR_DASHBOARD_CACHE_SECONDS="${SPUR_DASHBOARD_CACHE_SECONDS:-90}"
export SPUR_DASHBOARD_REFRESH_SECONDS="${SPUR_DASHBOARD_REFRESH_SECONDS:-120}"
exec python3 -u app.py --read-only --strict-port --port "${PORT}"
