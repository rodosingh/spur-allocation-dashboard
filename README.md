# SPUR Allocation Control

A dependency-free localhost dashboard for requesting, maintaining, inspecting,
and ending AMD SPUR GPU allocations.

The web app is deliberately a thin control plane:

- **Maintained chains** are mutated only through the bundled `node_holder.sh`
  (override with `SPUR_DASHBOARD_NODE_HOLDER` if your site maintains another
  compatible copy).
- **Normal sessions** are one finite `sbatch` sleep holder and end through
  `scancel`.
- Scheduler policy, account/QoS associations, limits, occupancy, and job state
  are read live. They are not hardcoded in the UI.

## Setup

### Requirements

- A SPUR login host with `squeue`, `sbatch`, `scancel`, and `spur` on `PATH`.
- Python 3.8 or newer (the cluster ships 3.12). No `pip install` step — the app
  uses only the Python standard library.
- Node.js is optional, and only to run the JavaScript tests.

### Get the code

Clone it anywhere you like; it is self-contained (the compatible `node_holder.sh`
and its tests are bundled), so it does not depend on any sibling directory:

```bash
git clone git@github.com:rodosingh/spur-allocation-dashboard.git ~/spur-allocation-dashboard
cd ~/spur-allocation-dashboard
chmod +x node_holder.sh          # normally preserved by git; harmless to re-run
python3 app.py
```

Use `https://github.com/rodosingh/spur-allocation-dashboard.git` instead if you
have not set up an SSH key. The repository is private, so you need access to it.

### Can it live directly in `$HOME`?

Yes. The code's location is irrelevant to how it runs: request history always
goes to `~/.spur-dashboard`, chain state to `~/.node_holder`, and logs are read
from `~/logs`, no matter where `app.py` sits. A subdirectory such as
`~/spur-allocation-dashboard` is the clean choice. You *can* drop the files
loose into `$HOME`, but that clutters your home directory and risks name
collisions (`app.py`, `scheduler.py`, `node_holder.sh`), so it is not advised.

### If you already run `node_holder.sh` elsewhere

The dashboard defaults to its **bundled** `node_holder.sh`. If you already keep a
canonical copy (for example `~/SCRIPTS/node_holder.sh`) whose chains are tended
by cron, point the dashboard at that same file so state, cron entries, and the
frozen per-chain runner all stay consistent:

```bash
SPUR_DASHBOARD_NODE_HOLDER=~/SCRIPTS/node_holder.sh python3 app.py
```

Otherwise a chain started from the dashboard installs cron lines referencing the
bundled copy, while one started from your other copy references that one — both
work, but they are easier to reason about pointing at a single script.

### Verify (optional)

```bash
bash -n node_holder.sh
python3 -m unittest tests.test_node_holder
python3 -m unittest test_app.py
```

## Run

Run on a SPUR login host, from this directory:

```bash
python3 app.py
```

Open `http://127.0.0.1:8876`.

If that port is occupied:

```bash
python3 app.py --port 8877
```

For a remote login host, tunnel from your workstation:

```bash
ssh -L 8876:127.0.0.1:8876 <spur-login-host>
```

The server refuses non-loopback binding unless `--allow-remote` is explicit.
There is CSRF protection but no user authentication, so loopback plus SSH
tunneling is the normal and recommended deployment.

For demonstrations or read-only monitoring:

```bash
python3 app.py --read-only
```

Every POST endpoint returns `403` in read-only mode.

## Request modes

### Chain

Chain mode invokes `node_holder.sh` with validated argv under a single chain
prefix. Supported request fields:

- Job/chain name
- Account and linked QoS
- GPUs, CPUs (`Auto` derives the GPU-proportional share), nodes
- Per-link wall time
- Optional exact node
- Exclusive/shared mode
- Queued depth or rolling runway
- Expiry deadline
- Pin recovery policy
- `start`, `adopt`, and `race`
- Explicit low-priority and preemption acknowledgements

The end-session action runs `node_holder.sh ... release`, which writes the
shared tombstone, disables maintenance, cancels queued successors first, then
cancels the running holder.

### Normal

Normal mode submits one finite job using direct `sbatch`. It requests the same
account/QoS/resources but has no successors or cron maintenance. Its end-session
action runs `scancel <jobid>`.

Normal jobs may not use the active chain prefix. This prevents the dashboard
from ever treating a normal job as a maintained chain.

## Configuration

Nothing is hardcoded to one person's setup; every default is derived or
overridable:

| Variable | Default | Purpose |
|---|---|---|
| `NODEHOLD_NAME` | `interactive` | Prefix used when the dashboard creates a new chain |
| `SPUR_DASHBOARD_CHAIN_PREFIX` | `$NODEHOLD_NAME` | Override the new-chain prefix for the dashboard only |
| `NODEHOLD_MIN_PRIO` | `10000` | Priority floor, shared with `node_holder.sh` |
| `SPUR_DASHBOARD_NODE_HOLDER` | `./node_holder.sh` | Path to a compatible script |
| `SPUR_DASHBOARD_PARTITION` | `amd-spur` | Scheduler partition |
| `SPUR_DASHBOARD_STATE_DIR` | `~/.spur-dashboard` | Request history location |
| `SPUR_DASHBOARD_READ_ONLY` | unset | `1` disables all mutations |

Existing maintained chains are discovered from active queue names plus their
saved `~/.node_holder/<name>.conf` profiles, regardless of prefix. You therefore
see and can release `hold-*`, `interactive-*`, and other prefixes in one
dashboard without configuring anything.

The prefix only controls names of **new chains** submitted from the dashboard.
If you want new chains to follow a convention such as `hold-*`, start the
server with:

```bash
NODEHOLD_NAME=hold python3 app.py
```

Two people on one login host must each pick a distinct `--port`.

## Views

- **Status** — maintained chains, runway, tending state, resources, active links,
  and every job in your personal queue.
- **Pools** — live account/QoS policy, configured priority, preemption mode,
  cap/usage, queue pressure, personal usage, wall limits, and submit limits.
  Click a row to expand all jobs in the QoS; your jobs are highlighted.
- **SQ** — the dashboard equivalent of the personal `sq` alias.
- **SQA** — all cluster jobs with text/state filters. This expensive view loads
  only when opened or manually refreshed.
- **Past requests** — dashboard request provenance plus terminal scheduler
  records from the prior seven days.
- **Diagnostics** — scheduler, name service, home writes, local cron state,
  saved-chain health, alarms, and bounded escaped chain log excerpts.

## Safe chain actions

The chain cards expose:

- `topup`
- `shrink N`
- `arm`
- `clear`
- `tend`
- `untend`
- `release`

Every mutation is allowlisted, passed as an argv array with `shell=False`, and
followed by an authoritative status refresh. Impact-aware confirmations are used
for submission, shrinking, maintenance changes, cancellation, and release.
Release requires typing the chain name.

The browser intentionally does **not** execute:

- `shell` or arbitrary `exec` commands (it provides copyable terminal commands)
- internal `tick`, `__hold`, `__arm`, or `__race`
- `stop` (cron can resurrect stopped jobs)
- broad `release --all`
- arbitrary hooks, filesystem paths, executables, or priority thresholds

## Container warning

Releasing or cancelling Slurm jobs does not stop containers created through the
host Docker daemon. Stop GPU containers before ending an allocation, otherwise
they can survive as orphans and keep consuming GPUs after the node is reassigned.
The UI repeats this warning at the release control.

## Architecture

```text
node_holder.sh               maintained-chain authority
app.py                       localhost HTTP/API server
├── scheduler.py             structured squeue/sacct + finite normal jobs
├── node_holder_bridge.py    validation + allowlisted chain subprocesses
├── request_store.py         atomic per-record JSON history (no SQLite locks)
├── tests/test_node_holder.py
└── static/
    ├── index.html
    ├── styles.css
    └── app.js
```

Request history is stored as one atomically renamed JSON file per record under
`~/.spur-dashboard`. SQLite is intentionally avoided because home is NFS and
the cluster has experienced NFS lock-manager outages.

`node_holder.sh` now exposes stable read-only integration commands:

```bash
NODEHOLD_NAME=hold ./node_holder.sh status-json
./node_holder.sh pools-json
./node_holder.sh doctor-json
```

The existing human-readable `status`, `pools`, and `doctor` output is unchanged.

## API

Read-only:

- `GET /api/capabilities`
- `GET /api/status`
- `GET /api/pools`
- `GET /api/queue?scope=mine|all`
- `GET /api/history`
- `GET /api/diagnostics`
- `GET /api/logs?chain=<name>&kind=tick|arm|on-start`
- `GET /api/csrf-token`

Mutating:

- `POST /api/requests`
- `POST /api/jobs/cancel`
- `POST /api/chains/action`

Mutations require the current CSRF token and a loopback browser origin.

## Tests

No automated test submits or cancels a live job.

```bash
bash -n node_holder.sh
python3 -m unittest tests.test_node_holder

python3 -m unittest -v test_app.py
python3 -m py_compile app.py scheduler.py node_holder_bridge.py request_store.py
```

If Node.js is available:

```bash
node --test test_app.js
```
