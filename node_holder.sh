#!/usr/bin/env bash
# ============================================================================
#  NODE HOLDER  |  GPU allocations on amd-spur
# ============================================================================
#  Request a node, maintain queued successors, and release the whole chain.
#  One chain = one node allocation at a time. Handover is not guaranteed.
#  Full workflows, scheduler snapshot and command reference: ../SPUR_GUIDE.md
#
#  READ IN ORDER, OR JUMP TO A SECTION
#    1  Quick start                 5  GPUs and multiple nodes
#    2  How renewal works           6  Commands at a glance
#    3  Time and chain depth        7  Limits and troubleshooting
#    4  Options                     8  Release and configuration
#
# ----------------------------------------------------------------------------
#  1. QUICK START
# ----------------------------------------------------------------------------
#  Run from /home/aditysin/SCRIPTS on a configured login host.
#
#    export NODEHOLD_NAME=hold       # use existing hold-* names
#    ./node_holder.sh pools         # check current access and limits
#    ./node_holder.sh -n work -q amd-brain-models-qos start
#    ./node_holder.sh -n work status
#    ./node_holder.sh -n work shell
#    ./node_holder.sh -n work release
#
#  Naming: default prefix is interactive; with NODEHOLD_NAME=hold, -n work
#  selects hold-work. Use the same prefix and tag when checking or releasing.
#
#  Common variations (choose the one you need):
#
#    # Two GPUs on a shared node; six-hour jobs; three queued successors
#    ./node_holder.sh -n small -q amd-brain-models-qos -g 2 \
#        --time 06:00:00 --chain 3 start
#
#    # Request this exact node, including after a loss
#    NODEHOLD_REPIN=0 ./node_holder.sh -n exact -q amd-brain-models-qos \
#        -w crsuse2-m2m-026 start
#
#    # Three independent exclusive chains: fleet-1, fleet-2, fleet-3
#    ./node_holder.sh -n fleet -q amd-brain-models-qos -N 3 --exclusive start
#
#    # Expire twelve hours FROM SUBMISSION, including queue time
#    ./node_holder.sh -n temp -q amd-brain-models-qos --for-hours 12 start
#
#    # Add successors behind an existing running single-node job
#    ./node_holder.sh -n work adopt JOBID
#
# ----------------------------------------------------------------------------
#  2. HOW RENEWAL WORKS
# ----------------------------------------------------------------------------
#  With --chain 3, a populated chain has FOUR jobs:
#
#      running head             three queued successors
#    +--------------+      +----------+   +----------+   +----------+
#    |    holder    | ---> |  next 1  |-->|  next 2  |-->|  next 3  |
#    +--------------+      +----------+   +----------+   +----------+
#                          afterany:<previous job> + same target node
#
#  An arrow means "eligible after the previous job ends", not reserved access.
#  The scheduler still checks quotas, competing jobs and available resources.
#  Queued jobs can continue if maintenance stops, but may wait or fail to start.
#
#    start / adopt --> save state + install local OS cron
#                              |
#                        every five minutes
#                              v
#                   check chain --> refill successors
#                              |
#                       repeat until release
#                       or a configured expiry
#
#  start also arms an initial waiter. Cron maintains the chain afterward.
#  Without -w, recovery may choose a different node. With -w, it waits for
#  that node initially; after losing a held node it may repin (section 5).
#  Set NODEHOLD_REPIN=0 for a strict pin. No depth guarantees uninterrupted access.
#
#  Each successor starts NEW processes. Use checkpoint/restart for work that
#  crosses job boundaries. A tmux session is not guaranteed to survive handover.
#  For a finite workload, edit batch_job.sh and submit it directly instead.
#
# ----------------------------------------------------------------------------
#  3. TIME AND CHAIN DEPTH
# ----------------------------------------------------------------------------
#  Setting                   Controls                        Example
#  ------------------------  ------------------------------  ---------------
#  --time T                  Wall limit of EACH job          --time 06:00:00
#  --chain N                 Number of QUEUED successors     --chain 3
#  --days N / --hours N      Rolling queued runway           --days 4
#  --for-days / --for-hours  Expiry FROM SUBMISSION          --for-hours 12
#
#    --days 4      keep replenishing four days of queued runway until release
#    --for-days 4  set a deadline four days after submission
#
#  --days/--hours are additive and rounded UP to whole successors. Choose
#  either runway or explicit --chain depth. Default: seven queued successors.
#  A nominal 24h job normally sleeps 23h57m, leaving a three-minute margin:
#  --days 3 therefore requests FOUR successors; --chain 3 means FOUR total jobs.
#
#  --time accepts HH:MM:SS or D-HH:MM:SS and is trimmed to live limits.
#  It must exceed three minutes. The partition ceiling was 24h on 10 Sep 2026;
#  current QoS/partition settings remain authoritative.
#
#  Expiry includes time spent pending. Current payloads observe the deadline;
#  cron cancels remaining jobs on its next tick (normally within five minutes).
#  Outages can delay cleanup. Neither expiry nor release cleans up detached
#  host containers; stop your workloads before releasing the allocation.
#
# ----------------------------------------------------------------------------
#  4. OPTIONS
# ----------------------------------------------------------------------------
#  -n, --name TAG       Chain tag, not Slurm task count
#  -q, --qos QOS        QoS to use; resolve a granted account if omitted
#  -A, --account ACCT   Account to use; resolve QoS if omitted
#  -w, --node NODE      Persist one exact target; not usable with multi-chain -N
#  --pair TAG           Partner tag in this prefix (or its full chain name)
#  -g, --gpus N         GPUs per allocation: 0..8; eight means all GPUs
#  -c, --cpus N         CPUs per allocation; 0/unset derives a proportional
#                       share for 1..7 GPUs. Without -w, samples the first node
#                       in the partition; set -c on heterogeneous hardware.
#                       Failed capacity lookup requires an explicit -c.
#  -N, --nodes N        Independent chains TAG-1 through TAG-N
#  --exclusive         Reserve a whole node
#  --no-exclusive      Permit sharing
#  --time T            Per-job wall limit
#  --chain N           Queued successor count (zero is allowed)
#  --days / --hours N   Rolling runway; aliases: -days / -hours
#  --for-days /        Expiry duration; start/adopt only
#    --for-hours N
#  --any-qos           Save an explicit bypass of the QoS priority floor
#  --all               With release: every chain under the selected prefix
#
#  Put flags before the command, especially adopt JOBID, shrink N and
#  exec COMMAND. Commands without positional arguments also accept flags after
#  the command (start -N 2). Explicit options override saved profiles.
#  Unknown options and options used with the wrong command are rejected.
#
# ----------------------------------------------------------------------------
#  5. GPUs AND MULTIPLE NODES
# ----------------------------------------------------------------------------
#  Below eight GPUs, allocations default to shared unless exclusivity is
#  explicitly requested. --exclusive reserves the whole node even with -g 2.
#
#    -n fleet -N 3 --exclusive        one sbatch -N 3 allocation
#    ------------------------        -------------------------
#    fleet-1 --> node A               one coordinated job
#    fleet-2 --> node B               across three nodes
#    fleet-3 --> node C               (needs a distributed launcher)
#    may start at different times
#
#  Multi-chain -N requires exclusivity for distinct nodes: use --exclusive
#  with a partial GPU request. Start can partially succeed; inspect status
#  before retrying. Separate chains are not synchronized distributed ranks.
#
#  To divide ONE node between two chains, pin both to the same node with -w and
#  start them separately. Pick a node that is completely idle: an unpinned ask
#  lands wherever there is room, which may be a node a stranger already sits on,
#  and then the second half gets only what is left. On 2026-09-10 a half-node
#  ask landed beside someone else's 4-GPU job, which used the last four GPUs,
#  so the second half could never fit until that job ended.
#
#    sinfo -N -p amd-spur -t idle -o '%N'          # find an empty node
#    ./node_holder.sh -n half1 -q QOS -g 4 -w NODE --pair half2 start
#    ./node_holder.sh -n half2 -q QOS -g 4 -w NODE --pair half1 start
#
#  --pair names the other half so recovery can try to reunite them. A
#  pin is normally waited out however long it takes, but a node this chain had
#  and lost has nothing left to wait for, so the tick moves the chain: onto the
#  partner's node when the partner still holds one with room, else onto the
#  lowest-named idle node, a choice both halves reach alike without conferring.
#  NODEHOLD_REPIN=0 turns this off and is saved for later cron ticks.
#  Pairing is best effort; idle capacity can change before either job starts.
#
#  An idle-node snapshot does not reserve capacity for either request.
#
#  Pinning does not beat a QOS cap: a pinned job on a full pool waits for a cap
#  slot AND for that node to still be free when the slot arrives.
#
#  Use only the GPU IDs assigned to your allocation, even if rocm-smi shows
#  all eight. Shared allocations skip the default whole-node Docker hook;
#  provide an allocation-aware NODEHOLD_ON_START if a custom hook is needed.
#
# ----------------------------------------------------------------------------
#  6. COMMANDS AT A GLANCE
# ----------------------------------------------------------------------------
#  ALLOCATE AND ACCESS
#    start           Take a node and keep it
#    adopt JOBID     Build a chain behind a job you already have
#    race            Queue in every eligible pool, keep the one that lands
#    shell           Attach to the persistent tmux session on the node
#    exec COMMAND    Run one command there, non-interactively
#
#  INSPECT
#    status          Chains, jobs, node, queued depth, pool, expiry
#    pools           Access, caps, usage and limits, as they are right now
#    doctor          Scheduler, home writes, local cron, account membership
#
#  MAINTAIN
#    topup           Refill the queued successors now
#    shrink N        Save a smaller queued depth, permanently
#    arm             Wait in the background, top up once a node lands
#    tick            One maintenance pass; this is what cron runs
#    clear           Clear the retry backoff so the next tick retries at once
#    tend / untend   Install / remove local cron, leaving the jobs alone
#
#  STOP
#    stop            Cancel the jobs only; maintenance can recreate the chain
#    release         Disable renewal and cancel the chain (section 8)
#
# ----------------------------------------------------------------------------
#  7. LIMITS AND TROUBLESHOOTING
# ----------------------------------------------------------------------------
#  Start with pools, status and doctor. Associations and caps can change.
#
#  Observation                    What it tells you
#  -----------------------------  -------------------------------------------
#  PENDING: QOSGrpNodeLimit       The QoS node cap blocks it. Usually just a
#                                 queue: you wait your turn. But a successor
#                                 waiting is also how a handover is lost.
#  Missing account association    Verify exact user and granted QoS. Successful
#                                 lookup normally rejects this before submit.
#  Repeated early termination     Check job reasons, logs and current access.
#                                 SIGTERM alone does not establish the cause.
#  Fewer successors than asked    Check submit limits, quota and top-up errors.
#  Maintenance goes quiet         Check home writes, and cron on the tending
#                                 host. A full home fails writes silently.
#
#  QoS priority below NODEHOLD_MIN_PRIO (default 10000), or unknown priority,
#  refuses submission. --any-qos is a saved override. Preemption mode is a
#  separate field: high priority is not immunity. Jobs sharing one QoS also
#  share its priority, so it breaks no ties between them, and ordering further
#  depends on placement, dependencies and the scheduler. Priority cannot be
#  inferred from QoS alone; resource requests and placement also affect starts.
#
#  Node caps belong to the whole QoS pool, not each user. Queued successors
#  consume submit slots (MaxSubmitPU), but not running-node quota. Submit
#  limits count running + pending across accounts sharing that QoS. Top-up
#  can leave less runway than requested. High priority bypasses none of these.
#
# ----------------------------------------------------------------------------
#  8. RELEASE AND CONFIGURATION
# ----------------------------------------------------------------------------
#    release --> shared disable marker --> remove local cron/race memberships
#            --> cancel QUEUED jobs --> cancel RUNNING job --> verify cleanup
#
#  Retry release if cancellation is unconfirmed. Explicit start re-enables
#  the name. An adopted external seed with a different name is not cancelled.
#
#    ./node_holder.sh -n work release        # one chain
#    ./node_holder.sh -n fleet -N 3 release  # just fleet-1 .. fleet-3
#    ./node_holder.sh release --all          # prefix-wide, dormant members too
#
#  Cron belongs to the login host where it was installed; another host may
#  still tend a chain even when local cron is absent. Shared disable markers
#  block current-version maintenance on all hosts. Old frozen maintenance
#  processes may not understand them and need to be stopped separately.
#
#  State/logs need writable shared home storage. doctor probes for write
#  failures; alarms also go to /tmp. The on-start hook runs for every holder
#  and must tolerate repetition. Existing chains use saved script snapshots;
#  editing this file does not update those copies or already queued payloads.
#
#  Environment defaults (NODEHOLD_ prefix on every name below):
#    NAME=interactive       DIR=~/.node_holder (keep on shared home)
#    CHAIN=7                GPUS=8          NODES=1       EXCLUSIVE=1
#    TMUX=1                 TEND=1          ARM=1         REQUEUE=0
#    MIN_PRIO=10000         ON_START=<state directory>/on_start
#
#  Set NODEHOLD_TIME or NODEHOLD_HOURS to request a wall time explicitly.
#  Otherwise NODEHOLD_WANT defaults to 7-00:00:00, trimmed to live limits.
#  NODEHOLD_TEND=0 opts out of cron; the initial arm waiter does not replace
#  ongoing maintenance. Partial GPU requests default to sharing unless an
#  exclusivity setting is supplied explicitly.
# ============================================================================
set -uo pipefail

# A login shell picks both of these up from /etc/profile.d; cron does not run
# one, so it gets PATH=/usr/bin:/bin and no controller address, and every Slurm
# command fails in a way that reads like the scheduler being down. Put them back
# rather than have the cron line carry them, so it needs no setup of its own.
case :$PATH: in *:/usr/local/bin:*) ;; *) PATH=/usr/local/bin:$PATH ;; esac
if [ -z "${SPUR_CONTROLLER_ADDR:-}" ] && [ -r /etc/environment ]; then
    set -a; . /etc/environment; set +a
fi
[ -n "${SPUR_CONTROLLER_ADDR:-}" ] || . /etc/profile.d/spur.sh 2>/dev/null || true

say() { printf '%s\n' "$*"; }
die() { printf 'node_holder: %s\n' "$*" >&2; exit 1; }
fld() { printf '%s' "$2" | cut -d'|' -f"$1"; }

# Small, dependency-free JSON primitives for the dashboard-facing read commands.
# Scheduler names are normally plain ASCII, but escaping here makes the output a
# real contract rather than "JSON as long as nobody used a quote in a job name".
json_quote() {
    local s=${1-}
    s=${s//\\/\\\\}; s=${s//\"/\\\"}
    s=${s//$'\n'/\\n}; s=${s//$'\r'/\\r}; s=${s//$'\t'/\\t}
    printf '"%s"' "$s"
}
json_number_or_null() {
    case ${1:-} in ''|*[!0-9]*) printf 'null' ;; *) printf '%d' "$((10#$1))" ;; esac
}
json_bool() { [ "${1:-0}" = 1 ] && printf true || printf false; }

# Everything this script leans on -- the state files, the lock files, the
# per-chain snapshots, the log cron appends to -- sits in $HOME, which is NFS
# under a per-user quota. Go over that quota and every one of those writes
# returns EDQUOT, the log line that would report it included, so the failure is
# perfectly silent: on 2026-08-31 ten hours of ticks left not one line behind
# and all three chains drained. The node's own /tmp is the one place still able
# to take a note when that happens, so say it in both places.
alarm() {
    local when host
    when=$(date -Is); host=$(hostname -s 2>/dev/null || echo '?')
    say "${when} [${host}] $*"
    printf '%s [%s] %s\n' "$when" "$host" "$*" \
        >> "/tmp/node_holder.$(id -un 2>/dev/null || echo user).alarm" 2>/dev/null || true
}

# True while $HOME will still take a write. Probing for it beats asking `quota`,
# which returns nothing whatsoever over this mount and so cannot tell being
# under the limit apart from not being told the limit.
home_writable() {
    local probe=${STATE_DIR}/probe.$$
    printf 'x' > "$probe" 2>/dev/null && { rm -f "$probe" 2>/dev/null; return 0; }
    rm -f "$probe" 2>/dev/null
    return 1
}

# One byte landing says nothing about the next megabyte, and the interesting
# moment is the one before the quota bites, not after. df is no help -- it
# reports the whole 10T volume, of which there is always plenty -- so the only
# honest measure of headroom is to try to use some and see. Twice an hour,
# because the chunk is real I/O; set NODEHOLD_HEADROOM_MB=0 to skip it.
home_headroom_ok() {
    local mb=${NODEHOLD_HEADROOM_MB:-64} every=${NODEHOLD_HEADROOM_EVERY:-1800}
    local stamp=${STATE_DIR}/headroom probe last now
    case $mb in ''|*[!0-9]*) return 0 ;; esac
    [ "$mb" -gt 0 ] || return 0

    now=$(date +%s); last=$(cat "$stamp" 2>/dev/null || true)
    case ${last:-} in ''|*[!0-9]*) last=0 ;; esac
    [ $(( now - last )) -ge "$every" ] || return 0
    printf '%s' "$now" > "$stamp" 2>/dev/null

    probe=${STATE_DIR}/headroom.$$
    if dd if=/dev/zero of="$probe" bs=1M count="$mb" 2>/dev/null; then
        rm -f "$probe" 2>/dev/null; return 0
    fi
    rm -f "$probe" 2>/dev/null
    return 1
}

# Who and where, asked of the system rather than the environment: under
# `spur exec` $USER reads "root" and $HOME reads /opt/spur, while `id -un` and
# the passwd entry are right in every context. That lookup goes over the network
# though, and it does fail now and then -- so fall back rather than carry on
# with a blank name, which would quietly match no jobs at all in squeue.
ME=$(id -un 2>/dev/null) || ME=""
[ -n "$ME" ] || ME=$(getent passwd "$(id -u 2>/dev/null)" 2>/dev/null | cut -d: -f1)
[ -n "$ME" ] || ME=${USER:-${LOGNAME:-}}
[ -n "$ME" ] || die "cannot tell who you are -- id, the passwd map and \$USER all came up empty"

HOME_DIR=$(getent passwd "$ME" 2>/dev/null | cut -d: -f6)
[ -n "$HOME_DIR" ] || HOME_DIR=${HOME:-}
case $HOME_DIR in ''|/opt/spur) HOME_DIR=/home/${ME} ;; esac
[ -d "$HOME_DIR" ] || die "cannot find the home directory of ${ME} (tried ${HOME_DIR})"

# Compute nodes re-run this file by absolute path, so it must sit on shared
# storage (anywhere under /home is fine). Background waiters run from a private
# copy of it and are handed the real path here, so that editing this file cannot
# derail one that is already running.
SELF=${NODEHOLD_SELF:-$(readlink -f "${BASH_SOURCE[0]}")}
LOG_DIR=${HOME_DIR}/logs

# Everything kept per chain -- state, profile, the frozen runner, the race file
# and its lock -- used to sit loose in $HOME as dotfiles, two dozen of them once
# three chains were running. They go here instead. It has to stay somewhere under
# /home: compute nodes re-run the frozen runner by absolute path, so this whole
# directory needs to be on shared storage rather than anywhere node-local.
STATE_DIR=${NODEHOLD_DIR:-${HOME_DIR}/.node_holder}
mkdir -p "$STATE_DIR" 2>/dev/null || true

# Lock files are the one thing that must NOT live on $STATE_DIR. flock() over
# NFSv3 depends on the server's Network Lock Manager (rpc.lockd/statd); when that
# goes -- as it did in the 2026-09-22 spur upgrade -- every flock() wedges in
# uninterruptible D-state on an rpc_wait_bit and never returns, so -w timeouts
# never fire and Ctrl-C does nothing. Since mutation_lock guards start/release/
# tick/topup, one hung flock stops all submission. Locks hold no data, so keep
# them on local disk: the guarantee narrows from cluster-wide to per-host, which
# is all that is achievable while NLM is down anyway, and contention here is
# almost always same-host (cron + a manual run on one login node).
LOCK_DIR=${NODEHOLD_LOCK_DIR:-/tmp/nodeholder-$(id -un 2>/dev/null || echo "$USER")}
mkdir -p "$LOCK_DIR" 2>/dev/null && [ -w "$LOCK_DIR" ] || LOCK_DIR=/tmp

PREFIX=${NODEHOLD_NAME:-interactive}
PARTITION=${NODEHOLD_PARTITION:-amd-spur}
# Empty means "work it out", which resolve_pool does by asking the scheduler
# which pools this user holds and taking the best. This was hardcoded to
# amd-burst until 2026-09-02, which put every new chain in the worst pool
# available -- priority 100 and preemptible, against 10000 everywhere else --
# and left the good pools as something you had to know to name. Either an
# account or a QOS may be given here; resolve_pool tells them apart.
ACCOUNT=${NODEHOLD_ACCOUNT:-}
GPUS=${NODEHOLD_GPUS:-8}              # 0 = don't ask for GPUs
# Left at 0 the scheduler applies its own default, which is a single CPU: fine
# for a holder that only sleeps, useless for a job meant to do work, and the
# reason a node can show 235 of 236 CPUs idle while still being held. Set it
# when two jobs are meant to divide one node, so the first does not have to be
# trusted to leave the rest behind.
CPUS=${NODEHOLD_CPUS:-0}              # 0 = derive from the GPU share
# How many links are queued behind the holder, so the runway is one more than
# this times whatever a single link is allowed to ask for: seven behind a holder
# on a one-day wall clock is eight days, a day of slack on the week that is the
# point of the thing. This was 3 while amd-burst-qos capped you at 4 submitted
# jobs; that cap was lifted on 2026-08-31, and queued links cost nothing against
# the node quota, which counts only what is running.
CHAIN=${NODEHOLD_CHAIN:-7}
EXCLUSIVE=${NODEHOLD_EXCLUSIVE:-1}    # 0 = share the node with other jobs
USE_TMUX=${NODEHOLD_TMUX:-1}          # 0 = `shell` is a plain, non-persistent bash

# Which node the holder asks for. Empty lets the scheduler choose, which is what
# a fresh hold wants; naming one is for taking back a particular node, or for
# landing beside work already sitting on it. Only the holder carries this -- the
# successors are pinned to whatever node it actually got, which is not always
# what was asked for.
PIN_NODE=${NODEHOLD_NODE_WANT:-}

# The other half of a split node. Two chains dividing one node are put together
# by pinning both to it, but a pin only names a node: when that node goes, the
# two have no way to agree on a replacement and drift onto separate ones. Naming
# the partner here lets a repin follow it instead of choosing for itself. Set it
# on both halves -- whichever moves first is the one the other follows.
PAIR=${NODEHOLD_PAIR:-}
REPIN=${NODEHOLD_REPIN:-1}

# Refuse pools a higher-priority job can take the node back from. A chain exists
# to keep a node put, and amd-burst-qos (priority 100, PreemptMode=cancel) cannot
# do that -- yet it is a pool this user holds, so until now nothing stopped a
# chain quietly landing there and being cancelled out from under itself. Set
# --any-qos to submit anyway.
ANY_QOS=${NODEHOLD_ANY_QOS:-0}

# How many nodes to hold. Each one is its own chain, because a chain is the
# thing that survives a handover and two nodes in one job would both be lost
# the moment either link ended. --nodes 3 -n big gives big-1, big-2, big-3.
WANT_NODES=${NODEHOLD_NODES:-1}

# How long the hold should last, in seconds, as opposed to how long one link
# lasts. Nothing on amd-spur may ask for more than a day at a time, so a hold
# measured in days is a matter of chain depth rather than wall clock: this is
# turned into CHAIN by depth_for_hold once the per-link limit is known. 0 leaves
# CHAIN alone.
HOLD_FOR_S=0
FINITE_FOR_S=0
EXPIRES_AT=0

# Whether --exclusive was actually chosen, as against left at its default. Worth
# distinguishing: asking for 2 GPUs and taking the node exclusively spends a
# whole node off your QOS cap to use a quarter of it, which is almost never what
# was meant -- but it is a legitimate thing to ask for, so only the default
# gives way.
EXCL_SET=${NODEHOLD_EXCLUSIVE:+1}; EXCL_SET=${EXCL_SET:-0}

# How long each link asks for. Left alone this is "as long as this pool allows",
# read from the QOS and the partition at submit time, because the ceilings differ
# per pool and change without notice: asking for one pool's maximum in another
# either wastes the difference or hangs on a limit. Set NODEHOLD_HOURS or
# NODEHOLD_TIME to pin it; either way the ask is still trimmed to fit.
WANT=${NODEHOLD_WANT:-7-00:00:00}
if [ -n "${NODEHOLD_TIME:-}" ]; then      TIME_LIMIT=$NODEHOLD_TIME;   TIME_PINNED=1
elif [ -n "${NODEHOLD_HOURS:-}" ]; then   TIME_LIMIT=${NODEHOLD_HOURS}:00:00; TIME_PINNED=1
else                                      TIME_LIMIT=$WANT;            TIME_PINNED=0
fi

# Slurm writes a wall clock as mm, mm:ss, hh:mm:ss, d-hh, d-hh:mm or d-hh:mm:ss.
time_to_seconds() {
    local t=$1 d=0 h=0 m=0 s=0 day=no f
    case $t in
        *[!0-9:-]*) printf '%d' $(( 365 * 86400 )); return ;;   # UNLIMITED and friends
    esac
    case $t in *-*) d=${t%%-*}; t=${t#*-}; day=yes ;; esac
    IFS=: read -ra f <<< "$t"
    case ${#f[@]}:$day in
        1:no)  m=${f[0]} ;;
        1:yes) h=${f[0]} ;;
        2:no)  m=${f[0]}; s=${f[1]} ;;
        2:yes) h=${f[0]}; m=${f[1]} ;;
        3:*)   h=${f[0]}; m=${f[1]}; s=${f[2]} ;;
    esac
    printf '%d' $(( 10#$d * 86400 + 10#$h * 3600 + 10#$m * 60 + 10#$s ))
}

seconds_to_time() {
    local s=$1 d h m
    d=$(( s / 86400 )); s=$(( s % 86400 ))
    h=$(( s / 3600 ));  s=$(( s % 3600 ))
    m=$(( s / 60 ));    s=$(( s % 60 ))
    [ "$d" -gt 0 ] && { printf '%d-%02d:%02d:%02d' "$d" "$h" "$m" "$s"; return; }
    printf '%02d:%02d:%02d' "$h" "$m" "$s"
}

# Stop a few minutes short of the wall clock so each link ends COMPLETED rather
# than killed; `afterany` fires either way, but a clean exit is predictable.
HOLD_SECONDS=${NODEHOLD_HOLD_SECONDS:-$(( $(time_to_seconds "$TIME_LIMIT") - 180 ))}

# --- chains ----------------------------------------------------------------
# A chain is just the jobs sharing one job name, which is how it is found again.
# The default chain is named $PREFIX, any other is "$PREFIX-<tag>".

JOB_NAME=""; STATE=""; TMUX_SOCK=""; PROFILE=""; ARM_PID=""; RUNNER=""; LOST=""
STRIKES=""

use_chain() {
    case $1 in ''|*[!a-zA-Z0-9_.-]*) die "chain names may contain only letters, digits, _, . and -" ;; esac
    JOB_NAME=$1
    STATE=${STATE_DIR}/${JOB_NAME}.state
    PROFILE=${STATE_DIR}/${JOB_NAME}.conf
    ARM_PID=${STATE_DIR}/${JOB_NAME}.arm.pid
    RUNNER=${STATE_DIR}/${JOB_NAME}.run.sh
    LOST=${STATE_DIR}/${JOB_NAME}.lost
    STRIKES=${STATE_DIR}/${JOB_NAME}.strikes
    TMUX_SOCK=/tmp/${ME}-${JOB_NAME}.tmux.sock   # /tmp is shared by every user
}

# The file each link of this chain actually runs. A job re-reads this script off
# /home by absolute path as it goes, so editing the original while a holder is
# sleeping in it makes bash resume at a byte offset that no longer means what it
# did, and it starts running fragments of whatever is now there. Give the chain a
# copy taken when it was created and leave it alone until the chain ends. Made
# once and reused, so successors queued days later run the same code as the
# holder they follow. Prints the path to use, or the original if copying failed.
chain_runner() {
    [ -s "$RUNNER" ] || cp -f "$SELF" "$RUNNER" 2>/dev/null || { printf '%s' "$SELF"; return; }
    printf '%s' "$RUNNER"
}

# `topup` now runs on the login node, hours or days after `start`, so what the
# chain was created with has to be remembered. Re-deriving it from the defaults
# would put successors in the wrong account or wall clock and lose the node.
save_profile() {
    local tmp=$PROFILE.new key
    {
        for key in PARTITION ACCOUNT GPUS CPUS EXCLUSIVE TIME_LIMIT CHAIN HOLD_SECONDS PIN_NODE PAIR REPIN ANY_QOS EXPIRES_AT; do
            printf '%s=%q\n' "$key" "${!key}"
        done
        printf 'QOS=%q\n' "${NODEHOLD_QOS:-}"
    } > "$tmp" && mv "$tmp" "$PROFILE" || die "cannot save $PROFILE"
}

load_profile() {
    [ -f "$PROFILE" ] || return 0
    local QOS=""
    PIN_NODE=""; PAIR=""; REPIN=1; ANY_QOS=0; EXPIRES_AT=0
    . "$PROFILE" 2>/dev/null || return 0

    # A variable set for this invocation still beats what was saved.
    PARTITION=${NODEHOLD_PARTITION:-$PARTITION}
    ACCOUNT=${NODEHOLD_ACCOUNT:-$ACCOUNT}
    GPUS=${NODEHOLD_GPUS:-$GPUS}
    CPUS=${NODEHOLD_CPUS:-$CPUS}
    EXCLUSIVE=${NODEHOLD_EXCLUSIVE:-$EXCLUSIVE}
    TIME_LIMIT=${NODEHOLD_TIME:-$TIME_LIMIT}
    CHAIN=${NODEHOLD_CHAIN:-$CHAIN}
    HOLD_SECONDS=${NODEHOLD_HOLD_SECONDS:-$HOLD_SECONDS}
    NODEHOLD_QOS=${CLI_QOS:-$QOS}
    PIN_NODE=${NODEHOLD_NODE_WANT:-$PIN_NODE}
    PAIR=${NODEHOLD_PAIR:-$PAIR}
    REPIN=${NODEHOLD_REPIN:-$REPIN}
    ANY_QOS=${NODEHOLD_ANY_QOS:-$ANY_QOS}
}

use_tag() {
    local tag=${1:-}
    if [ -n "$tag" ]; then use_chain "${PREFIX}-${tag}"; else use_chain "$PREFIX"; fi
}

tag_of() { [ "$1" = "$PREFIX" ] && printf '(default)' || printf '%s' "${1#"${PREFIX}"-}"; }

all_chains() {
    squeue -u "$ME" -h -o "%j" 2>/dev/null \
        | awk -v p="$PREFIX" '$0==p || index($0, p"-")==1' \
        | sort -u
}

# Every chain of this prefix that exists at all, whether or not it currently has
# jobs. all_chains reads the queue, so a chain whose links have just been
# preempted looks identical there to one that was never created -- and it is
# exactly the chain with nothing in the queue that most needs releasing, because
# it is the one cron is about to submit a fresh holder for.
known_chains() {
    local p names
    names=$(all_chains) || return 1
    { printf '%s\n' "$names"
      for p in "$STATE_DIR"/*.conf; do
          [ -e "$p" ] || continue
          basename "$p" .conf
      done
      for p in "$STATE_DIR"/*.race; do
          [ -f "$p" ] || continue
          cut -d'|' -f1 "$p"
      done
    } | awk -v p="$PREFIX" '$0==p || index($0, p"-")==1' | sort -u
}

# An explicit -n wins; otherwise the only chain there is, otherwise the default.
resolve_chain() {
    # Dashboard integrations sometimes know the full scheduler name but not the
    # prefix/tag split used when the chain was created. An exact name also avoids
    # the ambiguity between nested names such as hold-a and hold-a-b.
    if [ -n "${NODEHOLD_CHAIN_FULL_NAME:-}" ]; then
        use_chain "$NODEHOLD_CHAIN_FULL_NAME"
        load_profile
        return 0
    fi
    [ -n "$TAG" ] && { use_tag "$TAG"; load_profile; return 0; }

    local names count
    names=$(all_chains) || die "cannot query scheduler; specify the chain explicitly or retry"
    count=$(printf '%s' "$names" | grep -c . )

    case "$count" in
        0) use_tag "" ;;
        1) use_chain "$names" ;;
        *) printf '%s\n' "$names" | grep -qx -- "$PREFIX" && { use_tag ""; load_profile; return 0; }
           die "several chains running ($(printf '%s' "$names" | tr '\n' ' ')) -- pick one with -n <name>" ;;
    esac
    load_profile
}

# --- queue -----------------------------------------------------------------

# This chain's jobs as "jobid|state|node|reason", oldest first. Matching on the
# name column keeps this independent of whether `squeue -n` is supported.
chain_rows() {
    squeue -u "$ME" -h -o "%i|%j|%T|%N|%r" 2>/dev/null \
        | awk -F'|' -v n="$JOB_NAME" '$2==n {print $1"|"$3"|"$4"|"$5}' \
        | sort -t'|' -k1,1n
}

running_row()   { chain_rows | awk -F'|' '$2=="RUNNING" {print; exit}'; }

# A held job is not a queued job. Its priority is zero, nothing will ever
# schedule it, and there is no way back: this cluster ships no scontrol and spur
# has no verb for releasing one, so the only cure is to cancel it and submit
# another. JobHoldMaxRequeue is the one that arrives unasked -- requeue a job
# enough times and Slurm stops trying. Counting these as runway is what let a
# chain sit "1 queued" for half an hour with nothing that could ever start.
held_rows()    { chain_rows | awk -F'|' '$4 ~ /^Job(Hold|Held)/'; }

# Putting a chain straight back every five minutes is only persistence if the
# chain can survive being started. amd-hyperloom took 58 holders over a day and a
# half and destroyed 55 of them within ten seconds, because nothing here could
# tell "waiting its turn" from "killed on arrival". So each replacement that does
# not stick doubles the wait before the next: a pool that is briefly hostile is
# still retried, and one that is hostile all day is left alone.
backoff_until() { awk -F'|' 'NR==1 {print $2+0}' "$STRIKES" 2>/dev/null; }
backoff_clear() { rm -f "$STRIKES" 2>/dev/null; }

# Records a strike and echoes "<attempt>|<seconds waited before the next one>".
backoff_note() {
    local n s max=${NODEHOLD_BACKOFF_MAX:-7200}
    n=$(awk -F'|' 'NR==1 {print $1+0}' "$STRIKES" 2>/dev/null); n=$(( ${n:-0} + 1 ))
    s=$(( 300 * (1 << ( $([ "$n" -gt 8 ] && echo 8 || echo "$n") - 1 )) ))
    [ "$s" -gt "$max" ] && s=$max
    printf '%s|%s' "$n" "$(( $(date +%s) + s ))" > "$STRIKES" 2>/dev/null
    printf '%s|%s' "$n" "$s"
}
pending_count() {
    chain_rows | awk -F'|' '$2=="PENDING" && $4 !~ /^Job(Hold|Held)/' | wc -l | tr -d ' '
}

# An empty `squeue -u` is ambiguous: it is also what comes back when the name
# lookup on this login node fails, which it does intermittently. The cluster is
# never actually idle, so a non-empty queue overall says the answer was real.
scheduler_up() { squeue -h -o '%i' >/dev/null 2>&1; }

# The directory service behind the login nodes drops out from time to time. Reads
# survive it because $USER is still set, but sbatch refuses everything with
# "failed to determine current username" and jobs that start while it is down are
# cancelled seconds in. Worth naming, because it looks like a rejection.
name_service_up() { [ -n "$(getent passwd "$ME" 2>/dev/null)" ]; }

# "account|timelimit" as submitted. The queue is the authority on this; a saved
# profile can be missing or stale, and guessing the account wrong puts the next
# link in the wrong pool, which is how a node gets lost at a handover.
chain_facts() {
    squeue -u "$ME" -h -o "%j|%a|%l" 2>/dev/null \
        | awk -F'|' -v n="$JOB_NAME" '$1==n {print $2"|"$3; exit}'
}

# Ids are handed out in ascending order, so the newest job is the chain's tail.
chain_tail() { chain_rows | awk -F'|' 'END {print $1}'; }

# $1 = job to wait for (empty for the first), $2 = node to pin to (empty to let
# the scheduler choose). Echoes the new job id.
submit_link() {
    local dep=$1 node=$2 args=()

    # A chain restored from a profile arrives with its pool already named, and
    # every other caller resolves one first. This is for the case neither
    # happened -- a chain whose profile has been lost, topped up from cron --
    # where submitting with an empty -A would be refused for no obvious reason.
    [ -n "${ACCOUNT:-}" ] || resolve_pool

    [ ! -f "${STATE_DIR}/${JOB_NAME}.released" ] || die "${JOB_NAME} is released; explicitly start it to re-enable submissions"
    [ "$EXPIRES_AT" = 0 ] || [ "$(date +%s)" -lt "$EXPIRES_AT" ] || die "hold deadline has passed"
    require_hi_prio
    derive_cpus "$node" >&2
    args=(--parsable -J "$JOB_NAME" -p "$PARTITION" -A "$ACCOUNT" -N 1 -t "$TIME_LIMIT"
          -o "${LOG_DIR}/${JOB_NAME}.%j.out" -e "${LOG_DIR}/${JOB_NAME}.%j.err")
    [ "$EXCLUSIVE" = 1 ] && args+=(--exclusive)

    # Off, and not automatic. Under a pool that preempts by requeueing this flag
    # is what gets a link back instead of destroyed, which is why it was briefly
    # switched on by itself -- but these pools preempt every few minutes, and a
    # job requeued more than a handful of times is held on JobHoldMaxRequeue.
    # Held is the worse end: a cancelled link is resubmitted on the next tick,
    # whereas nothing on this cluster can release a held one. Job 91167 sat held
    # at priority zero for half an hour on 2026-08-31 having never run.
    [ "${NODEHOLD_REQUEUE:-0}" = 1 ] && args+=(--requeue)
    [ "$GPUS" -gt 0 ] 2>/dev/null && args+=(--gres="gpu:${GPUS}")
    [ "${CPUS:-0}" -gt 0 ] 2>/dev/null && args+=(-c "$CPUS")
    [ -n "$dep" ]  && args+=(-d "afterany:${dep}")
    [ -n "$node" ] && args+=(-w "$node")
    [ -n "${NODEHOLD_QOS:-}" ] && args+=(-q "$NODEHOLD_QOS")

    # The name has to travel with the job: the payload runs on a compute node
    # with none of this script's arguments. It runs the chain's own copy, but is
    # told where the real script is, so that what it prints is something you can
    # paste back here rather than a path to a snapshot.
    local wrap
    printf -v wrap 'NODEHOLD_SELF=%q exec bash %q __hold %q' "$SELF" "$(chain_runner)" "$JOB_NAME"
    sbatch "${args[@]}" --wrap "$wrap"
}

# Top the queued successors up to $CHAIN, pinned to $1. $2 is the job to queue
# behind when our own chain is still empty (that is how `adopt` works). Only
# ever appends, so it is safe to re-run.
topup() {
    local node=$1 seed=${2:-} have tail new i
    [ -n "$node" ] || die "top-up needs a node"

    have=$(pending_count) || return 1
    [ "$have" -ge "$CHAIN" ] && { say "chain already ${have} deep on ${node}, nothing to add"; return 0; }

    tail=$(chain_tail) || return 1
    [ -n "$tail" ] || tail=$seed
    [ -n "$tail" ] || die "no jobs in the chain to append to"

    for (( i = have; i < CHAIN; i++ )); do
        new=$(submit_link "$tail" "$node")
        case "$new" in
            ''|*[!0-9]*) say "submit refused after $((i - have)) added: ${new:-no output}"; return 1 ;;
        esac
        say "queued ${new} behind ${tail} on ${node}"
        tail=$new
    done
}

# --- pools ------------------------------------------------------------------
# A QOS owns a node quota shared by every account attached to it, so the nodes
# sinfo calls idle are usually spoken for by a quota you are not in. These read
# that landscape back out of the scheduler.

# "qos|priority|preempt|maxwall_min|maxjobspu|maxsubmitpu|nodecap|nodecap_pu".
# The table is column-aligned but the columns drift a character or two against
# their headings, so each value is matched to its nearest heading instead of a
# fixed offset -- blank cells would otherwise shift everything left.
qos_facts() {
    spur accounts show qos 2>/dev/null | awk '
        NR == 1 {
            s = $0
            for (i = 1; i <= NF; i++) {
                p = index(s, $i); off[i] = p - 1; head[i] = $i; cols = i
                s = substr(s, 1, p - 1) sprintf("%*s", length($i), "") substr(s, p + length($i))
            }
            next
        }
        $1 ~ /-qos$/ {
            split("", v); s = $0
            for (i = 1; i <= NF; i++) {
                p = index(s, $i) - 1; best = 1; bd = 9999
                for (j = 1; j <= cols; j++) {
                    d = off[j] - p; if (d < 0) d = -d
                    if (d < bd) { bd = d; best = j }
                }
                v[head[best]] = $i
                s = substr(s, 1, p) sprintf("%*s", length($i), "") substr(s, p + 1 + length($i))
            }
            # The first three columns are never blank, so take them by position
            # and leave the offset matching for the optional ones it exists for.
            # A name wider than the Name column pushes every value right, and
            # nearest-heading then lands them a column late: amd-hyperloom-geak-qos
            # is two characters longer than amd-brain-models-qos, enough to put its
            # PreemptMode under UsageFactor and leave the priority $2 had already
            # dropped in PreemptMode standing. That reads as a preemptible pool --
            # preempt_safe, pool_score and race_scan all trust this field -- so a
            # pool that is in fact off scored as one that can lose you the node.
            v["Name"] = $1
            if (head[2] == "Priority" && $2 ~ /^[0-9]+$/) v["Priority"] = $2
            if (head[3] == "PreemptMode" && $3 ~ /^[a-z]+$/) v["PreemptMode"] = $3
            cap = nodes(v["GrpTRES"]); cappu = nodes(v["MaxTRESPU"])
            printf "%s|%s|%s|%s|%s|%s|%s|%s\n", v["Name"], v["Priority"], v["PreemptMode"],
                   v["MaxWall"], v["MaxJobsPU"], v["MaxSubmitPU"], cap, cappu
        }
        function nodes(t) { return match(t, /node=[0-9]+/) ? substr(t, RSTART + 5, RLENGTH - 5) : "" }'
}

# "account|qos|default" per association. The QOS is the association default,
# which is the one a job actually lands in unless you pass -q yourself.
my_pools() {
    spur accounts show user "$ME" 2>/dev/null | awk -v me="$ME" '
        $1 == me && $NF ~ /qos/ { printf "%s|%s|%s\n", $2, $NF, ($4 == $2 ? "*" : "") }'
}

# "qos|running|pending" for this user's own jobs. Asked once and looked up per
# pool rather than once per pool: this used to be a squeue call inside the table
# loop, which is seven of them for seven pools, each able to return something
# different from the last while the table was still being printed.
my_usage() {
    squeue -u "$ME" -h -o "%q|%T" 2>/dev/null \
    | awk -F'|' '
        { seen[$1] }
        $2 == "RUNNING" { r[$1]++ }
        $2 == "PENDING" { p[$1]++ }
        END { for (q in seen) printf "%s|%d|%d\n", q, r[q], p[q] }'
}

# "qos|nodes_running|jobs_pending" across the whole cluster.
pool_usage() {
    { squeue -h -t RUNNING -o "R|%q|%D" 2>/dev/null
      squeue -h -t PENDING -o "P|%q|%D" 2>/dev/null; } \
    | awk -F'|' '
        $1 == "R" { run[$2] += $3; seen[$2] }
        $1 == "P" { wait[$2] += 1;  seen[$2] }
        END { for (q in seen) printf "%s|%d|%d\n", q, run[q], wait[q] }'
}

pick() { printf '%s\n' "$2" | awk -F'|' -v k="$1" '$1==k {print; exit}'; }

# The QOS an account's jobs land in unless one is named explicitly.
qos_of_account() { my_pools | awk -F'|' -v a="$1" '$1==a {print $2; exit}'; }

# "account|qos" for every pool you may submit to. my_pools gives one QOS per
# account -- the association default -- but an account often grants more:
# amd-hyperloom grants amd-burst-qos alongside its own, and a chain may go in
# either. The limit columns after Def QOS are usually blank and collapse away
# under awk, so the QOS fields are found by scanning in from the right rather
# than by position: the rightmost names the default, and the field before it,
# when it also names a QOS, is the full comma-separated list.
my_qos_pairs() {
    spur accounts show user "$ME" 2>/dev/null | awk -v me="$ME" '
        $1 == me {
            last = 0
            for (i = NF; i >= 3; i--) if ($i ~ /-qos/) { last = i; break }
            if (last == 0) next
            list = (last > 3 && $(last-1) ~ /-qos/) ? $(last-1) : $last
            n = split(list, q, ",")
            for (j = 1; j <= n; j++) printf "%s|%s\n", $2, q[j]
        }'
}

# What a pool is worth, as one number, so pools can be sorted. Priority
# dominates by a wide margin: 10000 against 100 decides whether you keep a node
# or lose it within the hour, and no other property comes close to that. Then
# surviving preemption at all, then the size of the node quota, on the grounds
# that a larger pool more often has something free.
pool_score() {
    local f=$1 prio pre cap score
    prio=$(fld 2 "$f"); pre=$(fld 3 "$f"); cap=$(fld 7 "$f")
    case ${prio:-} in ''|*[!0-9]*) prio=0 ;; esac
    case ${cap:-}  in ''|*[!0-9]*) cap=0  ;; esac
    [ "$cap" -gt 1000 ] && cap=1000
    score=$(( prio * 10000 + cap ))
    [ "$pre" = off ] && score=$(( score + 1000 ))
    printf '%d' "$score"
}

# The pool to use when the caller named none, best first.
best_pool() {
    local pairs facts acct qos f s best=-1 out=""
    pairs=$(my_qos_pairs); facts=$(qos_facts)
    [ -n "$pairs" ] || return 1
    while IFS='|' read -r acct qos; do
        [ -n "$acct" ] || continue
        f=$(pick "$qos" "$facts"); [ -n "$f" ] || continue
        s=$(pool_score "$f")
        [ "$s" -gt "$best" ] || continue
        best=$s; out="${acct}|${qos}"
    done <<< "$pairs"
    [ -n "$out" ] || return 1
    printf '%s' "$out"
}

# An account and a QOS are easy to confuse, and the confusion is the scheduler's
# fault as much as anyone's: pools are named and discussed by QOS, but jobs are
# submitted to an account, and the two rarely share a name. So take either here.
# Settles ACCOUNT and NODEHOLD_QOS between them, and is called by everything
# that submits. Idempotent -- once ACCOUNT is set it does nothing.
resolve_pool() {
    local pair granted

    # A QOS given where the account belongs: find an account that grants it.
    case ${ACCOUNT:-} in
        *-qos)
            NODEHOLD_QOS=${NODEHOLD_QOS:-$ACCOUNT}
            granted=$(my_qos_pairs | awk -F'|' -v q="$ACCOUNT" '$2==q {print $1; exit}')
            [ -n "$granted" ] ||
                die "no account of yours grants ${ACCOUNT} -- what you may use: ${SELF} pools"
            say "${ACCOUNT} is a QOS, not an account -- submitting to it through ${granted}"
            ACCOUNT=$granted
            ;;
    esac

    # A QOS on its own is enough, so long as one of your accounts grants it.
    if [ -z "${ACCOUNT:-}" ] && [ -n "${NODEHOLD_QOS:-}" ]; then
        ACCOUNT=$(my_qos_pairs | awk -F'|' -v q="$NODEHOLD_QOS" '$2==q {print $1; exit}')
        [ -n "$ACCOUNT" ] ||
            die "no account of yours grants ${NODEHOLD_QOS} -- what you may use: ${SELF} pools"
    fi

    if [ -z "${ACCOUNT:-}" ]; then
        pair=$(best_pool) ||
            die "cannot tell which accounts are yours -- is the scheduler answering? ${SELF} pools"
        ACCOUNT=${pair%%|*}; NODEHOLD_QOS=${NODEHOLD_QOS:-${pair#*|}}
        say "no pool named -- taking ${ACCOUNT}/${NODEHOLD_QOS}, the best one you hold"
    fi

    # An account that is not yours is refused by sbatch with a message about
    # associations that reads like a scheduler fault, one failed submission
    # later. Say it here instead, where there is room to say what to do about
    # it. Only when the scheduler answered, though: an outage returns no
    # associations at all, and that must not be read as a revocation.
    granted=$(my_qos_pairs)
    [ -n "$granted" ] || return 0
    printf '%s\n' "$granted" | grep -q "^${ACCOUNT}|" ||
        die "${ACCOUNT} is not one of your accounts -- what you may use: ${SELF} pools"
}

# `off` is never preempted and `requeue` puts the job back in the queue instead
# of destroying it, so both leave you with something. Only `cancel` actually
# loses the work, which is the distinction worth making when choosing a pool.
# These modes are changed by the admins from time to time -- amd-hyperloom-qos
# and amd-general-qos moved from off to requeue on 2026-08-28 -- so nothing here
# should assume a particular one.
preempt_safe() { case ${1:-} in off|requeue) return 0 ;; *) return 1 ;; esac; }

# Enforce priority independently of preemption mode. Failure to read policy is
# not authorization to submit into an unknown pool.
require_hi_prio() {
    [ "$ANY_QOS" = 1 ] && return 0
    local f prio min=${NODEHOLD_MIN_PRIO:-10000}
    case $min in ''|*[!0-9]*) die "NODEHOLD_MIN_PRIO must be an integer" ;; esac
    NODEHOLD_QOS=${NODEHOLD_QOS:-$(qos_of_account "$ACCOUNT")}
    f=$(pick "$NODEHOLD_QOS" "$(qos_facts_cached)")
    prio=$(fld 2 "$f")
    case $prio in ''|*[!0-9]*) die "cannot verify priority for ${NODEHOLD_QOS:-the default QoS}; refusing submission" ;; esac
    [ "$prio" -ge "$min" ] || die "${NODEHOLD_QOS} priority ${prio} is below required ${min}; use --any-qos only for an intentional exception"
}

# qos_facts shells out to spur, and submitting a chain asks for the same table
# once per link, so hold on to it briefly. Briefly and not for the life of the
# process: the waiter runs for days, and these limits are exactly what it must
# not go stale on.
QOS_FACTS_MEMO=""; QOS_FACTS_AT=0
qos_facts_cached() {
    local now; now=$(date +%s)
    if [ -z "$QOS_FACTS_MEMO" ] || [ $(( now - QOS_FACTS_AT )) -ge 300 ]; then
        QOS_FACTS_MEMO=$(qos_facts); QOS_FACTS_AT=$now
    fi
    printf '%s' "$QOS_FACTS_MEMO"
}

# How the pool this chain submits to treats a preemption: off, requeue or cancel.
my_preempt_mode() {
    fld 3 "$(pick "${NODEHOLD_QOS:-$(qos_of_account "$ACCOUNT")}" "$(qos_facts_cached)")"
}

# The partition caps a job on top of whatever its QOS allows, and it is the easy
# one to miss: amd-spur stops well short of some QOSes, which set no wall at all.
# Over-asking is not a rejection -- the job waits on PartitionTimeLimit forever.
partition_wall_min() {
    local t
    t=$(sinfo -h -p "$PARTITION" -o "%l" 2>/dev/null | head -1)
    case ${t:-} in ''|infinite|UNLIMITED|n/a|NONE) printf '0'; return ;; esac
    printf '%d' $(( $(time_to_seconds "$t") / 60 ))
}

# The smallest of what you asked for and every ceiling that applies, in seconds.
capped_seconds() {
    local want=$1 m out=$1
    shift
    for m in "$@"; do
        [ -n "$m" ] && [ "$m" -gt 0 ] && [ $(( m * 60 )) -lt "$out" ] && out=$(( m * 60 ))
    done
    printf '%d' "$out"
}

human_wall() {
    [ -n "${1:-}" ] || { printf 'none'; return; }
    local m=$1
    [ $(( m % 1440 )) -eq 0 ] && { printf '%dd' $(( m / 1440 )); return; }
    [ $(( m % 60 )) -eq 0 ]   && { printf '%dh' $(( m / 60 ));   return; }
    printf '%dm' "$m"
}

# --- payload (runs on the compute node) -------------------------------------

# Started here the tmux server belongs to the job's own step, so it outlives any
# `shell` attaching to it. Started from an attaching step it would die with that
# step, which is the whole thing we are avoiding.
start_session() {
    [ "$USE_TMUX" = 1 ] || return 0
    command -v tmux >/dev/null 2>&1 || { say "no tmux here; 'shell' falls back to plain bash"; return 0; }

    rm -f "$TMUX_SOCK"   # /tmp is node-wide and keeps sockets from earlier links
    if tmux -S "$TMUX_SOCK" new-session -d -s main -c "$HOME_DIR" 2>&1; then
        chmod 600 "$TMUX_SOCK" 2>/dev/null
        say "persistent session ready: ${SELF} shell"
    else
        say "could not start tmux; 'shell' falls back to plain bash"
    fi
}

# Run after each link takes the node, to put back whatever the last one was
# holding open. Worth being clear about what is and is not lost at a handover:
# the node does not change hands, and anything owned by the node rather than the
# job -- a docker container above all -- goes on running untouched. What dies is
# the job, and with it the tmux server this script starts, so every shell you had
# open inside it is gone even though the work behind them is not. The usual job
# of a hook is therefore to step back into something still running, not to build
# it again. It must be able to run twice with no ill effect, because it will.
#
# Failure here is logged and otherwise ignored: a hook that cannot run is not a
# reason to give up a node that took hours to get.
run_on_start() {
    local hook=${NODEHOLD_ON_START:-${STATE_DIR}/on_start} rc=0
    if [ "$EXCLUSIVE" != 1 ] && [ -z "${NODEHOLD_ON_START:-}" ]; then
        say "shared allocation: skipping default container hook; use a GPU-aware NODEHOLD_ON_START explicitly"
        return 0
    fi
    [ -n "$hook" ] && [ -x "$hook" ] || return 0

    say "on-start: ${hook}"
    NODEHOLD_GPUS=$GPUS NODEHOLD_EXCLUSIVE=$EXCLUSIVE NODEHOLD_NODE=$1 NODEHOLD_JOBID=$2 NODEHOLD_CHAIN_NAME=$JOB_NAME \
        "$hook" >> "${LOG_DIR}/${JOB_NAME}.on_start.log" 2>&1 || rc=$?
    if [ "$rc" = 0 ]
        then say "  done"
        else say "  exited ${rc}; see ${LOG_DIR}/${JOB_NAME}.on_start.log -- still holding"
    fi
    return 0
}

cmd_hold() {
    [ -n "${1:-}" ] && { use_chain "$1"; load_profile; }
    mkdir -p "$LOG_DIR"
    local node=${SLURM_JOB_NODELIST:-$(hostname)} jid=${SLURM_JOB_ID:-unknown}

    if [ "$EXPIRES_AT" != 0 ]; then
        local remaining=$(( EXPIRES_AT - $(date +%s) ))
        [ "$remaining" -gt 0 ] || return 0
        [ "$HOLD_SECONDS" -le "$remaining" ] || HOLD_SECONDS=$remaining
    fi
    say "=== ${JOB_NAME} ${jid} holding ${node} from $(date -Is) ==="
    printf 'chain=%s\njobid=%s\nnode=%s\nsince=%s\n' \
        "$JOB_NAME" "$jid" "$node" "$(date -Is)" > "$STATE" 2>/dev/null

    start_session
    run_on_start "$node" "$jid"

    # A link used to extend its own chain from here, but the compute image ships
    # no Slurm client any more, so top-up has to happen on the login node. Guard
    # rather than call it blindly: topup dies on failure and would drop the node.
    if command -v sbatch >/dev/null 2>&1; then
        ( mutation_lock; topup "$node" ) || say "top-up failed, still holding ${node}"
    else
        say "no Slurm client here -- extend the chain from the login node:"
        say "    ${SELF} topup"
    fi

    if [ "$EXPIRES_AT" != 0 ]; then
        local remaining=$(( EXPIRES_AT - $(date +%s) ))
        [ "$remaining" -gt 0 ] || return 0
        [ "$HOLD_SECONDS" -le "$remaining" ] || HOLD_SECONDS=$remaining
    fi
    say "sleeping ${HOLD_SECONDS}s, then handing ${node} to the next link"
    sleep "$HOLD_SECONDS"
    say "=== ${JOB_NAME} ${jid} releasing ${node} at $(date -Is) ==="
}

# --- commands ---------------------------------------------------------------

# A job asking for longer than its QOS or the partition allows is not refused,
# it just waits on that limit forever, so trim the ask rather than hang on it.
# Both ceilings move without notice and the QOS one is often unset, in which
# case the partition is the only thing standing between you and a job that never
# runs. Anything that submits a first link wants this.
trim_ask() {
    local want_s cap_s
    want_s=$(time_to_seconds "$TIME_LIMIT")
    cap_s=$(capped_seconds "$want_s" \
        "$(fld 4 "$(pick "${NODEHOLD_QOS:-$(qos_of_account "$ACCOUNT")}" "$(qos_facts)")")" \
        "$(partition_wall_min)")
    [ "$cap_s" -lt "$want_s" ] || return 0
    [ "$TIME_PINNED" = 1 ] &&
        say "${TIME_LIMIT} is more than ${ACCOUNT}/${PARTITION} allows -- asking for $(seconds_to_time "$cap_s") instead"
    TIME_LIMIT=$(seconds_to_time "$cap_s"); HOLD_SECONDS=$(( cap_s - 180 ))
    return 0
}

# Turn "hold it for four days" into a chain depth. Nothing on amd-spur may ask
# for more than a day of wall clock at a time, so duration here is depth, not
# --time. Sized so the QUEUED links alone cover the ask: the running head is
# already part-spent when you ask, and the case this has to survive is cron
# dying five minutes from now, where nothing tops the chain back up and only
# what is already queued still runs. Call it after trim_ask, which is what
# settles how long one link really gets.
depth_for_hold() {
    [ "${HOLD_FOR_S:-0}" -gt 0 ] || return 0
    local link_s
    link_s=$HOLD_SECONDS
    [ "${link_s:-0}" -gt 0 ] || return 0
    CHAIN=$(( (HOLD_FOR_S + link_s - 1) / link_s ))
    say "holding for $(seconds_to_time "$HOLD_FOR_S") at $(seconds_to_time "$link_s") a link:
         ${CHAIN} queued behind the holder, of potential runway, subject to scheduling and submit limits"
}

# A partial GPU ask and --exclusive contradict each other: exclusive spends a
# whole node off a QOS cap that counts nodes, to use a fraction of it. The
# default gives way rather than the choice, so asking for it explicitly still
# works.
# Ask for a fraction of the GPUs and the scheduler gives you its own default of
# a single CPU, which is how a node ends up held with 235 of its 236 CPUs idle
# and the holder unable to do anything with the GPUs it took. Take the matching
# fraction of the CPUs instead. The node size is read from the cluster rather
# than written down, so this stays right if the hardware ever changes, and an
# explicit -c always wins.
derive_cpus() {
    case ${CPUS:-} in ''|*[!0-9]*) die "CPU count must be a nonnegative integer (0 = automatic)" ;; esac
    CPUS=$((10#$CPUS))
    [ "$CPUS" = 0 ] || return 0
    [ "$GPUS" -gt 0 ] && [ "$GPUS" -lt 8 ] || return 0

    local n tot details
    n=${1:-${PIN_NODE:-}}
    if [ -z "$n" ]; then
        n=$(sinfo -h -N -p "$PARTITION" -o '%N' 2>/dev/null | awk 'NR==1 {first=$0} END {print first}')
    fi
    [ -n "$n" ] || die "cannot determine node CPU capacity; specify --cpus explicitly"
    details=$(spur show node "$n" 2>/dev/null) ||
        die "cannot query CPU capacity for ${n}; specify --cpus explicitly"
    tot=$(printf '%s\n' "$details" |
          awk 'match($0,/CPUTot=[0-9]+/){print substr($0,RSTART+7,RLENGTH-7); exit}')
    case $tot in ''|*[!0-9]*|0) die "unknown CPU capacity for ${n}; specify --cpus explicitly" ;; esac

    CPUS=$(( tot * GPUS / 8 ))
    [ "$CPUS" -gt 0 ] || CPUS=1
    say "taking ${CPUS} of ${tot} CPUs to match ${GPUS} of 8 GPUs (-c sets it yourself)"
}

reconcile_ask() {
    [ "$EXCL_SET" = 1 ] && return 0
    [ "${EXCLUSIVE:-0}" = 1 ] || return 0
    case ${GPUS:-} in ''|*[!0-9]*) return 0 ;; esac
    [ "$GPUS" -lt 8 ] || return 0
    EXCLUSIVE=0
    say "asking for ${GPUS} GPUs rather than the whole node, so not passing --exclusive"
    say "  (--exclusive takes the node anyway; it still costs one node off the QOS cap)"
}

cmd_start() {
    mkdir -p "$LOG_DIR"
    use_tag "$TAG"

    local existing
    existing=$(chain_rows) || die "cannot query scheduler; refusing to create a duplicate holder"
    if [ -n "$existing" ]; then
        say "chain '$(tag_of "$JOB_NAME")' already exists -- 'stop' it, or start another with -n <name>:"
        cmd_status
        return 1
    fi

    name_service_up ||
        die "this login node cannot resolve ${ME} at the moment, so sbatch will refuse
             every job and anything that does start gets cancelled seconds in. Your
             queued jobs are fine. Wait for it to come back, or use another login
             node, and run this again."

    resolve_pool
    require_hi_prio
    derive_cpus
    reconcile_ask
    trim_ask
    depth_for_hold
    if [ "${NODEHOLD_TEND:-1}" = 1 ] && [ "${IN_RACE:-0}" != 1 ]; then
        read_cron >/dev/null || die "cannot read cron; start aborted before submission (NODEHOLD_TEND=0 opts out)"
    fi
    [ "${EXPLICIT_START:-0}" != 1 ] || rm -f "${STATE_DIR}/${JOB_NAME}.released"

    local jid
    jid=$(submit_link "" "$PIN_NODE")
    case "$jid" in
        ''|*[!0-9]*) die "sbatch refused the holder: ${jid:-no output}" ;;
    esac

    save_profile
    ensure_race
    if [ "${NODEHOLD_TEND:-1}" = 1 ] && [ "${IN_RACE:-0}" != 1 ]; then tend_now; fi
    say "holder ${jid} submitted as chain '$(tag_of "$JOB_NAME")': 1 node, ${GPUS} GPUs, ${TIME_LIMIT}$([ "$EXCLUSIVE" = 1 ] && echo ', exclusive'), ${ACCOUNT}${NODEHOLD_QOS:+/${NODEHOLD_QOS}}${PIN_NODE:+, pinned to ${PIN_NODE}}"
    [ -n "$PIN_NODE" ] &&
        say "it waits for ${PIN_NODE} specifically, so it stays queued while that node is busy"
    say "nothing needs to stay running here -- check back with: ${SELF} status"

    # The successors are the point of the thing -- a holder on its own is just a
    # job that ends in a day -- and they cannot be pinned until it has a node.
    # So arrange for that here rather than leave it as a step to remember, which
    # is how chains used to end up one job deep and lose their node at handover.
    local i
    if [ "${NODEHOLD_ARM:-1}" != 1 ]; then
        say "not arming it (NODEHOLD_ARM=0) -- pin ${CHAIN} successors with: ${SELF} topup"
    # Per chain, not "is there any tick line at all": every cron line carries the
    # chain it tends in NODEHOLD_NAME, so a crontab full of lines for other
    # chains says nothing about this one. tick_in_cron answers the looser
    # question and answering it here claimed a brand new chain was being tended,
    # skipped arming it, and left it with nothing to pin its successors.
    elif [ "${IN_RACE:-0}" = 1 ] || tended_by_cron "$JOB_NAME"; then
        say "cron is tending this: ${CHAIN} successors go in within 5 minutes of it landing"
    else
        # squeue does not always admit a job the instant sbatch returns, and arm
        # refuses to arm a chain it cannot see.
        for (( i = 0; i < 10; i++ )); do [ -n "$(chain_rows)" ] && break; sleep 1; done
        if [ -n "$(chain_rows)" ]; then
            cmd_arm
        else
            say "the queue has not caught up yet -- arm it in a moment with: ${SELF} arm"
        fi
    fi
}

# Queue the chain behind a job you already hold, keeping its node, and leave
# that job alone.
cmd_adopt() {
    local jid=${1:-} node
    [ -n "$jid" ] || die "usage: ${SELF} [-n <name>] adopt <jobid>"
    use_tag "$TAG"
    local existing
    existing=$(chain_rows) || die "cannot query scheduler; adoption aborted"
    [ -z "$existing" ] || die "chain '$(tag_of "$JOB_NAME")' already exists -- 'stop' it first, or use -n <name>"

    node=$(squeue -u "$ME" -h -o "%i|%T|%N" 2>/dev/null \
           | awk -F'|' -v j="$jid" '$1==j && $2=="RUNNING" {print $3}')
    [ -n "$node" ] || die "job ${jid} is not a running job of yours"

    mkdir -p "$LOG_DIR"

    # The successors need only the job's id and its node, so they can go in
    # under a different account and QOS from the job they are following. That is
    # what keeps a node whose account has been taken away: on 2026-09-02
    # amd-primus was revoked from this user mid-hold, and the node survived it
    # by being adopted onto a pool the user still had.
    resolve_pool
    derive_cpus "$node"
    reconcile_ask
    trim_ask
    depth_for_hold

    require_hi_prio
    if [ "${NODEHOLD_TEND:-1}" = 1 ]; then
        read_cron >/dev/null || die "cannot read cron; adopt aborted before submission"
    fi
    [ ! -f "${STATE_DIR}/${JOB_NAME}.released" ] || die "chain is released; explicitly start a new chain name"
    save_profile
    ensure_race
    if [ "${NODEHOLD_TEND:-1}" = 1 ] && [ "${IN_RACE:-0}" != 1 ]; then tend_now; fi
    say "queueing ${CHAIN} successors behind job ${jid} on ${node} as chain '$(tag_of "$JOB_NAME")', ${ACCOUNT}${NODEHOLD_QOS:+/${NODEHOLD_QOS}}, ${TIME_LIMIT}"
    topup "$node" "$jid"
}

POOL_ROW='  %-19s %-23s %6s %-8s %6s %5s %5s %8s %8s\n'
cmd_pools() {
    local facts usage mine defs all acct qos def f u m prio pre wall cap cappu used wait best
    local mrun mpend trun=0 tpend=0 tnodes=0

    # Every pool you may submit to, not one per account: an account can grant
    # several, and a chain may go in any of them. my_pools reports only the
    # association default, which used to hide amd-burst-qos under amd-hyperloom.
    facts=$(qos_facts); usage=$(pool_usage); mine=$(my_qos_pairs); defs=$(my_pools)
    local selfuse; selfuse=$(my_usage)
    [ -n "$mine" ] || die "no associations found for ${ME}"

    all=$(spur accounts show account 2>/dev/null | tail -n +3 | grep -c . || true)
    say "partitions: $(sinfo -h -o '%P|%a|%l' 2>/dev/null | sort -u | awk -F'|' '
        { sub(/\*$/, "", $1); printf "%s (%s, %s) ", $1, $2, ($3 == "infinite" ? "no time limit" : "max " $3) }')"
    say "accounts:   $(printf '%s\n' "$mine" | cut -d'|' -f1 | sort -u | grep -c .) of ${all:-?} on the cluster are yours, $(printf '%s\n' "$mine" | grep -c .) pools between them"
    say ""
    # shellcheck disable=SC2059
    printf "$POOL_ROW" ACCOUNT QOS PRIO PREEMPT CAP USED QUEUED YOU-RUN YOU-WAIT

    while IFS='|' read -r acct qos; do
        [ -n "$acct" ] || continue
        def=$(printf '%s\n' "$defs" | awk -F'|' -v a="$acct" '$1==a {print $3; exit}')
        f=$(pick "$qos" "$facts"); u=$(pick "$qos" "$usage"); m=$(pick "$qos" "$selfuse")
        prio=$(fld 2 "$f"); pre=$(fld 3 "$f"); wall=$(fld 4 "$f"); cap=$(fld 7 "$f")
        cappu=$(fld 8 "$f"); used=$(fld 2 "$u"); wait=$(fld 3 "$u")
        mrun=$(fld 2 "$m"); mpend=$(fld 3 "$m")
        mrun=${mrun:-0}; mpend=${mpend:-0}


        # shellcheck disable=SC2059
        printf "$POOL_ROW" \
               "${acct}${def}" "$qos" "${prio:--}" \
               "$([ "$pre" = off ] && echo no || echo "${pre:-?}")" \
               "${cap:--}" "${used:-0}" "${wait:-0}" \
               "$([ "$mrun" = 0 ] && echo '-' || echo "$mrun")" \
               "$([ "$mpend" = 0 ] && echo '-' || echo "$mpend")"

        say "  limits: MaxJobsPU=$(fld 5 "$f"), MaxSubmitPU=$(fld 6 "$f") (blank = unspecified)"
        [ "$(human_wall "$wall")" = none ] ||
            say "  $(printf '%-19s' '') a job here may not exceed $(human_wall "$wall")"
        [ -n "$cappu" ] && [ -n "$cap" ] && [ "$cappu" != "$cap" ] &&
            say "  $(printf '%-19s' '') and no more than ${cappu} of those ${cap} nodes at once"
    done <<< "$mine"

    # What is actually held, as opposed to how many jobs are in flight: a chain
    # is one running job and several queued behind it, so counting jobs reads as
    # far more than you have. The nodes are the thing there is a quota on.
    read -r trun tpend <<< "$(printf '%s\n' "$selfuse" | awk -F'|' '{r+=$2;p+=$3} END {print r+0,p+0}')"
    tnodes=$(squeue -u "$ME" -h -t RUNNING -o '%D' 2>/dev/null | awk '{n+=$1} END {print n+0}')
    say ""
    say "  you: ${tnodes:-0} allocated node-slot(s) (shared nodes may repeat), ${trun} running job(s), ${tpend} queued"
    [ "$tpend" -gt 0 ] &&
        say "       queued jobs are mostly chain successors waiting their turn -- '${SELF} status' breaks them down by chain"

    say ""
    best=$(best_pool 2>/dev/null || true)
    [ -n "$best" ] &&
        say "  Naming no pool gets you ${best%%|*}/${best#*|}, the best of these:"$'\n'"  highest priority first, then surviving preemption, then the larger quota."
    say "  * = your default account. CAP/USED/QUEUED are the WHOLE pool, shared"
    say "  with every account on that QOS -- which is why idle-looking nodes are"
    say "  often unreachable, and why USED = CAP means you queue on QOSGrpNodeLimit."
    say "  PREEMPT describes policy, not a reservation or guarantee against cancellation."
}

# Stable machine-readable companion to `pools`. Keep the human table above for
# terminals; consumers should use this command instead of scraping its spacing
# and narrative lines.
cmd_pools_json() {
    local facts usage mine defs selfuse all owned trun tpend tnodes best
    local acct qos assoc def f u m prio pre wall cap cappu used wait mrun mpend
    local first=1
    facts=$(qos_facts); usage=$(pool_usage); mine=$(my_qos_pairs)
    defs=$(my_pools); selfuse=$(my_usage)
    [ -n "$mine" ] || die "no associations found for ${ME}"
    all=$(spur accounts show account 2>/dev/null | tail -n +3 | grep -c . || true)
    owned=$(printf '%s\n' "$mine" | cut -d'|' -f1 | sort -u | grep -c . || true)
    read -r trun tpend <<< "$(printf '%s\n' "$selfuse" | awk -F'|' '{r+=$2;p+=$3} END {print r+0,p+0}')"
    tnodes=$(squeue -u "$ME" -h -t RUNNING -o '%D' 2>/dev/null | awk '{n+=$1} END {print n+0}')
    best=$(best_pool 2>/dev/null || true)

    printf '{"schemaVersion":1,"user":'; json_quote "$ME"
    printf ',"partitions":['
    while IFS='|' read -r part avail maxwall; do
        [ -n "$part" ] || continue
        part=${part%\*}
        [ "$first" = 1 ] || printf ','
        first=0
        printf '{"name":'; json_quote "$part"
        printf ',"availability":'; json_quote "$avail"
        printf ',"maxWall":'; json_quote "$maxwall"
        printf '}'
    done < <(sinfo -h -o '%P|%a|%l' 2>/dev/null | sort -u)
    printf '],"accountsOwned":'; json_number_or_null "$owned"
    printf ',"accountsTotal":'; json_number_or_null "$all"
    printf ',"summary":{"allocatedNodeSlots":'; json_number_or_null "${tnodes:-0}"
    printf ',"runningJobs":'; json_number_or_null "${trun:-0}"
    printf ',"pendingJobs":'; json_number_or_null "${tpend:-0}"
    printf '},"bestPool":'
    if [ -n "$best" ]; then
        printf '{"account":'; json_quote "${best%%|*}"
        printf ',"qos":'; json_quote "${best#*|}"
        printf '}'
    else
        printf 'null'
    fi

    printf ',"pools":['; first=1
    while IFS='|' read -r acct qos; do
        [ -n "$acct" ] || continue
        assoc=$(printf '%s\n' "$defs" | awk -F'|' -v a="$acct" '$1==a {print; exit}')
        def=$(fld 2 "$assoc")
        f=$(pick "$qos" "$facts"); u=$(pick "$qos" "$usage"); m=$(pick "$qos" "$selfuse")
        prio=$(fld 2 "$f"); pre=$(fld 3 "$f"); wall=$(fld 4 "$f")
        cap=$(fld 7 "$f"); cappu=$(fld 8 "$f")
        used=$(fld 2 "$u"); wait=$(fld 3 "$u")
        mrun=$(fld 2 "$m"); mpend=$(fld 3 "$m")
        [ "$first" = 1 ] || printf ','
        first=0
        printf '{"account":'; json_quote "$acct"
        printf ',"qos":'; json_quote "$qos"
        printf ',"defaultQos":'; [ "$qos" = "$def" ] && printf true || printf false
        printf ',"priority":'; json_number_or_null "$prio"
        printf ',"preemptMode":'; json_quote "$pre"
        printf ',"preemptSafe":'; preempt_safe "$pre" && printf true || printf false
        printf ',"maxWallMinutes":'; json_number_or_null "$wall"
        printf ',"maxJobsPerUser":'; json_number_or_null "$(fld 5 "$f")"
        printf ',"maxSubmitPerUser":'; json_number_or_null "$(fld 6 "$f")"
        printf ',"nodeCap":'; json_number_or_null "$cap"
        printf ',"nodeCapPerUser":'; json_number_or_null "$cappu"
        printf ',"usedNodes":'; json_number_or_null "${used:-0}"
        printf ',"queuedJobs":'; json_number_or_null "${wait:-0}"
        printf ',"userRunning":'; json_number_or_null "${mrun:-0}"
        printf ',"userPending":'; json_number_or_null "${mpend:-0}"
        printf '}'
    done <<< "$mine"
    printf ']}\n'
}

show_one() {
    local rows run node
    load_profile
    rows=$(chain_rows) || die "cannot query scheduler"
    say "chain '$(tag_of "$JOB_NAME")'"
    say "  pool=${ACCOUNT}/${NODEHOLD_QOS:-default}, GPUs=${GPUS}, queued target=${CHAIN}, per-link wall=${TIME_LIMIT}"
    [ "$EXPIRES_AT" = 0 ] || say "  deadline: $(date -u -d "@$EXPIRES_AT" '+%Y-%m-%d %H:%M:%S UTC') (cron cleanup on next tick)"

    if [ -z "$rows" ]; then
        say "  no jobs in the queue"
        [ -f "$STATE" ] && { say "  last known state:"; sed 's/^/    /' "$STATE"; }
        return 0
    fi

    printf '  %-8s %-9s %-20s %s\n' JOBID STATE NODE REASON
    printf '%s\n' "$rows" | awk -F'|' '{printf "  %-8s %-9s %-20s %s\n", $1, $2, ($3==""?"-":$3), ($4==""?"-":$4)}'

    run=$(running_row)
    if [ -z "$run" ]; then
        say "  nothing running yet -- still waiting for a node"
        return 0
    fi
    node=$(fld 3 "$run")
    say "  holding ${node}; $(pending_count) queued + running holder; potential queued sleep=$(seconds_to_time "$(( $(pending_count) * HOLD_SECONDS ))") (not guaranteed)"
    say "  node state: $(sinfo -h -n "$node" -o '%t %E' 2>/dev/null | head -1)"
}

cmd_status() {
    local names n
    [ -n "$TAG" ] && { use_tag "$TAG"; show_one; return 0; }

    names=$(all_chains)
    [ -n "$names" ] || { say "no chains in the queue"; return 0; }

    while read -r n; do
        [ -n "$n" ] || continue
        use_chain "$n"
        show_one
        say ""
    done <<< "$names"
}

show_one_json() {
    local rows run node nodeinfo pending held backoff last_job last_node last_since first
    load_profile
    rows=$(chain_rows) || die "cannot query scheduler"
    run=$(printf '%s\n' "$rows" | awk -F'|' '$2=="RUNNING" {print; exit}')
    node=$(fld 3 "$run")
    pending=$(printf '%s\n' "$rows" | awk -F'|' '$2=="PENDING" && $4 !~ /^Job(Hold|Held)/ {n++} END {print n+0}')
    held=$(printf '%s\n' "$rows" | awk -F'|' '$4 ~ /^Job(Hold|Held)/ {n++} END {print n+0}')
    backoff=$(backoff_until)
    last_job=$(awk -F= '$1=="jobid" {print $2; exit}' "$STATE" 2>/dev/null)
    last_node=$(awk -F= '$1=="node" {print $2; exit}' "$STATE" 2>/dev/null)
    last_since=$(awk -F= '$1=="since" {sub(/^[^=]*=/,""); print; exit}' "$STATE" 2>/dev/null)
    nodeinfo=""
    [ -z "$node" ] || nodeinfo=$(sinfo -h -n "$node" -o '%t|%E' 2>/dev/null | head -1)

    printf '{"name":'; json_quote "$JOB_NAME"
    printf ',"tag":'; json_quote "$(tag_of "$JOB_NAME")"
    printf ',"account":'; json_quote "${ACCOUNT:-}"
    printf ',"qos":'; json_quote "${NODEHOLD_QOS:-}"
    printf ',"gpus":'; json_number_or_null "${GPUS:-}"
    printf ',"cpus":'; json_number_or_null "${CPUS:-}"
    printf ',"exclusive":'; json_bool "${EXCLUSIVE:-0}"
    printf ',"timeLimit":'; json_quote "${TIME_LIMIT:-}"
    printf ',"chainTarget":'; json_number_or_null "${CHAIN:-}"
    printf ',"holdSeconds":'; json_number_or_null "${HOLD_SECONDS:-}"
    printf ',"pinNode":'; [ -n "${PIN_NODE:-}" ] && json_quote "$PIN_NODE" || printf null
    printf ',"pair":'; [ -n "${PAIR:-}" ] && json_quote "$PAIR" || printf null
    printf ',"repin":'; json_bool "${REPIN:-1}"
    printf ',"anyQos":'; json_bool "${ANY_QOS:-0}"
    printf ',"expiresAt":'
    [ "${EXPIRES_AT:-0}" -gt 0 ] 2>/dev/null && json_number_or_null "$EXPIRES_AT" || printf null
    printf ',"tended":'; tended_by_cron "$JOB_NAME" 2>/dev/null && printf true || printf false
    printf ',"released":'; [ -f "${STATE_DIR}/${JOB_NAME}.released" ] && printf true || printf false
    printf ',"backoffUntil":'
    [ "${backoff:-0}" -gt 0 ] 2>/dev/null && json_number_or_null "$backoff" || printf null
    printf ',"runningNode":'; [ -n "$node" ] && json_quote "$node" || printf null
    printf ',"pendingCount":'; json_number_or_null "$pending"
    printf ',"heldCount":'; json_number_or_null "$held"
    printf ',"potentialQueuedSeconds":'; json_number_or_null "$(( pending * ${HOLD_SECONDS:-0} ))"
    printf ',"nodeState":'; [ -n "$nodeinfo" ] && json_quote "${nodeinfo%%|*}" || printf null
    printf ',"nodeReason":'
    [ -n "$nodeinfo" ] && [ "${nodeinfo#*|}" != "$nodeinfo" ] &&
        json_quote "${nodeinfo#*|}" || printf null
    printf ',"lastKnown":'
    if [ -n "$last_job$last_node$last_since" ]; then
        printf '{"jobId":'; [ -n "$last_job" ] && json_quote "$last_job" || printf null
        printf ',"node":'; [ -n "$last_node" ] && json_quote "$last_node" || printf null
        printf ',"since":'; [ -n "$last_since" ] && json_quote "$last_since" || printf null
        printf '}'
    else
        printf 'null'
    fi
    printf ',"jobs":['; first=1
    while IFS='|' read -r jid state jnode reason; do
        [ -n "$jid" ] || continue
        [ "$first" = 1 ] || printf ','
        first=0
        printf '{"id":'; json_quote "$jid"
        printf ',"state":'; json_quote "$state"
        printf ',"node":'; [ -n "$jnode" ] && json_quote "$jnode" || printf null
        printf ',"reason":'; [ -n "$reason" ] && json_quote "$reason" || printf null
        printf '}'
    done <<< "$rows"
    printf ']}'
}

cmd_status_json() {
    local names n first=1
    if [ -n "$TAG" ]; then
        use_tag "$TAG"; names=$JOB_NAME
    else
        names=$(all_chains)
    fi
    printf '{"schemaVersion":1,"user":'; json_quote "$ME"
    printf ',"prefix":'; json_quote "$PREFIX"
    printf ',"chains":['
    while read -r n; do
        [ -n "$n" ] || continue
        [ "$first" = 1 ] || printf ','
        first=0
        use_chain "$n"
        show_one_json
    done <<< "$names"
    printf ']}\n'
}

# Resolve the chain and leave its running job in $RUN_ROW. Sets globals rather
# than echoing, so the chain it picked (and any die) survives into the caller.
RUN_ROW=""
need_running() {
    resolve_chain
    RUN_ROW=$(running_row)
    [ -n "$RUN_ROW" ] || die "chain '$(tag_of "$JOB_NAME")' has nothing running -- try '${SELF} status'"
}

cmd_topup() {
    need_running
    trim_ask
    depth_for_hold
    save_profile
    topup "$(fld 3 "$RUN_ROW")"
}

# True when cron is already running `tick`, which does everything the background
# waiter does without being a long-lived process on a login node.
tick_in_cron() {
    crontab -l 2>/dev/null | grep -qE '^[^#].*node_holder[^|]*[[:space:]]tick([[:space:]]|$)'
}

# Queue a holder in every account you have and keep whichever lands first: the
# pools are capped separately, so a node refused by one may be free in another.
# Chains you already have are adopted rather than resubmitted, so nothing gives
# up its place in the queue.
cmd_race() {
    local IN_RACE=1 pairs acct qos chain f old_tag=$TAG
    local race=${STATE_DIR}/${PREFIX}.race
    [ ! -s "$race" ] || die "race already configured; use tick or release its chains first"
    pairs=$(my_qos_pairs)
    [ -n "$pairs" ] || die "no QoS associations available"
    mkdir -p "$LOG_DIR"
    while IFS='|' read -r acct qos; do
        [ -n "$acct" ] || continue
        ACCOUNT=$acct; NODEHOLD_QOS=$qos
        ( require_hi_prio ) || continue
        TAG="race-${acct}-${qos}"
        use_tag "$TAG"; chain=$JOB_NAME
        [ ! -f "${STATE_DIR}/${chain}.released" ] || continue
        [ -z "$(chain_rows)" ] || die "${chain} already exists; manage it explicitly"
        cmd_start || return 1
        # Aggregate tick owns these contenders; no independent per-chain cron.
        printf '%s|%s|%s|%s|%s\n' "$chain" "$acct" "$qos" "$(my_preempt_mode)" \
            "$(time_to_seconds "$TIME_LIMIT")" >> "$race" || die "cannot save race"
    done <<< "$pairs"
    [ -s "$race" ] || die "no eligible high-priority pools to race"
    TAG=$old_tag; use_chain "$PREFIX"
    tend_now
    say "race configured; cron will keep the winner and retire other contenders"
}

# Best runner in the race file: never being preempted beats any wall clock, and
# past that it is simply whoever can hold the node for longer.
RACE_BEST=""; RACE_ACCT=""; RACE_JID=""; RACE_NODE=""; RACE_PRE=""
race_scan() {
    local chain acct qos preempt wall run score best=-1 facts prio
    RACE_BEST=""; RACE_ACCT=""; RACE_JID=""; RACE_NODE=""; RACE_PRE=""
    facts=$(qos_facts_cached)

    while IFS='|' read -r chain acct qos preempt wall; do
        [ -n "$chain" ] || continue
        [ ! -f "${STATE_DIR}/${chain}.released" ] || continue
        use_chain "$chain"
        run=$(running_row)
        [ -n "$run" ] || continue

        # Priority belongs in here. Until 2026-09-02 this weighed only preempt
        # mode and wall clock, which separated the pools while some were `off`
        # or `requeue` and their walls differed. Once every pool became `cancel`
        # on the same one-day wall they all scored identically, and "keep the
        # best node" quietly degraded into "keep whichever the race file listed
        # first" -- trading a hyperloom node at priority 10000 for a burst one
        # at 100 on a coin toss. Read priority live rather than from the race
        # file, which records only what was true when the race was set up.
        prio=$(fld 2 "$(pick "$qos" "$facts")")
        case ${prio:-} in ''|*[!0-9]*) prio=0 ;; esac

        score=$(( prio * 10000000 + wall ))
        preempt_safe "$preempt" && score=$(( score + 1000000000000 ))
        [ "$score" -gt "$best" ] || continue
        best=$score; RACE_BEST=$chain; RACE_ACCT=$acct; RACE_PRE=$preempt
        RACE_JID=$(fld 1 "$run"); RACE_NODE=$(fld 3 "$run")
    done < "$1"

    [ -n "$RACE_BEST" ]
}

# The -n form of a chain name, so a chain can be handed back to `start`.
tag_arg() { [ "$1" = "$PREFIX" ] && printf '' || printf '%s' "${1#"${PREFIX}"-}"; }

# Whether $1 could still take this chain's next link. Free CPUs are the test
# because that is the one number reported for both halves of a shared node, and
# an exclusive chain needs the node untouched rather than merely roomy. State
# comes first: a drained or downed node has all its CPUs free by this measure
# and cannot run a thing.
node_has_room() {
    local n=$1 details tot alloc want
    details=$(spur show node "$n" 2>/dev/null) || return 2
    case " $(printf '%s' "$details" | awk 'match($0,/State=[^ ]+/){print substr($0,RSTART+6,RLENGTH-6); exit}') " in
        *DRAIN*|*DOWN*|*FAIL*|*INVAL*|*MAINT*|*POWER*|*RESERVED*|*NOT_RESPONDING*) return 1 ;;
    esac
    tot=$(printf '%s' "$details" | awk 'match($0,/CPUTot=[0-9]+/){print substr($0,RSTART+7,RLENGTH-7); exit}')
    alloc=$(printf '%s' "$details" | awk 'match($0,/CPUAlloc=[0-9]+/){print substr($0,RSTART+9,RLENGTH-9); exit}')
    case ${tot:-x}-${alloc:-x} in *[!0-9-]*) return 2 ;; esac
    [ "$tot" -gt 0 ] && [ "$alloc" -le "$tot" ] || return 2
    if [ "$EXCLUSIVE" = 1 ]; then
        [ "$alloc" = 0 ]
        return
    fi
    want=${CPUS:-0}; [ "$want" -gt 0 ] 2>/dev/null || want=1
    [ $(( tot - alloc )) -ge "$want" ]
}

# Where a chain whose pin has died should go instead. The partner's node comes
# first so that a pair which was dividing one node goes on dividing one. Failing
# that it takes a node nothing is on at all, lowest name first -- both halves
# tick from the same cron minute with no way to ask each other what they chose,
# and sorted order is an answer each arrives at alone. Multi-node entries are
# skipped because a compressed nodelist is not a node this can pin to.
REPIN_TO=""
repin_target() {
    local n
    if [ -n "$PAIR" ]; then
        n=$(squeue -u "$ME" -h -t RUNNING -o '%j|%N' 2>/dev/null |
            awk -F'|' -v j="$PAIR" '$1==j && $2 !~ /[],[]/ {print $2; exit}')
        if [ -n "$n" ] && [ "$n" != "$REFLOAT_NODE" ] && node_has_room "$n"; then
            printf '%s' "$n"; return 0
        fi
    fi
    for n in $(sinfo -h -N -p "$PARTITION" -t idle -o '%N' 2>/dev/null | sort -u); do
        [ "$n" = "$REFLOAT_NODE" ] && continue
        node_has_room "$n" && { printf '%s' "$n"; return 0; }
    done
    return 1
}

# Successors carry -w <node>, which is what holds the node across a handover. If
# the holder stopped for any other reason that pin becomes a liability: the links
# are queued for one specific node and cannot take any other, however free the
# pool gets. True when the pins have gone stale and the chain should be refloated.
REFLOAT_NODE=""; REFLOAT_WHY=""
refloat_needed() {
    local first now room
    REFLOAT_NODE=$(awk -F= '$1=="node" {print $2}' "$STATE" 2>/dev/null)
    REFLOAT_WHY=""; REPIN_TO=""

    if [ -n "$(running_row)" ] || [ -z "$(chain_rows)" ] || [ -z "$REFLOAT_NODE" ]; then
        rm -f "$LOST"; return 1
    fi

    # An explicit -w asks to wait for one node however long that takes, so a pin
    # onto a node somebody else is on is normally none of our business. Losing a
    # node we already had is the exception: the links behind it are queued for a
    # node that is gone, and on a preemptible pool the whole hold gets spent
    # waiting -- node 196 was taken on 2026-09-10 and its two halves would have
    # sat there for the owner's full day. Only the node this chain actually ran
    # on counts, which is what separates "it was taken from me" from "I am still
    # queueing for the node I asked for". Room rather than occupancy decides it,
    # so a half that lost its own link goes back beside a partner still holding.
    if [ -n "$PIN_NODE" ]; then
        [ "$REPIN" = 1 ] || return 1
        [ "$PIN_NODE" = "$REFLOAT_NODE" ] || return 1
        node_has_room "$PIN_NODE"; room=$?
        [ "$room" != 0 ] || { rm -f "$LOST"; return 1; }
        [ "$room" = 1 ] || { say "cannot verify capacity on ${PIN_NODE}; leaving queued jobs unchanged"; return 1; }
        if ! REPIN_TO=$(repin_target); then
            say "$(date -Is) pinned to ${PIN_NODE}, which has no room left, and no other node is free -- still waiting"
            return 1
        fi
        REFLOAT_WHY="has no room for this link any more"
        rm -f "$LOST"; return 0
    fi

    # Somebody else is on it, so the pin cannot come good however long we wait.
    if squeue -h -t RUNNING -o '%N' 2>/dev/null | grep -qFx "$REFLOAT_NODE"; then
        REFLOAT_WHY="it belongs to another job now"; return 0
    fi

    # Nobody on it is ambiguous: either a successor is seconds from starting, or
    # the node has been drained or taken down and the pin is dead. Both look the
    # same from here, so wait out a handover before writing it off. Checking the
    # node's state instead would miss every other reason a pin never comes good.
    now=$(date +%s); first=$(cat "$LOST" 2>/dev/null || true)
    case ${first:-} in
        ''|*[!0-9]*) printf '%s' "$now" > "$LOST" 2>/dev/null; return 1 ;;
    esac
    [ $(( now - first )) -ge "${NODEHOLD_REFLOAT_AFTER:-300}" ] || return 1

    REFLOAT_WHY="nothing has started on it in $(( (now - first) / 60 ))m"
    return 0
}

# The race file records each pool's preempt mode and wall clock as they stood
# when the race was set up, and both move under you: amd-hyperloom-qos went off
# -> requeue -> cancel over three days, and the partition halved from 3 days to
# 1. A stale `off` in here is not a cosmetic problem -- on 2026-08-31 it talked a
# tick into throwing away a held node with three links queued behind it for a
# pool that cancelled the replacement 90 seconds later. Re-read from the
# scheduler before anything is decided on it.
refresh_race() {
    local f=$1 tmp=$1.new chain acct qos preempt wall facts qf part
    facts=$(qos_facts_cached); [ -n "$facts" ] || return 0
    part=$(partition_wall_min)
    : > "$tmp" 2>/dev/null || return 0
    while IFS='|' read -r chain acct qos preempt wall; do
        [ -n "$chain" ] || continue
        [ ! -f "${STATE_DIR}/${chain}.released" ] || continue
        qf=$(pick "$qos" "$facts")
        if [ -n "$qf" ]; then
            preempt=$(fld 3 "$qf")
            wall=$(capped_seconds "$wall" "$(fld 4 "$qf")" "$part")
        fi
        printf '%s|%s|%s|%s|%s\n' "$chain" "$acct" "$qos" "$preempt" "$wall" >> "$tmp"
    done < "$f"
    if [ -s "$tmp" ]; then mv -f "$tmp" "$f"; else rm -f "$tmp"; fi
}

# Winning the race is not the same as being finished with it. Deleting the file
# here stops `tick` dead: it has nothing left to top up, refloat or resubmit, so
# the first time the winner loses its node nothing notices. That is exactly how
# 90 minutes went by with no jobs at all. Keep the winner on file instead -- the
# race is over because it is the only pool left in it, not because the file is.
keep_only() {
    local f=$1 chain=$2
    if grep -F "${chain}|" "$f" > "${f}.new" 2>/dev/null && [ -s "${f}.new" ]
        then mv -f "${f}.new" "$f"
        else rm -f "${f}.new"
    fi
}

# One pass of the race, worked out from the queue rather than from anything held
# in memory, so that it can run from cron. Worth preferring to the waiter: this
# login node SIGKILLs long-lived background processes without warning, and a cron
# line that runs every few minutes is worth more than a waiter that has been
# killed by the time the node finally comes free.
cmd_tick() {
    local IN_RACE=1
    local race=${STATE_DIR}/${PREFIX}.race
    local chain acct qos preempt wall out pid
    [ -f "$race" ] || die "no pools set up to tend -- run '${SELF} race' once first"

    # Ask whether the filesystem will take a write before asking who holds the
    # lock, because a full one presents as the other. `exec 9>` against a home
    # that is over quota leaves fd 9 closed, bash carries on regardless, and the
    # flock below then fails with EBADF -- which is indistinguishable from a
    # rival tender. On 2026-09-01 that sent the morning hunting a phantom process
    # on some other login node when nothing anywhere could write a byte.
    if ! home_writable; then
        alarm "${STATE_DIR} will not take a write -- over quota? nothing can be tended until it can"
        return 0
    fi
    home_headroom_ok ||
        alarm "under ${NODEHOLD_HEADROOM_MB:-64}M of room left in ${HOME_DIR} -- clear space before the quota stops all of this"

    # One tender at a time: two of these would each see an empty chain and each
    # put a holder in for it. A live waiter counts, a SIGKILLed one does not.
    # Both refusals are logged, and logged with the host: returning quietly here
    # is what made the 2026-08-31 outage look like cron simply being dead.
    local racelock="${LOCK_DIR}/${PREFIX}.race.lock"
    if ! exec 9> "$racelock"; then
        alarm "cannot open ${racelock} -- standing down"
        return 0
    fi
    if ! flock -n 9 2>/dev/null; then
        alarm "another tender holds ${racelock} -- standing down"
        return 0
    fi
    pid=$(cat "${race}.pid" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        alarm "a waiter (pid ${pid}) is minding this -- standing down"
        return 0
    fi

    scheduler_up    || { say "$(date -Is) scheduler not answering; leaving well alone"; return 0; }
    name_service_up || { say "$(date -Is) this login node cannot resolve ${ME}; leaving well alone"; return 0; }
    # Expiration runs under the same mutation lock as submission/release.
    while IFS='|' read -r chain acct qos preempt wall; do
        [ -n "$chain" ] || continue
        use_chain "$chain"; load_profile
        if [ "$EXPIRES_AT" != 0 ] && [ "$(date +%s)" -ge "$EXPIRES_AT" ]; then
            ( TAG=$(tag_arg "$chain"); cmd_release ) || return 1
        fi
    done < "$race"
    [ -s "$race" ] || return 0
    refresh_race "$race"

    if race_scan "$race"; then
        use_chain "$RACE_BEST"; load_profile
        backoff_clear                  # it is running, so whatever went before is settled
        topup "$RACE_NODE" > /dev/null || return 1
        say "$(date -Is) on ${RACE_ACCT}: job ${RACE_JID}, node ${RACE_NODE}, $(pending_count) queued behind it"

        # Drop only what cannot improve on this: while the node can still be
        # taken away, a queue slot in a safer pool is worth more than the node.
        while IFS='|' read -r chain acct qos preempt wall; do
            [ -n "$chain" ] && [ "$chain" != "$RACE_BEST" ] || continue
            preempt_safe "$RACE_PRE" || ! preempt_safe "$preempt" || continue
            use_chain "$chain"
            [ -n "$(chain_rows)" ] || continue
            say "  dropping ${acct}"
            ( TAG=$(tag_arg "$chain"); cmd_release ) || return 1
        done < "$race"

        if preempt_safe "$RACE_PRE"; then
            say "$(date -Is) ${RACE_ACCT} is ${RACE_PRE} rather than cancel -- race over, still tending it"
            keep_only "$race" "$RACE_BEST"; return 0
        fi
    fi

    local idle="" now until strike
    while IFS='|' read -r chain acct qos preempt wall; do
        [ -n "$chain" ] || continue
        [ ! -f "${STATE_DIR}/${chain}.released" ] || continue
        [ "$chain" = "${RACE_BEST:-}" ] && continue

        # Once a node is held, only a pool that would be a genuine upgrade earns
        # a queue slot. The drop loop above cancels the losers, but it can only
        # cancel what exists: a loser that had already died was skipped there and
        # then resubmitted here, which is how amd-hyperloom went on being queued
        # every few minutes while amd-burst held a node it could not improve on.
        # Same test as the drop loop, so the two cannot disagree.
        if [ -n "${RACE_BEST:-}" ] &&
           { preempt_safe "$RACE_PRE" || ! preempt_safe "$preempt"; }; then
            continue
        fi

        use_chain "$chain"; load_profile

        if refloat_needed; then
            say "$(date -Is) ${acct} is pinned to ${REFLOAT_NODE} and ${REFLOAT_WHY} -- $(
                [ -n "$REPIN_TO" ] && printf 'repinning to %s' "$REPIN_TO" || printf 'refloating')"
            cancel_chain > /dev/null || return 1
            # The resubmission below reloads the profile, so a replacement pin
            # has to be on disk by then and not just in this shell.
            [ -z "$REPIN_TO" ] || { PIN_NODE=$REPIN_TO; save_profile; }
        elif [ -n "$(held_rows)" ] && [ -z "$(running_row)" ]; then
            say "$(date -Is) ${acct} is held on $(held_rows | awk -F'|' '{print $4; exit}') -- nothing can release it, replacing"
            cancel_chain > /dev/null || return 1
        elif [ -n "$(chain_rows)" ]; then
            [ -n "$(running_row)" ] && backoff_clear
            idle="${idle}${idle:+, }${acct#amd-} $(chain_rows | wc -l | tr -d ' ') queued"
            continue
        fi

        now=$(date +%s); until=$(backoff_until)
        if [ -n "$until" ] && [ "$now" -lt "$until" ]; then
            idle="${idle}${idle:+, }${acct#amd-} held off for $(( (until - now + 59) / 60 ))m"
            continue
        fi

        # Submit into the pool this loop is about to name, rather than letting
        # cmd_start resolve one afresh: with no profile loaded ACCOUNT is empty,
        # best_pool picks whichever pool scores highest, and the say() below
        # still reports $acct. On 2026-09-09 that put a brain-models chain onto
        # amd-hyperloom while the log read amd-brain-models, and the node was
        # killed a second after it landed -- a resubmission that lies about
        # where it went is worse than one that fails outright.
        load_profile
        ACCOUNT=$acct; NODEHOLD_QOS=$qos
        TAG=$(tag_arg "$chain")
        if out=$( cmd_start 2>&1 ); then
            strike=$(backoff_note)
            say "$(date -Is) resubmitted ${acct} (attempt ${strike%%|*}; $(( ${strike##*|} / 60 ))m before another)"
        elif printf '%s' "$out" | grep -qiE 'not associated with account|is not one of your accounts'; then
            # Associations are taken away as well as granted, and no amount of
            # retrying rebuilds a chain behind one that has gone: on 2026-09-02
            # amd-primus was revoked from this user with a job still running on
            # it. Take a strike so this backs off to hourly instead of shouting
            # every five minutes, and put it where a missed log cannot bury it.
            backoff_note > /dev/null
            alarm "${acct} is not yours any more -- '${chain}' cannot be refilled and ends with its last link.
  Keep the node by adopting it onto a pool you still have:
      ${SELF} -n <newname> adopt <the running jobid>
  What you may use now: ${SELF} pools"
        else
            say "$(date -Is) could not resubmit ${acct}: $(printf '%s' "$out" | tail -1)"
        fi
    done < "$race"

    # Say something every time even when there is nothing to do. A log that only
    # gets written when something happens is indistinguishable from a dead cron
    # entry, and that is exactly how a node went unnoticed as lost for 11 hours.
    [ -n "${RACE_BEST:-}" ] ||
        say "$(date -Is) [$(hostname -s)] no node yet -- waiting: ${idle:-nothing queued}"
}

# The detached half of `race`. It takes the first node that lands so that you are
# working straight away, but treats a preemptible one as a stopgap: the queue
# slots in the pools that cannot be preempted stay where they are, and the chain
# moves across the moment one of them comes through. Nothing here is idempotent
# against a storm either -- links preempted seconds apart can empty a chain in
# under a minute -- so a holder that disappears completely is put back.
cmd_racewait() {
    local race=$1
    while [ -s "$race" ]; do
        NODEHOLD_NAME="$(basename "$race" .race)" bash "$SELF" tick
        sleep "${NODEHOLD_POLL:-60}"
    done
}

# Successors can only be pinned once the holder actually has a node, which on a
# full cluster may be hours away. `arm` waits for that moment in the background.
# It waits on the login node, not on your laptop, so the lid can still be shut.
cmd_arm() {
    resolve_chain
    local pid
    pid=$(cat "$ARM_PID" 2>/dev/null || true)
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null &&
        die "chain '$(tag_of "$JOB_NAME")' is already armed (pid ${pid}) -- stop it with: kill ${pid}"

    [ -n "$(chain_rows)" ] || die "chain '$(tag_of "$JOB_NAME")' has no jobs -- 'start' one first"
    mkdir -p "$LOG_DIR"

    # bash reads a script as it runs it, so editing this file while a waiter is
    # asleep in it makes the waiter resume at the wrong byte and run nonsense.
    # Give it a snapshot of its own; $SELF still points at the real thing.
    local runner=${ARM_PID%.pid}.sh
    cp -f "$SELF" "$runner" 2>/dev/null || runner=$SELF
    NODEHOLD_SELF=$SELF setsid nohup bash "$runner" __arm "$JOB_NAME" \
        >> "${LOG_DIR}/${JOB_NAME}.arm.log" 2>&1 < /dev/null 8>&- &
    sleep 1

    pid=$(cat "$ARM_PID" 2>/dev/null || true)
    say "chain '$(tag_of "$JOB_NAME")' armed${pid:+ (pid ${pid})}: ${CHAIN} successors go in the moment it lands"
    say "  watching in ${LOG_DIR}/${JOB_NAME}.arm.log${pid:+ -- call it off with: kill ${pid}}"
}

# The detached half of `arm`. One shot: wait for a node, top the chain up, exit.
cmd_armwait() {
    use_chain "$1"; load_profile
    printf '%s' "$$" > "$ARM_PID"
    trap 'rm -f "$ARM_PID" "${ARM_PID%.pid}.sh"' EXIT

    local i run
    for (( i = 0; i < 2880; i++ )); do          # a minute apart, so ~48h
        if [ -z "$(chain_rows)" ]; then
            say "$(date -Is) chain '${JOB_NAME}' left the queue, nothing to arm"
            return 0
        fi
        run=$(running_row)
        if [ -n "$run" ]; then
            say "$(date -Is) holder $(fld 1 "$run") landed on $(fld 3 "$run")"
            NODEHOLD_NAME="$JOB_NAME" bash "$SELF" topup
            return $?
        fi
        sleep 60
    done
    say "$(date -Is) gave up waiting for a node after 48h"
}

cmd_shell() {
    need_running
    local jid node
    jid=$(fld 1 "$RUN_ROW"); node=$(fld 3 "$RUN_ROW")

    if [ "$USE_TMUX" = 1 ]; then
        # srun forwards TERM as-is, and tmux refuses to start under a dumb one.
        local term=${TERM:-}
        case "$term" in ''|dumb|unknown) term=xterm-256color ;; esac
        say "attaching to the persistent session on ${node} (job ${jid}) -- detach with Ctrl-b d"
        exec srun --jobid "$jid" --overlap --pty \
             env TERM="$term" tmux -S "$TMUX_SOCK" new-session -A -s main -c "$HOME_DIR"
    fi

    say "opening a plain shell on ${node} (job ${jid}) -- this one does not survive a disconnect"
    exec srun --jobid "$jid" --overlap --pty bash
}

cmd_exec() {
    [ $# -gt 0 ] || die "usage: ${SELF} [-n <name>] exec <command...>"
    need_running
    exec spur exec "$(fld 1 "$RUN_ROW")" "$@"
}

# Wipe the chain currently selected. Successors first: cancelling the running
# link while a dependent is still queued would just hand the node straight over.
# True if there was anything to cancel.
cancel_chain() {
    local rows pend run remaining
    rows=$(chain_rows) || { say "cannot query jobs; keeping chain state" >&2; return 1; }
    pend=$(printf '%s\n' "$rows" | awk -F'|' '$2=="PENDING" {print $1}')
    run=$(printf '%s\n' "$rows" | awk -F'|' '$2!="PENDING" {print $1}')
    if [ -n "$pend" ]; then
        say "  cancelling queued: $(echo $pend)"
        scancel $pend || return 1
        remaining=$(chain_rows) || return 1
        if printf '%s\n' "$remaining" | awk -F'|' '$2=="PENDING" {print $1}' | grep -q .; then
            say "queued cancellation is not yet confirmed; retry release" >&2; return 1
        fi
    fi
    if [ -n "$run" ]; then
        say "  cancelling running: $(echo $run)"
        scancel $run || return 1
    fi
    remaining=$(chain_rows) || return 1
    [ -z "$remaining" ] || { say "jobs remain in the queue; keeping state, retry release" >&2; return 1; }
    # Waiters observe the empty queue / released marker. A PID on shared NFS
    # may belong to a different host, so never kill an unverified local PID.
    rm -f "$STATE" "$PROFILE" "$RUNNER" "$LOST" "$STRIKES"
    return 0
}

# `topup` grows a chain and `stop` destroys one; nothing trimmed one until
# 2026-09-02, so cutting an over-deep chain back meant picking job ids out of
# squeue by hand -- 86 of them, the day a 90-link chain turned out to be no
# better than a 4-link one on a contended pool. Cancels from the tail, which is
# the only safe end: the oldest queued link is the successor holding the node.
cmd_shrink() {
    local want=${1:-} pend n drop
    resolve_chain
    case ${want:-} in
        '')       want=$CHAIN ;;
        *[!0-9]*) die "usage: ${SELF} [-n <name>] shrink [how-many-links-to-keep]" ;;
    esac

    pend=$(chain_rows | awk -F'|' '$2=="PENDING" {print $1}')
    n=$(printf '%s\n' "$pend" | grep -c .)
    if [ "$n" -le "$want" ]; then
        say "chain '$(tag_of "$JOB_NAME")' has ${n} queued, which is already ${want} or fewer"
    else
        drop=$(printf '%s\n' "$pend" | tail -n +"$(( want + 1 ))")
        say "cancelling $(printf '%s\n' "$drop" | grep -c .) of ${n} queued links, newest first: $(echo $drop)"
        scancel $drop 2>/dev/null
    fi

    # The new depth has to reach the profile as well, or the next tick tops the
    # chain straight back up to the old number and the trim reads as a no-op.
    CHAIN=$want; save_profile
    say "depth is now ${want}, saved -- cron will hold it there"
}

# A chain that cannot place a job backs off, doubling the wait each time up to
# two hours. After a cluster-wide event every strike it holds was earned by a
# failure that no longer applies, and sitting out the escalation only leaves the
# node unclaimed for longer. The strikes file was deleted by hand until now.
cmd_clear() {
    resolve_chain
    local until now strikes
    until=$(backoff_until); now=$(date +%s)
    if [ -z "$until" ] || [ "$until" = 0 ]; then
        say "chain '$(tag_of "$JOB_NAME")' is not backed off"
        return 0
    fi

    # Strikes outlive the wait they caused: the count is what decides how long
    # the next failure waits, so an expired backoff is still worth clearing.
    strikes=$(awk -F'|' 'NR==1 {print $1+0}' "$STRIKES" 2>/dev/null)
    backoff_clear
    if [ "$until" -gt "$now" ]; then
        say "cleared ${strikes:-?} strikes on '$(tag_of "$JOB_NAME")' -- it had $(( (until - now + 59) / 60 ))m left to wait"
        say "the next tick will try it again straight away"
    else
        say "cleared ${strikes:-?} strikes on '$(tag_of "$JOB_NAME")' -- it was not waiting, but the count"
        say "was still there, and would have made the next failure wait longer than it should"
    fi
}

# Every check that has been run by hand through an outage so far, in one place:
# a full quota that silenced ten hours of ticks, a controller restart that
# emptied the queue, a directory service that made sbatch refuse everything, and
# an account revoked from under a running chain.
cmd_doctor() {
    local pairs p chain acct qos alarms n bad=0

    scheduler_up    && say "scheduler:    answering" \
                    || { bad=1; say "scheduler:    NOT answering -- nothing can be submitted or tended"; }
    name_service_up && say "name service: resolves ${ME}" \
                    || { bad=1; say "name service: cannot resolve ${ME} -- sbatch will refuse every job"; }

    if home_writable; then
        say "home:         ${HOME_DIR} takes writes"
    else
        bad=1
        say "home:         WILL NOT TAKE A WRITE -- over quota. This one is silent by"
        say "              nature: the log line reporting it cannot be written either."
    fi

    n=$(crontab -l 2>/dev/null | grep -cE '^[^#].*node_holder[^|]*[[:space:]]tick([[:space:]]|$)')
    if [ "${n:-0}" -gt 0 ]; then
        say "cron:         tending ${n} chain(s) every 5 minutes"
    else
        bad=1
        say "cron:         NOTHING is tending these chains -- they will not refill"
        say "              add: */5 * * * * ${SELF} tick >> ${LOG_DIR}/tick.log 2>&1"
    fi

    say ""
    pairs=$(my_qos_pairs)
    for p in "$STATE_DIR"/*.conf; do
        [ -e "$p" ] || continue
        chain=$(basename "$p" .conf)
        # Both have to be cleared, not just ACCOUNT: load_profile lets a value
        # already in the environment win over the file, so whatever the previous
        # chain left behind would be reported for this one.
        use_chain "$chain"; ACCOUNT=""; NODEHOLD_QOS=""; load_profile
        acct=$ACCOUNT; qos=${NODEHOLD_QOS:-}

        printf '  %-18s %-32s %s\n' "$chain" "${acct:-?}${qos:+/${qos}}" \
            "$(chain_rows | awk -F'|' '$2!="PENDING"{r++} $2=="PENDING"{p++}
                END {printf "%d running, %d queued", r+0, p+0}')"

        if [ -n "$acct" ] && ! printf '%s\n' "$pairs" | grep -q "^${acct}|"; then
            bad=1
            say "                     ACCOUNT GONE -- ${acct} is not yours any more, so this"
            say "                     chain cannot be refilled. Keep its node with:"
            say "                       ${SELF} -n <newname> adopt <the running jobid>"
        fi
        n=$(backoff_until)
        [ -n "$n" ] && [ "$n" != 0 ] && [ "$n" -gt "$(date +%s)" ] &&
            say "                     backed off for another $(( (n - $(date +%s) + 59) / 60 ))m -- clear it with: ${SELF} -n $(tag_arg "$chain") clear"
    done

    alarms="/tmp/node_holder.${ME}.alarm"
    if [ -s "$alarms" ]; then
        say ""
        say "  last alarms ($(grep -c . "$alarms") in ${alarms}):"
        tail -3 "$alarms" | sed 's/^/    /'
    fi

    say ""
    [ "$bad" = 0 ] && say "nothing wrong here." || say "see above -- something needs attention."
}

cmd_doctor_json() {
    local scheduler=0 nameservice=0 homeok=0 cron_count pairs p chain acct qos
    local rows running pending backoff granted first=1 alarms=/tmp/node_holder.${ME}.alarm
    scheduler_up && scheduler=1
    name_service_up && nameservice=1
    home_writable && homeok=1
    cron_count=$(crontab -l 2>/dev/null | grep -cE '^[^#].*node_holder[^|]*[[:space:]]tick([[:space:]]|$)' || true)
    pairs=$(my_qos_pairs)

    printf '{"schemaVersion":1,"user":'; json_quote "$ME"
    printf ',"scheduler":{"answering":'; json_bool "$scheduler"
    printf '},"nameService":{"resolvesUser":'; json_bool "$nameservice"
    printf '},"home":{"path":'; json_quote "$HOME_DIR"
    printf ',"writable":'; json_bool "$homeok"
    printf '},"cron":{"tendedChains":'; json_number_or_null "${cron_count:-0}"
    printf '},"chains":['
    for p in "$STATE_DIR"/*.conf; do
        [ -e "$p" ] || continue
        chain=$(basename "$p" .conf)
        use_chain "$chain"; ACCOUNT=""; NODEHOLD_QOS=""; load_profile
        acct=${ACCOUNT:-}; qos=${NODEHOLD_QOS:-}
        rows=$(chain_rows)
        running=$(printf '%s\n' "$rows" | awk -F'|' '$2!="PENDING" {n++} END {print n+0}')
        pending=$(printf '%s\n' "$rows" | awk -F'|' '$2=="PENDING" {n++} END {print n+0}')
        backoff=$(backoff_until)
        granted=0
        if [ -n "$acct" ] && printf '%s\n' "$pairs" | grep -q "^${acct}|"; then
            granted=1
        fi
        [ "$first" = 1 ] || printf ','
        first=0
        printf '{"name":'; json_quote "$chain"
        printf ',"account":'; json_quote "$acct"
        printf ',"qos":'; json_quote "$qos"
        printf ',"accountGranted":'; json_bool "$granted"
        printf ',"runningJobs":'; json_number_or_null "$running"
        printf ',"pendingJobs":'; json_number_or_null "$pending"
        printf ',"tended":'; tended_by_cron "$chain" 2>/dev/null && printf true || printf false
        printf ',"released":'; [ -f "${STATE_DIR}/${chain}.released" ] && printf true || printf false
        printf ',"backoffUntil":'
        [ "${backoff:-0}" -gt 0 ] 2>/dev/null && json_number_or_null "$backoff" || printf null
        printf '}'
    done
    printf '],"alarms":['; first=1
    if [ -s "$alarms" ]; then
        while IFS= read -r line; do
            [ "$first" = 1 ] || printf ','
            first=0
            json_quote "$line"
        done < <(tail -3 "$alarms")
    fi
    printf ']}\n'
}

cmd_stop() {
    resolve_chain
    chain_exists || die "no chain named '${JOB_NAME}' -- nothing to stop; list chains with: ${SELF} status"
    cancel_chain || die "cancellation incomplete; state preserved"
    say "jobs cancelled for ${JOB_NAME}; use release to disable maintenance as well"
}

# --- tending ---------------------------------------------------------------

# Chain names nest (hold-bm3 is a prefix of hold-bm30), so every match here is on
# the assignment plus its trailing space, never the bare name.
# Serialize scheduler/state/cron mutations across invocations on shared home.
mutation_lock() {
    exec 8> "${LOCK_DIR}/operations.lock" || die "cannot open operation lock"
    flock -w 30 8 || die "another operation is active; retry shortly"
}

ensure_race() {
    [ ! -f "${STATE_DIR}/${JOB_NAME}.released" ] || die "chain is released; use start to re-enable it"
    [ "${IN_RACE:-0}" = 1 ] && return 0
    local race=${STATE_DIR}/${JOB_NAME}.race
    [ -f "$race" ] && return 0
    [ -n "$ACCOUNT" ] || resolve_pool
    NODEHOLD_QOS=${NODEHOLD_QOS:-$(qos_of_account "$ACCOUNT")}
    printf '%s|%s|%s|%s|%s\n' "$JOB_NAME" "$ACCOUNT" "$NODEHOLD_QOS" \
        "$(my_preempt_mode)" "$(time_to_seconds "$TIME_LIMIT")" > "$race" || die "cannot write $race"
}

# Some login nodes deny the user crontab entirely (crontab.deny). That is a
# permanent, expected condition on parts of this fleet, not a transient error.
# cron_available answers it (memoized per process) and is checked directly at
# every place that would read or write the crontab, so a chain can still be
# submitted and released on such a node -- it just is not tended from there.
# A global flag set inside read_cron would not do: read_cron is called as
# $(read_cron), a subshell, so anything it assigns is lost to the caller.
CRON_OK=""
cron_available() {
    if [ -z "$CRON_OK" ]; then
        local err
        err=$(crontab -l 2>&1 >/dev/null)
        if [ -z "$err" ] || printf '%s' "$err" | grep -qi 'no crontab for'; then
            CRON_OK=1
        else
            CRON_OK=0
        fi
    fi
    [ "$CRON_OK" = 1 ]
}

read_cron() {
    local err out rc
    err=$(mktemp) || return 1
    out=$(crontab -l 2> "$err"); rc=$?
    if [ "$rc" != 0 ] && ! grep -qi 'no crontab for' "$err"; then
        # A node that denies crontab is handled by cron_available at every write
        # site; return empty (not failure) so the start guard does not abort and
        # tended_by_cron does not echo the scheduler's refusal on every tick.
        if grep -qiE 'not allowed to use this program|not permitted to use|are not allowed' "$err"; then
            rm -f "$err"; return 0
        fi
        cat "$err" >&2; rm -f "$err"; return 1
    fi
    rm -f "$err"
    printf '%s' "$out"
}

tended_by_cron() {
    local current
    current=$(read_cron) || return 1
    printf '%s\n' "$current" | awk -v n="NODEHOLD_NAME=$1 " \
        '$0 !~ /^[[:space:]]*#/ && index($0,n) {found=1} END {exit !found}'
}

cron_quote() {
    local value=$1
    value=${value//\'/\'\\\'\'}
    value=${value//%/\\%}
    printf "'%s'" "$value"
}

tick_line() {
    printf '*/5 * * * * NODEHOLD_NAME=%s NODEHOLD_DIR=%s %s tick >> %s 2>&1\n' \
        "$JOB_NAME" "$(cron_quote "$STATE_DIR")" "$(cron_quote "$SELF")" \
        "$(cron_quote "${LOG_DIR}/${JOB_NAME}.tick.log")"
}

tend_now() {
    local current
    if ! cron_available; then
        say "note: crontab is disabled for you on $(hostname -s), so '${JOB_NAME}' is not tended from here."
        say "      its holder and queued successors still run, but it will not auto-renew until you tend"
        say "      it from a login node that allows crontab: ${SELF} -n $(tag_arg "$JOB_NAME") tend"
        return 0
    fi
    mkdir -p "$LOG_DIR" || die "cannot create log directory"
    current=$(read_cron) || die "cannot read crontab; no changes made"
    if printf '%s\n' "$current" | awk -v n="NODEHOLD_NAME=$JOB_NAME " \
        '$0 !~ /^[[:space:]]*#/ && index($0,n) {found=1} END {exit !found}'; then return 0; fi
    { [ -z "$current" ] || printf '%s\n' "$current"; tick_line; } | crontab - || die "cannot install cron"
}

untend_now() {
    cron_available || return 0
    local current
    current=$(read_cron) || die "cannot read crontab; no changes made"
    printf '%s\n' "$current" | awk -v n="NODEHOLD_NAME=$JOB_NAME " 'index($0,n)==0 && NF' | crontab - || die "cannot update cron"
}

cmd_tend() {
    resolve_chain
    [ -f "$PROFILE" ] || die "no saved chain; start or adopt it first"
    ensure_race
    tend_now
    cron_available && say "cron tends ${JOB_NAME} every five minutes on $(hostname -s)"
}

cmd_untend() {
    resolve_chain
    if ! cron_available; then
        say "crontab is disabled for you on $(hostname -s) -- nothing to untend here"
    elif untend_now; then
        say "cron no longer tends '$(tag_of "$JOB_NAME")' -- its jobs are untouched"
    else
        say "cron was not tending '$(tag_of "$JOB_NAME")'"
    fi
}

# Give a node up for good. This is three things, and the order is the whole
# point: `stop` cancels the jobs but leaves the race file and the cron line, so
# the next tick -- up to five minutes later, long after you have stopped
# watching -- finds an empty chain and submits a fresh holder for it. Tending
# goes first, then the race file that tending reads, and only then the jobs.
# Several nodes at once, as several chains. Each cmd_start runs in a subshell
# because start calls die() on a refusal and die() exits: called plainly, one
# pool being full would take the whole run down and leave the nodes it had
# already got unreported. What each one writes to the state dir survives the
# subshell, which is the part that matters.
cmd_start_many() {
    local base=$TAG i got=0
    reconcile_ask
    [ "$EXCLUSIVE" = 1 ] || die "-N requires --exclusive for distinct nodes; otherwise use separate -w requests"
    [ -z "$PIN_NODE" ] ||
        die "--nodes ${WANT_NODES} and -w ${PIN_NODE} cannot both hold: one node
             cannot take ${WANT_NODES} chains. Start them one at a time with -w."

    for (( i = 1; i <= WANT_NODES; i++ )); do
        TAG="${base:+${base}-}${i}"
        say "--- chain '${TAG}' (${i} of ${WANT_NODES}) ---"
        ( cmd_start "$@" ) && got=$(( got + 1 ))
    done

    TAG=$base
    say "started ${got} of ${WANT_NODES} chains -- '${SELF} status' shows what landed"
    [ "$got" -eq "$WANT_NODES" ]
}

# A name is a real chain only if it has jobs, a saved profile, or race
# membership. Anything else is almost always a mistake -- most often a full
# chain name passed to -n, which is then re-prefixed into a phantom -- so the
# stop/release paths refuse it instead of silently tombstoning a name that never
# existed and reporting success, which reads as "the cancel did nothing".
chain_exists() {
    [ -n "$(chain_rows)" ] && return 0
    [ -f "$PROFILE" ] && return 0
    local race
    for race in "$STATE_DIR"/*.race; do
        [ -f "$race" ] || continue
        awk -F'|' -v n="$JOB_NAME" '$1==n {found=1} END {exit !found}' "$race" && return 0
    done
    return 1
}

cmd_release() {
    resolve_chain
    chain_exists || die "no chain named '${JOB_NAME}' -- nothing to release. A full chain
             name passed to -n is re-prefixed into a name like this; list the real
             ones and release by tag with: ${SELF} status"
    local race tmp gone=${STATE_DIR}/superseded ts
    ts=$(date +%Y%m%d-%H%M%S)
    # Persistent shared tombstone blocks every new submission, even from a
    # remaining remote cron entry. Explicit start is the only reset.
    printf '%s\n' "$ts" > "${STATE_DIR}/${JOB_NAME}.released" || die "cannot disable chain"
    mkdir -p "$gone" || die "cannot create archive"
    untend_now
    for race in "$STATE_DIR"/*.race; do
        [ -f "$race" ] || continue
        awk -F'|' -v n="$JOB_NAME" '$1==n {found=1} END {exit !found}' "$race" || continue
        cp "$race" "$gone/$(basename "$race").released-$ts" || die "cannot archive race"
        tmp=$race.new
        awk -F'|' -v n="$JOB_NAME" '$1!=n' "$race" > "$tmp" || die "cannot remove race membership"
        if [ -s "$tmp" ]; then mv "$tmp" "$race"; else rm -f "$tmp" "$race"; fi
    done
    [ ! -f "$PROFILE" ] || cp "$PROFILE" "$gone/${JOB_NAME}.conf.released-$ts" || die "cannot archive profile"
    cancel_chain || die "release incomplete: submissions disabled, but cancellation not confirmed; retry release"
    say "released ${JOB_NAME}; no jobs remain and new submissions are disabled"
    say "local cron removed; remote cron entries, if any, are blocked by shared release state"
}

# Every chain at once. Holding four nodes means four chains, and releasing them
# one command at a time leaves cron tending the ones you have not reached yet.
# Each release runs in a subshell because release dies on a refusal, and one
# chain that will not let go must not strand the rest still being tended.
cmd_release_all() {
    local names n done_=0 failed=0
    names=$(known_chains) || die "cannot enumerate chains"
    [ -n "$names" ] || { say "no chains of '${PREFIX}' to release"; return 0; }

    say "releasing every '${PREFIX}' chain: $(printf '%s' "$names" | tr '\n' ' ')"
    while read -r n; do
        [ -n "$n" ] || continue
        say "--- ${n} ---"
        if ( TAG=$(tag_arg "$n"); cmd_release ); then done_=$(( done_ + 1 ))
        else failed=$(( failed + 1 )); say "  could not release ${n} -- retry it on its own"
        fi
    done <<< "$names"

    say "released ${done_} chain(s)$( [ "$failed" -gt 0 ] && printf ', %s still need attention' "$failed" )"
    [ "$failed" = 0 ]
}

usage() {
    cat <<EOF
node_holder.sh -- keep ${PARTITION} GPU nodes across a chain of batch jobs

  usage: ${SELF} [-n <name>] <command> [args]

  start            grab a node (${GPUS} GPUs, ${TIME_LIMIT}$([ "$EXCLUSIVE" = 1 ] && echo ', exclusive')) and automatically tend ${CHAIN} successors
  race             race eligible high-priority pools; cron keeps the winner
  pools            which accounts and QOS you may use, and how full each one is
  pools-json       pools as stable JSON for dashboards and other tools
  adopt <jobid>    chain behind a job you already hold, keeping its node
  status           show every chain, its node and remaining runway
  status-json      status as stable JSON for dashboards and other tools
  shell            attach to the persistent session on the held node
  exec <cmd...>    run one command on the held node (non-interactive)
  topup            re-fill the chain back to ${CHAIN} queued successors
  shrink [n]       cancel queued links from the tail until only n are left
  arm              wait in the background and topup the moment a node lands
  clear            drop the backoff so the next tick retries at once
  doctor           check quota, scheduler, cron and every chain's account
  doctor-json      diagnostics as stable JSON for dashboards and other tools
  tend             have cron tick this chain every 5 minutes on this login node
  untend           stop cron ticking it, leaving its jobs alone
  stop             cancel the chain's jobs (cron and race file survive, so
                   the next tick raises a fresh holder within 5 minutes)
  release          give the node up for good: tombstone it in shared state so
                   no cron on any login node can revive it, untend, leave the
                   race membership, archive settings, then cancel. Use this rather
                   than stop when you are actually done with a node.
  release --all    every chain of this prefix, including ones with no jobs left

  -n <name>        which chain to act on; omit for the default one, or for the
                   only one that exists. Use it to hold a second node:
                       ${SELF} -n two start
  -w <node>        keep this exact node as the target, including replacements
  -q <qos>         submit to a named QOS        -A <account>  and/or account
  --any-qos        explicitly bypass the QoS priority threshold (default 10000)
  -g <n>           GPUs per node, 0-8 (default ${GPUS}); below 8 drops
                   --exclusive unless you pass it back explicitly
  -N <n>           hold n nodes, as n chains named <name>-1 .. <name>-n
  --days <n>       how much runway to keep QUEUED. No single job on
  --hours <n>      ${PARTITION} may exceed its ceiling, so this sets chain
                   depth, not --time. Additive: --days 2 --hours 12
  --chain <n>      number of QUEUED successors (running holder is additional)
  --time <time>    per-job wall limit, e.g. 06:00:00; trimmed to live caps
  -c, --cpus <n>   CPUs per allocation; 0 derives a share for 1..7 GPUs
  --for-days <n> / --for-hours <n>  expire from submission, queue time included
  --exclusive      take the whole node       --no-exclusive   share it

  examples:
    ${SELF} -n big -q amd-brain-models-qos start     high-priority pool by name
    ${SELF} -n big -w crsuse2-m2m-026 start          that node specifically
    ${SELF} -n big -N 3 --days 4 start               three nodes, 4 days runway
    ${SELF} -n big -g 2 start                        two GPUs, sharing the node
    ${SELF} -n big release                           hand the node back for good
    ${SELF} release --all                            hand every one back

Without --for-days/--for-hours the hold has no deadline.
--days/--hours set potential queued runway, not guaranteed retention.
start/adopt install cron automatically; NODEHOLD_TEND=0 opts out.
Use NODEHOLD_NAME=hold for existing hold-* chains. Full guide in the header.
EOF
}

CLI_QOS=${NODEHOLD_QOS:-}
ALL_CHAINS=0
TAG=""

# Flags are read twice, so that `start -N 2 --days 3` and `-N 2 --days 3 start`
# mean the same thing -- writing them after the verb is the natural way round
# and used to leave them sitting unparsed in "$@", where --all silently became
# an argument to release rather than a flag on it. Anything not a flag ends the
# pass and is handed back untouched, which is what keeps `exec` and `adopt`
# arguments out of this.
FLAG_REST=()
parse_flags() {
    FLAG_REST=()
    while [ $# -gt 0 ]; do
    case "$1" in
        -n|--name) TAG=${2:-}; [ -n "$TAG" ] || die "-n needs a chain name"; shift 2 ;;
        -w|--node) PIN_NODE=${2:-}; NODEHOLD_NODE_WANT=$PIN_NODE; [ -n "$PIN_NODE" ] || die "-w needs a node name"; shift 2 ;;
        --pair)    PAIR=${2:-}; NODEHOLD_PAIR=$PAIR; [ -n "$PAIR" ] || die "--pair needs the other chain's name"; shift 2 ;;
        # Both of these are also read from the environment at the top of the
        # script, and load_profile prefers NODEHOLD_* over whatever a saved
        # profile holds -- so setting the variable too is what stops a stale
        # .conf quietly overriding the pool you just named on the command line.
        -q|--qos)  NODEHOLD_QOS=${2:-}; CLI_QOS=$NODEHOLD_QOS; [ -n "$NODEHOLD_QOS" ] || die "-q needs a QOS"
                   shift 2 ;;
        -A|--account) ACCOUNT=${2:-}; [ -n "$ACCOUNT" ] || die "-A needs an account"
                   NODEHOLD_ACCOUNT=$ACCOUNT; shift 2 ;;
        --any-qos) ANY_QOS=1; NODEHOLD_ANY_QOS=1; shift ;;
        -g|--gpus) GPUS=${2:-}; NODEHOLD_GPUS=$GPUS
                   case $GPUS in ''|*[!0-9]*) die "-g needs a number of GPUs (0-8)" ;; esac
                   [ "$GPUS" -le 8 ] || die "-g must be between 0 and 8"
                   GPUS=$((10#$GPUS)); NODEHOLD_GPUS=$GPUS
                   shift 2 ;;
        -c|--cpus) CPUS=${2:-}
                   case $CPUS in ''|*[!0-9]*) die "-c needs a number of CPUs per allocation" ;; esac
                   CPUS=$((10#$CPUS)); NODEHOLD_CPUS=$CPUS
                   shift 2 ;;
        -N|--nodes) WANT_NODES=${2:-}; NODEHOLD_NODES=$WANT_NODES
                   case $WANT_NODES in ''|*[!0-9]*|0) die "-N needs a node count of 1 or more" ;; esac
                   WANT_NODES=$((10#$WANT_NODES)); NODEHOLD_NODES=$WANT_NODES
                   [ "$WANT_NODES" -gt 0 ] || die "-N must be positive"
                   shift 2 ;;
        # Duration of the whole hold, not of one link. Additive, so
        # "--days 2 --hours 12" is two and a half days.
        -days|--days)  case ${2:-} in ''|*[!0-9]*) die "--days needs a whole number" ;; esac
                 HOLD_FOR_S=$(( HOLD_FOR_S + 10#$2 * 86400 )); shift 2 ;;
        -hours|--hours) case ${2:-} in ''|*[!0-9]*) die "--hours needs a whole number" ;; esac
                 HOLD_FOR_S=$(( HOLD_FOR_S + 10#$2 * 3600 )); shift 2 ;;
        --chain) case ${2:-} in ''|*[!0-9]*) die "--chain needs a nonnegative integer" ;; esac
                 CHAIN=$((10#$2)); NODEHOLD_CHAIN=$CHAIN; shift 2 ;;
        --time) TIME_LIMIT=${2:-}
                [[ $TIME_LIMIT =~ ^([0-9]+-)?[0-9]+:[0-9]+:[0-9]+$ ]] || die "--time expects HH:MM:SS or D-HH:MM:SS"
                NODEHOLD_TIME=$TIME_LIMIT; TIME_PINNED=1
                HOLD_SECONDS=$(( $(time_to_seconds "$TIME_LIMIT") - 180 ))
                [ "$HOLD_SECONDS" -gt 0 ] || die "--time must exceed three minutes"
                NODEHOLD_HOLD_SECONDS=$HOLD_SECONDS; shift 2 ;;
        --for-days|--for-hours)
                case ${2:-} in ''|*[!0-9]*) die "$1 needs a positive integer" ;; esac
                [ "$2" -gt 0 ] || die "$1 needs a positive integer"
                if [ "$1" = --for-days ]; then FINITE_FOR_S=$((FINITE_FOR_S + 10#$2 * 86400)); else FINITE_FOR_S=$((FINITE_FOR_S + 10#$2 * 3600)); fi
                shift 2 ;;
        --exclusive)    EXCLUSIVE=1; EXCL_SET=1; NODEHOLD_EXCLUSIVE=1; shift ;;
        --no-exclusive) EXCLUSIVE=0; EXCL_SET=1; NODEHOLD_EXCLUSIVE=0; shift ;;
        --all) ALL_CHAINS=1; shift ;;
        --) shift; FLAG_REST+=("$@"); return 0 ;;
        -h|--help) FLAG_REST+=("$@"); return 0 ;;
        -*) die "unknown option $1" ;;
        *) FLAG_REST+=("$@"); return 0 ;;
    esac
    done
    return 0
}

# Resolve partner tags once in the caller's prefix, before cron uses the full
# chain name as its prefix. Store the full scheduler name in the profile.
normalize_pair() {
    [ -n "$PAIR" ] || return 0
    case $PAIR in *[!a-zA-Z0-9_.-]*) die "invalid partner chain name" ;; esac
    case $PAIR in "$PREFIX"|"$PREFIX"-*) ;; *) PAIR="${PREFIX}-${PAIR}" ;; esac
    NODEHOLD_PAIR=$PAIR
}

parse_flags "$@"
set -- ${FLAG_REST[@]+"${FLAG_REST[@]}"}

# Only commands with no positional arguments of their own get the second pass.
# exec takes a command line, adopt a job id and shrink a depth, and none of
# those are ours to reinterpret.
case "${1:-}" in
    start|status|status-json|pools|pools-json|doctor|doctor-json|tend|untend|release|stop|topup|race|arm|clear|tick)
        NH_CMD=$1; shift
        parse_flags "$@"
        set -- "$NH_CMD" ${FLAG_REST[@]+"${FLAG_REST[@]}"}
        ;;
esac

normalize_pair
case $REPIN in 0|1) ;; *) die "NODEHOLD_REPIN must be 0 or 1" ;; esac
case $GPUS in ''|*[!0-9]*) die "GPU count must be 0..8" ;; esac
[ "$GPUS" -le 8 ] || die "GPU count must be 0..8"
case $CPUS in ''|*[!0-9]*) die "CPU count must be a nonnegative integer (0 = automatic)" ;; esac
CPUS=$((10#$CPUS))
case $WANT_NODES in ''|*[!0-9]*) die "node count must be positive" ;; esac
[ "$WANT_NODES" -gt 0 ] || die "node count must be positive"
case $CHAIN in ''|*[!0-9]*) die "queued chain depth must be nonnegative" ;; esac
if [ "$FINITE_FOR_S" -gt 0 ]; then
    case ${1:-} in start|adopt) EXPIRES_AT=$(( $(date +%s) + FINITE_FOR_S )) ;; *) die "--for-days/--for-hours apply only to start or adopt" ;; esac
fi
case ${1:-} in
    start|status|status-json|pools|pools-json|doctor|doctor-json|tend|untend|release|stop|topup|race|arm|clear|tick)
        [ $# -eq 1 ] || die "unexpected arguments after $1" ;;
esac
[ "$ALL_CHAINS" = 0 ] || [ "${1:-}" = release ] || die "--all applies only to release"
case "${1:-}" in
    start|adopt|topup|shrink|tick|race|clear|tend|untend|release|stop)
        mutation_lock ;;
esac
if [ "${1:-}" = start ]; then
    EXPLICIT_START=1
fi

case "${1:-}" in
    start)  shift
            if [ "${WANT_NODES:-1}" -gt 1 ]; then cmd_start_many "$@"; else cmd_start "$@"; fi ;;
    adopt)  shift; cmd_adopt  "$@" ;;
    status) shift; cmd_status "$@" ;;
    status-json) shift; cmd_status_json "$@" ;;
    shell)  shift; cmd_shell  "$@" ;;
    exec)   shift; cmd_exec   "$@" ;;
    topup)  shift; cmd_topup  "$@" ;;
    shrink) shift; cmd_shrink "$@" ;;
    tick)   shift; cmd_tick   "$@" ;;
    pools)  shift; cmd_pools  "$@" ;;
    pools-json) shift; cmd_pools_json "$@" ;;
    race)   shift; cmd_race   "$@" ;;
    arm)    shift; cmd_arm    "$@" ;;
    clear)  shift; cmd_clear  "$@" ;;
    doctor) shift; cmd_doctor "$@" ;;
    doctor-json) shift; cmd_doctor_json "$@" ;;
    tend)    shift; cmd_tend    "$@" ;;
    untend)  shift; cmd_untend  "$@" ;;
    release) shift
             if [ "${ALL_CHAINS:-0}" = 1 ]; then cmd_release_all "$@"
             elif [ "$WANT_NODES" -gt 1 ]; then
                 base=$TAG; failed=0
                 for ((i=1;i<=WANT_NODES;i++)); do ( TAG="${base:+${base}-}${i}"; cmd_release ) || failed=1; done
                 [ "$failed" = 0 ]
             else cmd_release "$@"; fi ;;
    stop)   shift; cmd_stop   "$@" ;;
    __hold) shift; cmd_hold    "$@" ;;
    __arm)  shift; cmd_armwait "$@" ;;
    __race) shift; cmd_racewait "$@" ;;
    ''|-h|--help|help) usage ;;
    *) die "unknown command '$1' (try --help)" ;;
esac
