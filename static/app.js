const state = {
  csrf: document.querySelector('meta[name="csrf-token"]').content,
  capabilities: null,
  status: null,
  pools: null,
  sqa: null,
  history: null,
  diagnostics: null,
  currentTab: "status",
  expandedPools: new Set(),
  refreshing: false,
  messageTimer: null,
  confirmResolve: null,
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

function element(tag, className = "", text = null) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== null) node.textContent = text;
  return node;
}

function setText(selector, value) {
  const node = $(selector);
  if (node) node.textContent = value ?? "—";
}

function showMessage(text, isError = false) {
  const box = $("#message");
  clearTimeout(state.messageTimer);
  box.textContent = text;
  box.className = isError ? "visible error" : "visible";
  state.messageTimer = setTimeout(() => {
    box.className = "";
  }, 7000);
}

async function fetchJson(path) {
  const response = await fetch(path, { cache: "no-store" });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || `Request failed (${response.status})`);
  return payload;
}

async function refreshCsrf() {
  const payload = await fetchJson("/api/csrf-token");
  state.csrf = payload.token;
}

async function postJson(path, payload) {
  await refreshCsrf();
  const send = () =>
    fetch(path, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": state.csrf,
      },
      body: JSON.stringify(payload),
    });
  let response = await send();
  let data = await response.json();
  if (response.status === 403 && data.error === "Invalid CSRF token") {
    await refreshCsrf();
    response = await send();
    data = await response.json();
  }
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

async function copyText(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const input = element("textarea");
  input.value = text;
  input.setAttribute("readonly", "");
  input.style.position = "fixed";
  input.style.opacity = "0";
  document.body.append(input);
  input.select();
  try {
    if (!document.execCommand("copy")) throw new Error("Copy failed");
  } finally {
    input.remove();
  }
}

function statusBadge(value) {
  return element("span", `status ${String(value || "").toLowerCase()}`, value || "UNKNOWN");
}

function cell(content, className = "") {
  const td = element("td", className);
  if (content instanceof Node) td.append(content);
  else td.textContent = content ?? "—";
  return td;
}

function primaryCell(primary, secondary) {
  const wrap = element("div", "primary-cell");
  wrap.append(element("strong", "", primary), element("small", "", secondary || ""));
  return wrap;
}

function formatDate(value) {
  if (!value || value === "Unknown" || value === "N/A") return "—";
  const parsed = new Date(value.endsWith?.("Z") ? value : `${value}Z`);
  return Number.isNaN(parsed.valueOf()) ? value : parsed.toLocaleString();
}

function formatSeconds(seconds) {
  if (seconds === null || seconds === undefined) return "—";
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return days ? `${days}d ${hours}h` : hours ? `${hours}h ${minutes}m` : `${minutes}m`;
}

const CHAIN_ACTION_HELP = Object.freeze({
  topup: "Submit missing successors until the chain reaches its saved queue depth.",
  shrink: "Cancel the newest pending successors and save N as the new queue depth.",
  arm: "Wait in the background for the chain to land, then top it up once.",
  clear: "Reset retry backoff so the next maintenance tick can retry immediately.",
  tend: "Install local cron maintenance to check and refill the chain every 5 minutes.",
  untend: "Remove local cron maintenance; existing running and queued jobs stay.",
  release: "Disable renewal, cancel queued successors, then cancel the running holder.",
});

function actionButton(label, handler, className = "ghost", title = "") {
  const button = element("button", `button compact ${className}`, label);
  button.type = "button";
  if (title) {
    button.title = title;
    button.dataset.tooltip = title;
    button.className += " has-tooltip";
    button.setAttribute("aria-label", `${label}: ${title}`);
  }
  button.addEventListener("click", handler);
  return button;
}

function selectedMode() {
  return $('input[name="mode"]:checked').value;
}

function selectedPool() {
  const account = $("#account").value;
  const qos = $("#qos").value;
  return state.pools?.pools?.find((pool) => pool.account === account && pool.qos === qos) || null;
}

function updatePrefixHint() {
  const capabilities = state.capabilities;
  if (!capabilities) return;
  const prefix =
    selectedMode() === "chain" ? capabilities.chainPrefix : capabilities.normalPrefix;
  setText("#prefix-hint", `Submitted to the scheduler as ${prefix}-<name>.`);
}

function renderCapabilities() {
  const capabilities = state.capabilities;
  if (!capabilities) return;
  updatePrefixHint();
  setText(
    "#priority-floor-hint",
    `Override node_holder’s priority floor if this QoS is below ${capabilities.minPriority}`,
  );
}

function renderSummary() {
  const summary = state.status?.summary || {};
  setText("#running-jobs", summary.runningJobs ?? "—");
  setText("#pending-jobs", summary.pendingJobs ?? "—");
  setText("#chain-count", summary.chains ?? "—");
  setText("#running-detail", `Across ${summary.runningNodes ?? "—"} allocated nodes`);
  setText("#pool-count", state.pools?.pools?.length ?? "—");
  const best = state.pools?.bestPool;
  setText("#best-pool", best ? `${best.account} / ${best.qos}` : "No pool resolved");
}

function renderPoolSelectors() {
  if (!state.pools) return;
  const accountSelect = $("#account");
  const previousAccount = accountSelect.value;
  const accounts = [...new Set(state.pools.pools.map((pool) => pool.account))];
  accountSelect.replaceChildren();
  for (const account of accounts) {
    const option = element("option", "", account);
    option.value = account;
    accountSelect.append(option);
  }
  const bestAccount = state.pools.bestPool?.account;
  accountSelect.value = accounts.includes(previousAccount)
    ? previousAccount
    : accounts.includes(bestAccount)
      ? bestAccount
      : accounts[0] || "";
  renderQosOptions();
}

function renderQosOptions() {
  const account = $("#account").value;
  const qosSelect = $("#qos");
  const previousQos = qosSelect.value;
  const pools = (state.pools?.pools || []).filter((pool) => pool.account === account);
  qosSelect.replaceChildren();
  for (const pool of pools) {
    const option = element(
      "option",
      "",
      `${pool.qos}${pool.defaultQos ? " · default" : ""}`,
    );
    option.value = pool.qos;
    qosSelect.append(option);
  }
  const bestQos = state.pools?.bestPool?.account === account ? state.pools.bestPool.qos : null;
  qosSelect.value = pools.some((pool) => pool.qos === previousQos)
    ? previousQos
    : pools.some((pool) => pool.qos === bestQos)
      ? bestQos
      : pools[0]?.qos || "";
  updateRequestPreview();
}

function updateRequestMode() {
  const chain = selectedMode() === "chain";
  $$(".chain-only").forEach((node) => node.classList.toggle("hidden", !chain));
  $("#adopt-field").classList.toggle(
    "hidden",
    !chain || $("#strategy").value !== "adopt",
  );
  $("#submit-button").firstChild.textContent = chain ? "Submit chain " : "Submit normal job ";
  updatePrefixHint();
  updateRequestPreview();
}

function updateRequestPreview() {
  const preview = $("#request-preview");
  const pool = selectedPool();
  if (!pool) {
    preview.className = "request-preview";
    preview.textContent = "Choose an account and QoS to preview this request.";
    return;
  }
  const mode = selectedMode();
  const gpus = $("#gpus").value;
  const nodes = $("#nodes").value;
  const cpus = $("#cpus").value || "auto";
  const time = $("#time-limit").value;
  const depth = $("#runway-hours").value > 0
    ? `${$("#runway-hours").value}h rolling runway`
    : `${$("#chain-depth").value} queued successors`;
  const floor = state.capabilities?.minPriority ?? 10000;
  const warnings = [];
  if (pool.preemptMode === "cancel") warnings.push("preemption cancels running work");
  if ((pool.priority ?? 0) < floor) warnings.push(`priority ${pool.priority} needs an override`);
  if (pool.nodeCap && pool.usedNodes >= pool.nodeCap) warnings.push("pool is currently at its node cap");
  if (pool.maxSubmitPerUser) warnings.push(`maximum ${pool.maxSubmitPerUser} submitted jobs per user`);
  preview.className = `request-preview${warnings.length ? " warning" : ""}`;
  preview.textContent =
    `${mode === "chain" ? `Maintained chain with ${depth}` : "One finite job"} · ` +
    `${nodes} node${nodes === "1" ? "" : "s"} · ${gpus} GPU${gpus === "1" ? "" : "s"}/node · ` +
    `${cpus} CPUs · ${time} · ${pool.account}/${pool.qos}.` +
    (warnings.length ? ` Warning: ${warnings.join("; ")}.` : "");
  setText(
    "#pool-policy",
    `Priority ${pool.priority ?? "?"} · preempt ${pool.preemptMode || "?"} · ` +
      `${pool.usedNodes}/${pool.nodeCap ?? "?"} nodes used`,
  );
  if (pool.maxSubmitPerUser && selectedMode() === "chain") {
    $("#chain-depth").max = Math.max(0, pool.maxSubmitPerUser - 1);
  } else {
    $("#chain-depth").max = 64;
  }
  const needsSafety = pool.preemptMode === "cancel" || (pool.priority ?? 0) < floor;
  $("#advanced-options").classList.toggle(
    "hidden",
    selectedMode() !== "chain" && !needsSafety,
  );
}

function requestPayload() {
  const mode = selectedMode();
  const runway = Number($("#runway-hours").value || 0);
  return {
    mode,
    strategy: mode === "chain" ? $("#strategy").value : "start",
    jobName: $("#job-name").value.trim(),
    timeLimit: $("#time-limit").value.trim(),
    gpus: Number($("#gpus").value),
    cpus: $("#cpus").value ? Number($("#cpus").value) : null,
    nodes: Number($("#nodes").value),
    account: $("#account").value,
    qos: $("#qos").value,
    node: $("#node").value.trim(),
    exclusive: $("#exclusive").checked,
    chainDepth: runway ? null : Number($("#chain-depth").value),
    runwayHours: runway,
    expiryHours: Number($("#expiry-hours").value || 0),
    repin: $("#repin").checked,
    anyQos: $("#any-qos").checked,
    acceptPreemption: $("#accept-preemption").checked,
    adoptJobId: $("#adopt-job-id").value.trim(),
  };
}

function confirmAction({ title, copy, acceptLabel = "Confirm", typed = null, inputLabel = null }) {
  const dialog = $("#confirm-dialog");
  $("#confirm-title").textContent = title;
  $("#confirm-copy").textContent = copy;
  $("#confirm-accept").textContent = acceptLabel;
  const wrap = $("#confirm-type-wrap");
  const input = $("#confirm-type");
  input.value = "";
  wrap.classList.toggle("hidden", !typed && !inputLabel);
  wrap.querySelector("span").textContent = inputLabel || `Type “${typed}” to continue`;
  dialog.showModal();
  if (!wrap.classList.contains("hidden")) input.focus();
  else $("#confirm-cancel").focus();
  return new Promise((resolve) => {
    state.confirmResolve = () => {
      if (typed && input.value !== typed) {
        showMessage(`Type ${typed} exactly to continue.`, true);
        return;
      }
      dialog.close();
      state.confirmResolve = null;
      resolve(inputLabel ? input.value : true);
    };
    dialog.addEventListener(
      "close",
      () => {
        if (state.confirmResolve) {
          state.confirmResolve = null;
          resolve(false);
        }
      },
      { once: true },
    );
  });
}

async function submitRequest(event) {
  event.preventDefault();
  const payload = requestPayload();
  const pool = selectedPool();
  const confirmed = await confirmAction({
    title: `Submit ${payload.mode} request?`,
    copy:
      `${payload.jobName} will request ${payload.nodes} node(s), ${payload.gpus} GPU(s) each, ` +
      `through ${payload.account}/${payload.qos}. ` +
      (pool?.preemptMode === "cancel"
        ? "This QoS cancels the job when preempted."
        : "This QoS currently has preemption disabled."),
    acceptLabel: "Submit request",
  });
  if (!confirmed) return;
  const button = $("#submit-button");
  button.disabled = true;
  button.textContent = "Submitting…";
  try {
    const result = await postJson("/api/requests", payload);
    const identifier = result.result.chainName || result.result.jobId || "request";
    showMessage(`${identifier} submitted successfully.`);
    await refreshCore();
  } catch (error) {
    showMessage(error.message, true);
  } finally {
    button.disabled = false;
    button.textContent = "Submit request →";
  }
}

function renderEndTargets() {
  const select = $("#end-target");
  const previous = select.value;
  select.replaceChildren(element("option", "", "Select a session"));
  select.firstChild.value = "";
  for (const chain of state.status?.chains || []) {
    const option = element(
      "option",
      "",
      `Chain · ${chain.name} · ${chain.runningNode || "waiting"}`,
    );
    option.value = `chain:${chain.name}`;
    select.append(option);
  }
  for (const job of state.status?.jobs || []) {
    if (job.kind !== "normal") continue;
    const option = element("option", "", `Normal · ${job.id} · ${job.name} · ${job.state}`);
    option.value = `job:${job.id}`;
    select.append(option);
  }
  select.value = [...select.options].some((option) => option.value === previous) ? previous : "";
  updateEndImpact();
}

function selectedEndTarget() {
  const [kind, id] = $("#end-target").value.split(":", 2);
  if (kind === "chain") {
    return { kind, item: state.status?.chains?.find((chain) => chain.name === id) };
  }
  if (kind === "job") {
    return { kind, item: state.status?.jobs?.find((job) => job.id === id) };
  }
  return null;
}

function updateEndImpact() {
  const target = selectedEndTarget();
  const card = $("#end-impact");
  const button = $("#end-button");
  card.replaceChildren();
  if (!target?.item) {
    card.className = "impact-card empty-impact";
    card.textContent = "Select a running or pending session to see exactly what will end.";
    button.disabled = true;
    return;
  }
  card.className = "impact-card";
  if (target.kind === "chain") {
    const chain = target.item;
    card.append(
      element("strong", "", chain.name),
      element("span", "", `${chain.jobs.length} active link(s) · ${chain.runningNode || "no running node"}`),
      element("span", "", "Release writes the tombstone first, removes maintenance, then cancels queued links before the holder."),
    );
    button.textContent = "Release chain";
  } else {
    const job = target.item;
    card.append(
      element("strong", "", `Job ${job.id} · ${job.name}`),
      element("span", "", `${job.state} · ${job.nodeListOrReason}`),
      element("span", "", "This uses scancel for this one job only."),
    );
    button.textContent = "Cancel normal job";
  }
  button.disabled = false;
}

async function endSelected() {
  const target = selectedEndTarget();
  if (!target?.item) return;
  const identity = target.kind === "chain" ? target.item.name : target.item.id;
  const confirmed = await confirmAction({
    title: target.kind === "chain" ? `Release ${identity}?` : `Cancel job ${identity}?`,
    copy:
      target.kind === "chain"
        ? "Renewal will be disabled and every queued/running link will be cancelled. Stop host Docker containers first."
        : "The selected one-off job will be cancelled immediately.",
    acceptLabel: target.kind === "chain" ? "Release chain" : "Cancel job",
    typed: identity,
  });
  if (!confirmed) return;
  const button = $("#end-button");
  button.disabled = true;
  try {
    if (target.kind === "chain") {
      await postJson("/api/chains/action", {
        chainName: target.item.name,
        action: "release",
      });
      showMessage(`${target.item.name} released.`);
    } else {
      await postJson("/api/jobs/cancel", { jobId: target.item.id });
      showMessage(`Job ${target.item.id} cancelled.`);
    }
    await refreshCore();
  } catch (error) {
    showMessage(error.message, true);
  } finally {
    button.disabled = false;
  }
}

function renderChains() {
  const host = $("#chain-cards");
  host.replaceChildren();
  const chains = state.status?.chains || [];
  if (!chains.length) {
    host.append(element("div", "empty table-card", "No maintained chains are active."));
    return;
  }
  for (const chain of chains) {
    const card = element("article", "chain-card");
    const header = element("div", "chain-header");
    const heading = element("div");
    heading.append(
      element("h3", "", chain.name),
      element("p", "", `${chain.account || "unknown"} / ${chain.qos || "default QoS"}`),
    );
    const chainState = chain.runningNode ? "RUNNING" : chain.jobs.length ? "PENDING" : "DORMANT";
    header.append(heading, statusBadge(chainState));
    const meta = element("div", "chain-meta");
    const values = [
      ["Node", chain.runningNode || "waiting"],
      ["Resources", `${chain.gpus ?? "?"} GPU · ${chain.cpus || "auto"} CPU`],
      ["Queue", `${chain.pendingCount} / ${chain.chainTarget} successors`],
      ["Runway", formatSeconds(chain.potentialQueuedSeconds)],
      ["Wall", chain.timeLimit || "—"],
      ["Cron", chain.tended ? "tending" : "not tending"],
    ];
    for (const [label, value] of values) {
      const item = element("div");
      item.append(element("small", "", label), element("strong", "", value));
      meta.append(item);
    }
    const actions = element("div", "chain-actions");
    actions.append(
      actionButton(
        "Top up",
        () => chainAction(chain, "topup"),
        "ghost",
        CHAIN_ACTION_HELP.topup,
      ),
      actionButton(
        "Shrink",
        () => promptShrink(chain),
        "ghost",
        CHAIN_ACTION_HELP.shrink,
      ),
      actionButton(
        chain.tended ? "Untend" : "Tend",
        () => chainAction(chain, chain.tended ? "untend" : "tend"),
        "ghost",
        CHAIN_ACTION_HELP[chain.tended ? "untend" : "tend"],
      ),
      actionButton(
        "Clear backoff",
        () => chainAction(chain, "clear"),
        "ghost",
        CHAIN_ACTION_HELP.clear,
      ),
      actionButton(
        "Arm",
        () => chainAction(chain, "arm"),
        "ghost",
        CHAIN_ACTION_HELP.arm,
      ),
      actionButton("Copy shell", async () => {
        await copyText(chain.copyShellCommand);
        showMessage(`Shell command for ${chain.name} copied.`);
      }),
      actionButton(
        "Release",
        () => releaseChain(chain),
        "danger",
        CHAIN_ACTION_HELP.release,
      ),
    );
    card.append(header, meta, actions);
    host.append(card);
  }
}

async function chainAction(chain, action, value = null) {
  const effects = {
    topup: "This may submit additional dependent jobs.",
    arm: "This starts a detached one-shot waiter for the chain.",
    clear: "The next maintenance tick may retry immediately.",
    tend: "A five-minute local cron entry will be installed.",
    untend: "Jobs remain, but automatic renewal on this host stops.",
  };
  const confirmed = await confirmAction({
    title: `${action} ${chain.name}?`,
    copy: effects[action] || `Run ${action} for this chain?`,
    acceptLabel: action,
  });
  if (!confirmed) return;
  try {
    await postJson("/api/chains/action", { chainName: chain.name, action, value });
    showMessage(`${chain.name}: ${action} completed.`);
    await refreshCore();
  } catch (error) {
    showMessage(error.message, true);
  }
}

async function promptShrink(chain) {
  const value = await confirmAction({
    title: `Shrink ${chain.name}`,
    copy: `The newest queued links will be cancelled until this many successors remain. Current target: ${chain.chainTarget}.`,
    acceptLabel: "Shrink chain",
    inputLabel: "Successors to keep",
  });
  if (value === false) return;
  if (!/^\d+$/.test(value)) {
    showMessage("Depth must be a nonnegative integer.", true);
    return;
  }
  try {
    await postJson("/api/chains/action", {
      chainName: chain.name,
      action: "shrink",
      value: Number(value),
    });
    showMessage(`${chain.name} shrunk to ${value} successors.`);
    await refreshCore();
  } catch (error) {
    showMessage(error.message, true);
  }
}

async function releaseChain(chain) {
  $("#end-target").value = `chain:${chain.name}`;
  updateEndImpact();
  await endSelected();
}

function jobRow(job, { allowActions = true } = {}) {
  const row = element("tr", job.isMine ? "mine" : "");
  row.append(
    cell(primaryCell(job.id, `${job.name}${job.user ? ` · ${job.user}` : ""}`), "primary-cell"),
    cell(primaryCell(job.account, job.qos), "primary-cell"),
    cell(job.priority),
    cell(statusBadge(job.state)),
    cell(`${job.nodes || "?"}n · ${job.gres || "GPU ?"} · ${job.cpus ?? "?"}c`),
    cell(job.state === "RUNNING" ? `${job.elapsed} / ${job.timeLeft || "—"}` : job.waitDisplay),
    cell(job.nodeListOrReason),
  );
  if (allowActions) {
    const actions = element("div", "row-actions");
    if (job.state === "RUNNING") {
      actions.append(
        actionButton("Copy login", async () => {
          await copyText(`srun --jobid=${job.id} --overlap --pty bash -l`);
          showMessage(`Login command for ${job.id} copied.`);
        }),
      );
    }
    if (job.kind === "normal") {
      actions.append(
        actionButton("Cancel", () => {
          $("#end-target").value = `job:${job.id}`;
          updateEndImpact();
          void endSelected();
        }, "danger"),
      );
    } else {
      const chain = state.status?.chains?.find((item) => item.name === job.name);
      if (chain) {
        actions.append(
          actionButton(
            "Release chain",
            () => releaseChain(chain),
            "danger",
            CHAIN_ACTION_HELP.release,
          ),
        );
      }
    }
    row.append(cell(actions));
  }
  return row;
}

function renderStatusJobs() {
  const body = $("#status-jobs");
  body.replaceChildren();
  const jobs = state.status?.jobs || [];
  setText("#mine-count", `${jobs.length} job${jobs.length === 1 ? "" : "s"}`);
  if (!jobs.length) {
    const row = element("tr");
    const empty = cell("No jobs in your queue.", "empty");
    empty.colSpan = 8;
    row.append(empty);
    body.append(row);
    return;
  }
  for (const job of jobs) body.append(jobRow(job));
}

function renderQueueTable(hostSelector, jobs, allowActions = false) {
  const host = $(hostSelector);
  host.replaceChildren();
  const card = element("div", "table-card");
  const scroll = element("div", "table-scroll");
  const table = element("table");
  const head = element("thead");
  const headRow = element("tr");
  for (const label of ["Job", "Account / QoS", "Priority", "State", "Resources", "Time", "Node / reason"]) {
    headRow.append(element("th", "", label));
  }
  if (allowActions) headRow.append(element("th", "", "Action"));
  head.append(headRow);
  const body = element("tbody");
  if (!jobs.length) {
    const row = element("tr");
    const empty = cell("No jobs found.", "empty");
    empty.colSpan = allowActions ? 8 : 7;
    row.append(empty);
    body.append(row);
  } else {
    for (const job of jobs) body.append(jobRow(job, { allowActions }));
  }
  table.append(head, body);
  scroll.append(table);
  card.append(scroll);
  host.append(card);
}

function renderPools() {
  const body = $("#pool-rows");
  body.replaceChildren();
  for (const pool of state.pools?.pools || []) {
    const key = `${pool.account}|${pool.qos}`;
    const row = element("tr", `pool-row${state.expandedPools.has(key) ? " expanded" : ""}`);
    row.tabIndex = 0;
    row.setAttribute("role", "button");
    row.setAttribute("aria-expanded", String(state.expandedPools.has(key)));
    const usage = pool.nodeCap ? Math.min(100, Math.round((pool.usedNodes / pool.nodeCap) * 100)) : 0;
    const usageCell = element("div");
    usageCell.append(element("span", "", `${pool.usedNodes} / ${pool.nodeCap ?? "∞"}`));
    const bar = element("div", "usage-bar");
    const fill = element("i", usage >= 100 ? "full" : usage >= 80 ? "warning" : "");
    fill.style.width = `${usage}%`;
    bar.append(fill);
    usageCell.append(bar);
    row.append(
      cell(primaryCell(pool.account, pool.defaultQos ? "association default" : ""), "primary-cell"),
      cell(pool.qos),
      cell(pool.priority),
      cell(pool.preemptMode === "off" ? statusBadge("SAFE") : statusBadge(pool.preemptMode.toUpperCase())),
      cell(usageCell),
      cell(pool.queuedJobs),
      cell(`${pool.userRunning} running · ${pool.userPending} waiting`),
      cell(
        `wall ${pool.maxWallMinutes ? `${Math.round(pool.maxWallMinutes / 60)}h` : "∞"} · ` +
          `submit ${pool.maxSubmitPerUser ?? "∞"}`,
      ),
    );
    const togglePool = () => {
      if (state.expandedPools.has(key)) state.expandedPools.delete(key);
      else state.expandedPools.add(key);
      renderPools();
    };
    row.addEventListener("click", togglePool);
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        togglePool();
      }
    });
    body.append(row);
    if (state.expandedPools.has(key)) {
      const detailRow = element("tr", "detail-row");
      const detailCell = element("td");
      detailCell.colSpan = 8;
      const host = element("div", "pool-jobs");
      const nested = element("table");
      const nestedHead = element("thead");
      const nestedHeadRow = element("tr");
      for (const label of ["Position", "Job", "User", "State", "Priority", "Node / reason", "Submitted"]) {
        nestedHeadRow.append(element("th", "", label));
      }
      nestedHead.append(nestedHeadRow);
      const nestedBody = element("tbody");
      const sorted = [...pool.jobs].sort((a, b) => {
        if (a.state !== b.state) return a.state === "RUNNING" ? -1 : 1;
        return (b.priority || 0) - (a.priority || 0) || a.id.localeCompare(b.id, undefined, { numeric: true });
      });
      if (!sorted.length) {
        const emptyRow = element("tr");
        const empty = cell("No active jobs in this QoS.", "empty");
        empty.colSpan = 7;
        emptyRow.append(empty);
        nestedBody.append(emptyRow);
      }
      let pendingPosition = 0;
      for (const job of sorted) {
        const dependent = String(job.nodeListOrReason).includes("Dependency");
        if (job.state === "PENDING" && !dependent) pendingPosition += 1;
        const position =
          job.state === "RUNNING" ? "holding" : dependent ? "dependent" : pendingPosition;
        const nestedRow = element("tr", job.isMine ? "mine" : "");
        nestedRow.append(
          cell(position),
          cell(primaryCell(job.id, job.name), "primary-cell"),
          cell(`${job.user}${job.isMine ? " · you" : ""}`),
          cell(statusBadge(job.state)),
          cell(job.priority),
          cell(job.nodeListOrReason),
          cell(formatDate(job.submittedAt)),
        );
        nestedBody.append(nestedRow);
      }
      nested.append(nestedHead, nestedBody);
      host.append(nested);
      detailCell.append(host);
      detailRow.append(detailCell);
      body.append(detailRow);
    }
  }
}

function renderHistory() {
  const host = $("#request-history");
  host.replaceChildren();
  const requests = state.history?.requests || [];
  if (!requests.length) {
    host.append(element("div", "history-card", "No dashboard requests have been recorded yet."));
  } else {
    for (const record of requests.slice(0, 8)) {
      const request = record.request || {};
      const result = record.result || {};
      const card = element("article", "history-card");
      card.append(
        element("h3", "", request.jobName || result.chainName || result.jobId || "Request"),
        element("p", "", `${request.mode || "unknown"} · ${request.account || "auto"} / ${request.qos || "auto"}`),
        element("p", "", `${request.nodes || "?"} node · ${request.gpus || "?"} GPU · ${request.timeLimit || "?"}`),
        element("p", "", formatDate(record.submittedAt || record.recordedAt)),
      );
      host.append(card);
    }
  }
  const body = $("#history-jobs");
  body.replaceChildren();
  const jobs = state.history?.jobs || [];
  if (!jobs.length) {
    const row = element("tr");
    const empty = cell(state.history?.error || "No terminal jobs found.", "empty");
    empty.colSpan = 7;
    row.append(empty);
    body.append(row);
    return;
  }
  for (const job of jobs) {
    const row = element("tr");
    row.append(
      cell(primaryCell(job.id, job.name), "primary-cell"),
      cell(primaryCell(job.account, job.qos), "primary-cell"),
      cell(statusBadge(job.state)),
      cell(job.elapsed),
      cell(formatDate(job.startedAt)),
      cell(formatDate(job.endedAt)),
      cell(job.exitCode),
    );
    body.append(row);
  }
}

function renderDiagnostics() {
  const host = $("#diagnostic-cards");
  host.replaceChildren();
  const doctor = state.diagnostics || {};
  const checks = [
    ["Scheduler", doctor.scheduler?.answering, doctor.scheduler?.answering ? "Controller is answering" : "Scheduler unavailable"],
    ["Name service", doctor.nameService?.resolvesUser, doctor.nameService?.resolvesUser ? "User identity resolves" : "sbatch may refuse jobs"],
    ["Home storage", doctor.home?.writable, doctor.home?.writable ? "State directory takes writes" : "State cannot be persisted"],
    ["Cron", (doctor.cron?.tendedChains || 0) > 0, `${doctor.cron?.tendedChains || 0} locally tended chain(s)`],
  ];
  for (const [label, ok, copy] of checks) {
    const card = element("article", "diagnostic-card");
    card.append(
      element("h3", "", label),
      element("div", `diagnostic-value${ok ? "" : " bad"}`, ok ? "Healthy" : "Attention"),
      element("p", "", copy),
    );
    host.append(card);
  }
  const chainSelect = $("#log-chain");
  const current = chainSelect.value;
  chainSelect.replaceChildren(element("option", "", "Select a chain"));
  chainSelect.firstChild.value = "";
  for (const chain of doctor.chains || []) {
    const option = element("option", "", chain.name);
    option.value = chain.name;
    chainSelect.append(option);
  }
  chainSelect.value = [...chainSelect.options].some((option) => option.value === current) ? current : "";
  if (doctor.alarms?.length) {
    $("#log-output").textContent = `Recent alarms:\n${doctor.alarms.join("\n")}`;
  }
}

async function loadSqa(force = false) {
  if (state.sqa && !force) {
    renderSqa();
    return;
  }
  try {
    state.sqa = await fetchJson("/api/queue?scope=all");
    renderSqa();
  } catch (error) {
    showMessage(error.message, true);
  }
}

function renderSqa() {
  const search = $("#sqa-search").value.trim().toLowerCase();
  const wantedState = $("#sqa-state").value;
  const jobs = (state.sqa?.jobs || []).filter((job) => {
    if (wantedState && job.state !== wantedState) return false;
    if (!search) return true;
    return [job.id, job.name, job.user, job.account, job.qos, job.nodeListOrReason]
      .join(" ")
      .toLowerCase()
      .includes(search);
  });
  renderQueueTable("#sqa-table", jobs);
}

async function loadHistory(force = false) {
  if (state.history && !force) return renderHistory();
  try {
    state.history = await fetchJson("/api/history");
    renderHistory();
  } catch (error) {
    showMessage(error.message, true);
  }
}

async function loadDiagnostics(force = false) {
  if (state.diagnostics && !force) return renderDiagnostics();
  try {
    state.diagnostics = await fetchJson("/api/diagnostics");
    renderDiagnostics();
  } catch (error) {
    showMessage(error.message, true);
  }
}

async function loadLog() {
  const chain = $("#log-chain").value;
  if (!chain) {
    showMessage("Choose a chain first.", true);
    return;
  }
  try {
    const payload = await fetchJson(
      `/api/logs?chain=${encodeURIComponent(chain)}&kind=${encodeURIComponent($("#log-kind").value)}`,
    );
    $("#log-output").textContent = payload.lines.length ? payload.lines.join("\n") : "No log lines found.";
  } catch (error) {
    showMessage(error.message, true);
  }
}

function switchTab(name) {
  state.currentTab = name;
  $$(".tab").forEach((tab) => tab.classList.toggle("active", tab.dataset.tab === name));
  $$(".tab-panel").forEach((panel) => panel.classList.toggle("active", panel.dataset.panel === name));
  if (name === "sq") renderQueueTable("#sq-table", state.status?.jobs || [], true);
  if (name === "sqa") void loadSqa();
  if (name === "history") void loadHistory();
  if (name === "diagnostics") void loadDiagnostics();
}

async function refreshCore() {
  if (state.refreshing) return;
  state.refreshing = true;
  $("#refresh-button").disabled = true;
  $("#health-dot").className = "health-dot loading";
  setText("#health-label", "Refreshing");
  try {
    const [status, pools, capabilities] = await Promise.all([
      fetchJson("/api/status"),
      fetchJson("/api/pools"),
      state.capabilities ? Promise.resolve(state.capabilities) : fetchJson("/api/capabilities"),
    ]);
    state.status = status;
    state.pools = pools;
    state.capabilities = capabilities;
    renderCapabilities();
    renderSummary();
    renderPoolSelectors();
    renderChains();
    renderStatusJobs();
    renderPools();
    renderEndTargets();
    if (state.currentTab === "sq") renderQueueTable("#sq-table", status.jobs, true);
    setText("#health-label", Object.keys(status.errors || {}).length ? "Partial data" : "Scheduler online");
    setText("#updated-at", `Updated ${new Date(status.updatedAt).toLocaleTimeString()}`);
    $("#health-dot").className = Object.keys(status.errors || {}).length ? "health-dot error" : "health-dot";
  } catch (error) {
    $("#health-dot").className = "health-dot error";
    setText("#health-label", "Refresh failed");
    setText("#updated-at", error.message);
    showMessage(error.message, true);
  } finally {
    state.refreshing = false;
    $("#refresh-button").disabled = false;
  }
}

function initialize() {
  $("#confirm-accept").addEventListener("click", () => state.confirmResolve?.());
  $("#confirm-cancel").addEventListener("click", () => $("#confirm-dialog").close());
  $("#request-form").addEventListener("submit", submitRequest);
  $('input[name="mode"][value="chain"]').addEventListener("change", updateRequestMode);
  $('input[name="mode"][value="normal"]').addEventListener("change", updateRequestMode);
  $("#account").addEventListener("change", renderQosOptions);
  $("#qos").addEventListener("change", updateRequestPreview);
  $("#gpus").addEventListener("change", () => {
    $("#exclusive").checked = $("#gpus").value === "8";
    updateRequestPreview();
  });
  for (const selector of ["#cpus", "#nodes", "#node", "#time-limit", "#chain-depth", "#runway-hours", "#exclusive"]) {
    $(selector).addEventListener("input", updateRequestPreview);
    $(selector).addEventListener("change", updateRequestPreview);
  }
  $("#strategy").addEventListener("change", () => {
    $("#adopt-field").classList.toggle("hidden", $("#strategy").value !== "adopt");
    const race = $("#strategy").value === "race";
    $("#account").disabled = race;
    $("#qos").disabled = race;
    updateRequestPreview();
  });
  $("#end-target").addEventListener("change", updateEndImpact);
  $("#end-button").addEventListener("click", endSelected);
  $("#refresh-button").addEventListener("click", refreshCore);
  $$(".tab").forEach((tab) => tab.addEventListener("click", () => switchTab(tab.dataset.tab)));
  $("#sqa-search").addEventListener("input", renderSqa);
  $("#sqa-state").addEventListener("change", renderSqa);
  $("#sqa-refresh").addEventListener("click", () => loadSqa(true));
  $("#diagnostics-refresh").addEventListener("click", () => loadDiagnostics(true));
  $("#log-load").addEventListener("click", loadLog);

  updateRequestMode();
  void refreshCore();
  setInterval(refreshCore, 30_000);
}

globalThis.__spurDashboardTestApi = {
  state,
  postJson,
  requestPayload,
  selectedEndTarget,
  updateEndImpact,
  endSelected,
  runChainAction: chainAction,
};

if (!globalThis.__SPUR_DASHBOARD_TEST__) initialize();
