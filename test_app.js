const assert = require("node:assert/strict");
const fs = require("node:fs");
const test = require("node:test");
const vm = require("node:vm");

function response(status, data) {
  return {
    status,
    ok: status >= 200 && status < 300,
    async json() {
      return data;
    },
  };
}

class MockNode {
  constructor() {
    this.children = [];
    this.className = "";
    this.checked = false;
    this.content = "";
    this.dataset = {};
    this.disabled = false;
    this.listeners = {};
    this.open = false;
    this.options = this.children;
    this.style = {};
    this.textContent = "";
    this.value = "";
    this.classList = {
      toggle: (name, force) => {
        const names = new Set(this.className.split(/\s+/).filter(Boolean));
        const enabled = force === undefined ? !names.has(name) : force;
        if (enabled) names.add(name);
        else names.delete(name);
        this.className = [...names].join(" ");
      },
      contains: (name) => this.className.split(/\s+/).includes(name),
    };
  }

  append(...children) {
    this.children.push(...children);
    this.options = this.children;
    this.firstChild = this.children[0] || this.firstChild || new MockNode();
  }

  addEventListener(name, handler) {
    (this.listeners[name] ||= []).push(handler);
  }

  close() {
    this.open = false;
    for (const handler of this.listeners.close || []) handler();
  }

  focus() {}
  remove() {}
  replaceChildren(...children) {
    this.children = [...children];
    this.options = this.children;
    this.firstChild = this.children[0] || new MockNode();
  }
  select() {}
  setAttribute() {}
  showModal() {
    this.open = true;
  }
  querySelector() {
    return new MockNode();
  }
}

function statusPayload() {
  return {
    updatedAt: "2026-09-24T00:00:00Z",
    summary: { runningJobs: 1, pendingJobs: 1, runningNodes: 1, chains: 1 },
    chains: [],
    jobs: [],
    errors: {},
  };
}

function poolsPayload() {
  return {
    pools: [],
    bestPool: null,
  };
}

function loadApp(fetch) {
  const elements = new Map();
  const get = (selector) => {
    if (!elements.has(selector)) {
      const node = new MockNode();
      node.firstChild = new MockNode();
      elements.set(selector, node);
    }
    return elements.get(selector);
  };
  get('meta[name="csrf-token"]').content = "stale-token";
  const context = {
    __SPUR_DASHBOARD_TEST__: true,
    Node: MockNode,
    clearTimeout() {},
    document: {
      body: new MockNode(),
      createElement() {
        return new MockNode();
      },
      execCommand() {
        return true;
      },
      querySelector: get,
      querySelectorAll() {
        return [];
      },
    },
    fetch,
    globalThis: null,
    navigator: {},
    setInterval() {},
    setTimeout() {
      return 1;
    },
  };
  context.globalThis = context;
  vm.createContext(context);
  vm.runInContext(fs.readFileSync("static/app.js", "utf8"), context);
  return { api: context.__spurDashboardTestApi, element: get };
}

test("POST refreshes the CSRF token and sends JSON", async () => {
  const calls = [];
  const { api } = loadApp(async (path, options = {}) => {
    calls.push({ path, options });
    if (path === "/api/csrf-token") return response(200, { token: "fresh-token" });
    return response(201, { ok: true });
  });
  const result = await api.postJson("/api/requests", { mode: "normal" });
  assert.equal(result.ok, true);
  assert.equal(calls[1].options.headers["X-CSRF-Token"], "fresh-token");
  assert.equal(calls[1].options.body, JSON.stringify({ mode: "normal" }));
});

test("request payload keeps chain and normal semantics explicit", () => {
  const { api, element } = loadApp(async () => response(200, {}));
  element('input[name="mode"]:checked').value = "chain";
  element("#strategy").value = "start";
  element("#job-name").value = "demo";
  element("#time-limit").value = "12:00:00";
  element("#gpus").value = "4";
  element("#cpus").value = "";
  element("#nodes").value = "1";
  element("#account").value = "amd-brain-models";
  element("#qos").value = "amd-brain-models-qos";
  element("#node").value = "";
  element("#exclusive").checked = false;
  element("#chain-depth").value = "3";
  element("#runway-hours").value = "0";
  element("#expiry-hours").value = "0";
  element("#repin").checked = true;
  element("#any-qos").checked = false;
  element("#accept-preemption").checked = false;
  element("#adopt-job-id").value = "";
  const payload = api.requestPayload();
  assert.equal(payload.mode, "chain");
  assert.equal(payload.chainDepth, 3);
  assert.equal(payload.cpus, null);
  assert.equal(payload.exclusive, false);
});

test("end target links a chain to release and a normal job to scancel path", () => {
  const { api, element } = loadApp(async () => response(200, {}));
  api.state.status = {
    chains: [{ name: "hold-demo", jobs: [], runningNode: "node026" }],
    jobs: [{ id: "123", kind: "normal", name: "demo", state: "RUNNING" }],
  };
  element("#end-target").value = "chain:hold-demo";
  assert.equal(api.selectedEndTarget().kind, "chain");
  assert.equal(api.selectedEndTarget().item.name, "hold-demo");
  element("#end-target").value = "job:123";
  assert.equal(api.selectedEndTarget().kind, "job");
  assert.equal(api.selectedEndTarget().item.id, "123");
});

test("source keeps destructive chain and normal endpoints separate", () => {
  const source = fs.readFileSync("static/app.js", "utf8");
  assert.match(source, /postJson\("\/api\/chains\/action"/);
  assert.match(source, /action: "release"/);
  assert.match(source, /postJson\("\/api\/jobs\/cancel"/);
  assert.match(source, /async function cancelSingleJob\(job\)/);
  assert.doesNotMatch(source, /action: "tick"/);
});
