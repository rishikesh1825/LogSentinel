/* LogSentinel frontend. Talks to the backend ONLY through REST calls to API_BASE_URL. */
"use strict";

const CFG = window.LOGSENTINEL_CONFIG || {};
const API = (CFG.API_BASE_URL || "").replace(/\/$/, "");
const MAX_BYTES = CFG.MAX_INPUT_BYTES || 5 * 1024 * 1024;

// Example logs: one healthy block and one block with failures (HDFS format).
const EXAMPLE_LOGS = [
  "081109 203518 35 INFO dfs.FSNamesystem: BLOCK* NameSystem.allocateBlock: /mnt/hadoop/mapred/system/job_200811092030_0001/job.jar. blk_-1608999687919862906",
  "081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.19.102:54106 dest: /10.250.19.102:50010",
  "081109 203519 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.10.6:40524 dest: /10.250.10.6:50010",
  "081109 203519 145 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.14.224:42420 dest: /10.250.14.224:50010",
  "081109 203519 145 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_-1608999687919862906 terminating",
  "081109 203519 145 INFO dfs.DataNode$PacketResponder: Received block blk_-1608999687919862906 of size 91178 from /10.250.10.6",
  "081109 203519 147 INFO dfs.DataNode$PacketResponder: PacketResponder 2 for block blk_-1608999687919862906 terminating",
  "081109 203519 147 INFO dfs.DataNode$PacketResponder: Received block blk_-1608999687919862906 of size 91178 from /10.250.19.102",
  "081109 203520 149 INFO dfs.DataNode$PacketResponder: PacketResponder 0 for block blk_-1608999687919862906 terminating",
  "081109 203520 149 INFO dfs.DataNode$PacketResponder: Received block blk_-1608999687919862906 of size 91178 from /10.250.14.224",
  "081109 203520 26 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.250.10.6:50010 is added to blk_-1608999687919862906 size 91178",
  "081109 203520 30 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.250.19.102:50010 is added to blk_-1608999687919862906 size 91178",
  "081109 203520 31 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.250.14.224:50010 is added to blk_-1608999687919862906 size 91178",
  "081109 212512 35 INFO dfs.FSNamesystem: BLOCK* NameSystem.allocateBlock: /user/root/rand/_temporary/_task_200811092030_0002_m_000296_0/part-00296. blk_-2918118818249673980",
  "081109 212512 1001 INFO dfs.DataNode$DataXceiver: Receiving block blk_-2918118818249673980 src: /10.251.90.64:56743 dest: /10.251.90.64:50010",
  "081109 212512 1002 INFO dfs.DataNode$DataXceiver: Receiving block blk_-2918118818249673980 src: /10.251.30.6:49128 dest: /10.251.30.6:50010",
  "081109 212519 1003 INFO dfs.DataNode$DataXceiver: writeBlock blk_-2918118818249673980 received exception java.io.IOException: Could not read from stream",
  "081109 212519 1004 INFO dfs.DataNode$PacketResponder: PacketResponder 2 for block blk_-2918118818249673980 Interrupted.",
  "081109 212520 1005 WARN dfs.DataNode$DataXceiver: 10.251.30.6:50010:Got exception while serving blk_-2918118818249673980 to /10.251.90.64:",
  "081109 212520 1006 INFO dfs.DataNode$DataXceiver: Exception in receiveBlock for block blk_-2918118818249673980 java.io.IOException: Connection reset by peer",
].join("\n");

// --------------------------------------------------------------- helpers
const $ = (sel) => document.querySelector(sel);
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (c) => (
  { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const pct = (x, digits = 1) => `${(100 * (x || 0)).toFixed(digits)}%`;
const num = (x) => (x === null || x === undefined ? "–" : Number(x).toLocaleString());

function toast(message, isError = false) {
  const el = $("#toast");
  el.textContent = message;
  el.className = `toast${isError ? " error" : ""}`;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => el.classList.add("hidden"), isError ? 8000 : 3500);
}

async function api(path, options = {}) {
  const headers = { "X-API-Key": CFG.API_KEY || "", ...(options.headers || {}) };
  let response;
  try {
    response = await fetch(`${API}${path}`, { ...options, headers });
  } catch (err) {
    throw new Error("Cannot reach the LogSentinel server. Check that it is running.");
  }
  let body = null;
  try { body = await response.json(); } catch (err) { body = null; }
  if (!response.ok) {
    const message = (body && (body.error || body.detail)) || `Request failed (${response.status})`;
    throw new Error(message);
  }
  return body;
}

function setBusy(button, busy, label) {
  button.disabled = busy;
  button.innerHTML = busy ? `<span class="spinner"></span>${esc(label)}` : esc(button.dataset.label);
}

// ------------------------------------------------------------------ tabs
document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t === tab));
    document.querySelectorAll(".panel").forEach((p) => p.classList.toggle("active", p.id === `tab-${tab.dataset.tab}`));
    if (tab.dataset.tab === "pipeline") loadPipeline();
    if (tab.dataset.tab === "monitoring") loadGrafana();
  });
});

// --------------------------------------------------------------- analyze
const input = $("#log-input");
const fileInput = $("#file-input");
const analyzeBtn = $("#btn-analyze");
analyzeBtn.dataset.label = "Analyze";
let selectedFile = null;
let lastSessions = [];

function updateInputStats() {
  if (selectedFile) {
    $("#input-stats").textContent = `File selected: ${(selectedFile.size / 1024).toFixed(1)} KB`;
    return;
  }
  const lines = input.value.split("\n").filter((l) => l.trim()).length;
  $("#input-stats").textContent = lines ? `${lines.toLocaleString()} line(s) ready to analyze` : "";
}

input.addEventListener("input", () => { selectedFile = null; fileInput.value = ""; $("#file-name").textContent = ""; updateInputStats(); });
fileInput.addEventListener("change", () => {
  selectedFile = fileInput.files[0] || null;
  if (selectedFile && selectedFile.size > MAX_BYTES) {
    toast(`That file is ${(selectedFile.size / 1048576).toFixed(1)} MB. The limit is ${MAX_BYTES / 1048576} MB.`, true);
    selectedFile = null; fileInput.value = "";
  }
  $("#file-name").textContent = selectedFile ? selectedFile.name : "";
  if (selectedFile) input.value = "";
  updateInputStats();
});
$("#btn-sample").addEventListener("click", () => { input.value = EXAMPLE_LOGS; selectedFile = null; fileInput.value = ""; $("#file-name").textContent = ""; updateInputStats(); });
$("#btn-clear").addEventListener("click", () => {
  input.value = ""; selectedFile = null; fileInput.value = ""; $("#file-name").textContent = "";
  $("#results").classList.add("hidden"); updateInputStats();
});
$("#only-anomalies").addEventListener("change", () => renderSessions(lastSessions));

analyzeBtn.addEventListener("click", async () => {
  if (!selectedFile && !input.value.trim()) { toast("Paste some log lines or choose a file first.", true); return; }
  if (!selectedFile && new Blob([input.value]).size > MAX_BYTES) { toast("The pasted text is larger than 5 MB. Please use a smaller part.", true); return; }
  setBusy(analyzeBtn, true, "Analyzing…");
  try {
    let result;
    if (selectedFile) {
      const form = new FormData();
      form.append("file", selectedFile);
      result = await api("/api/v1/predict/file", { method: "POST", body: form });
    } else {
      result = await api("/api/v1/predict", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ logs: input.value }),
      });
    }
    renderResults(result);
  } catch (err) {
    toast(err.message, true);
  } finally {
    setBusy(analyzeBtn, false);
  }
});

function renderResults(result) {
  const s = result.summary;
  $("#results").classList.remove("hidden");
  $("#summary").innerHTML = [
    ["Lines read", num(s.total_lines)],
    ["Lines skipped", num(s.skipped_lines), s.skipped_lines ? "warn" : ""],
    ["Sessions (blocks)", num(s.sessions)],
    ["Problems found", num(s.anomalies), s.anomalies ? "danger" : ""],
    ["Borderline", num((result.sessions || []).filter((x) => verdict(x) === "borderline").length), "warn"],
    ["Problem rate", pct(s.anomaly_rate)],
    ["Unseen events", `${num(s.unseen_lines)} (${pct(s.unseen_ratio)})`, s.unseen_lines ? "warn" : ""],
    ["Time taken", `${num(s.latency_ms)} ms`],
  ].map(([label, value, cls]) => `<div class="stat ${cls || ""}"><div class="label">${esc(label)}</div><div class="value">${esc(value)}</div></div>`).join("");
  lastSessions = result.sessions || [];
  renderSessions(lastSessions);
  $("#detail").innerHTML = lastSessions.length
    ? '<p class="hint">Click a session on the left to see the log events behind its verdict.</p>'
    : '<p class="hint">No sessions were found.</p>';
  if (lastSessions.length && verdict(lastSessions[0]) !== "normal") showDetail(lastSessions[0]);
  if (s.skipped_lines) toast(`${s.skipped_lines} line(s) were skipped because they are not HDFS log lines with a block ID.`);
}

function riskColor(risk) { return risk >= 0.7 ? "var(--danger)" : risk >= 0.4 ? "var(--warn)" : "var(--ok)"; }

// Three verdicts: the model's decision (score >= threshold) is "Problem"; sessions just
// below the strict threshold but with risk >= 50% are "Borderline" so a human looks at them.
const BORDERLINE_RISK = 0.5;
function verdict(s) {
  if (s.is_anomaly) return "problem";
  return s.risk >= BORDERLINE_RISK ? "borderline" : "normal";
}
function verdictBadge(s) {
  return {
    problem: '<span class="badge badge-danger">Problem</span>',
    borderline: '<span class="badge badge-warn">Borderline</span>',
    normal: '<span class="badge badge-ok">Normal</span>',
  }[verdict(s)];
}

function renderSessions(sessions) {
  const onlyAnomalies = $("#only-anomalies").checked;
  const rows = sessions.filter((s) => !onlyAnomalies || verdict(s) !== "normal");
  const tbody = $("#sessions-table tbody");
  if (!rows.length) { tbody.innerHTML = '<tr><td colspan="4" class="hint">Nothing to show.</td></tr>'; return; }
  tbody.innerHTML = rows.map((s, i) => `
    <tr data-index="${i}" class="${verdict(s) === "normal" ? "" : verdict(s)}">
      <td class="mono">${esc(s.block_id)}</td>
      <td>${num(s.n_lines)}</td>
      <td><span class="riskbar"><span style="width:${Math.round(100 * s.risk)}%;background:${riskColor(s.risk)}"></span></span>${pct(s.risk, 0)}</td>
      <td>${verdictBadge(s)}</td>
    </tr>`).join("");
  tbody.querySelectorAll("tr").forEach((tr) => tr.addEventListener("click", () => {
    tbody.querySelectorAll("tr").forEach((r) => r.classList.remove("selected"));
    tr.classList.add("selected");
    showDetail(rows[Number(tr.dataset.index)]);
  }));
}

function plainExplanation(s) {
  const events = (s.top_events || []).filter((e) => e.contribution > 0);
  const v = verdict(s);
  if (v === "normal") {
    return "This block's events look like a normal, healthy HDFS block.";
  }
  if (!events.length) return "This block's overall pattern of events differs from healthy blocks.";
  const top = events[0];
  const times = top.count === 1 ? "once" : `${top.count} times`;
  let text = v === "problem"
    ? `Flagged mainly because the event “${top.template}” appeared ${times}.`
    : `Borderline: just below the problem threshold (risk ${pct(s.risk, 0)}), mainly because the event “${top.template}” appeared ${times}. Worth checking.`;
  if (events.length > 1) text += ` Other contributing events: ${events.slice(1, 3).map((e) => `“${e.template}”`).join(", ")}.`;
  if (s.n_unseen) text += ` It also contains ${s.n_unseen} event(s) never seen during training.`;
  return text;
}

function showDetail(s) {
  const events = s.top_events || [];
  const maxAbs = Math.max(...events.map((e) => Math.abs(e.contribution)), 1e-9);
  $("#detail").innerHTML = `
    <p><span class="mono">${esc(s.block_id)}</span> –
      ${verdictBadge(s)}
      <span class="hint">risk ${pct(s.risk, 0)} · ${num(s.n_lines)} lines · ${num(s.n_unseen)} unseen</span></p>
    <p class="explain">${esc(plainExplanation(s))}</p>
    <h3 class="hint">Events that influenced the verdict (red = towards “problem”)</h3>
    ${events.map((e) => `
      <div class="event">
        <div class="text">${esc(e.template)} <span class="hint">× ${num(e.count)}</span></div>
        <div class="bar ${e.contribution >= 0 ? "pos" : "neg"}" style="width:${Math.max(4, Math.round(100 * Math.abs(e.contribution) / maxAbs))}%"></div>
      </div>`).join("") || '<p class="hint">No event details available.</p>'}
    <div class="row">
      <span class="hint">Is this verdict wrong?</span>
      <button class="btn btn-secondary" id="fb-normal" type="button">It is normal</button>
      <button class="btn btn-secondary" id="fb-anomaly" type="button">It is a problem</button>
    </div>`;
  $("#fb-normal").addEventListener("click", () => sendFeedback(s.block_id, "Normal"));
  $("#fb-anomaly").addEventListener("click", () => sendFeedback(s.block_id, "Anomaly"));
}

async function sendFeedback(blockId, label) {
  try {
    await api("/api/v1/feedback", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ block_id: blockId, correct_label: label }) });
    toast("Thanks - your feedback was saved and will be used for retraining.");
  } catch (err) { toast(err.message, true); }
}

// -------------------------------------------------------- pipeline console
const STAGE_HIGHLIGHTS = {
  validate: (m) => m["reports/validation.json"] && [["checks passed", m["reports/validation.json"].all_passed ? "all" : "NO"], ["lines", num(m["reports/validation.json"].log?.total_lines)]],
  parse: (m) => m["reports/parse_stats.json"] && [["lines/s", num(m["reports/parse_stats.json"].lines_per_second)], ["seconds", m["reports/parse_stats.json"].parse_seconds]],
  split: (m) => m["reports/split_stats.json"] && [["train blocks", num(m["reports/split_stats.json"].train?.blocks)], ["test blocks", num(m["reports/split_stats.json"].test?.blocks)]],
  mine_templates: (m) => m["reports/template_stats.json"] && [["templates", m["reports/template_stats.json"].n_templates], ["lines/s", num(m["reports/template_stats.json"].lines_per_second)]],
  match: (m) => m["reports/match_stats.json"] && [["rows/s", num(m["reports/match_stats.json"].rows_per_second)], ["test unseen", pct(m["reports/match_stats.json"].by_split?.test?.unseen_ratio, 3)]],
  sessionize: (m) => m["reports/session_stats.json"] && [["sessions", num(m["reports/session_stats.json"].total_blocks)], ["seconds", m["reports/session_stats.json"].sessionize_seconds]],
  featurize: (m) => m["reports/featurize_stats.json"] && [["features", m["reports/featurize_stats.json"].n_features], ["seconds", m["reports/featurize_stats.json"].featurize_seconds]],
  train: (m) => { const r = m["reports/train_metrics.json"]; return r && [["served", r.served_model], ["test F1", r[r.served_model]?.test?.f1], ["test PR-AUC", r[r.served_model]?.test?.pr_auc]]; },
  package: (m) => { const r = m["reports/package.json"]; return r && [["version", r.new_version], ["alias", r.alias]]; },
};

async function loadPipeline() {
  const refresh = $("#btn-refresh");
  refresh.dataset.label = "Refresh";
  setBusy(refresh, true, "Loading…");
  try {
    const [status, runs, errors, bench] = await Promise.all([
      api("/api/v1/pipeline/status"), api("/api/v1/pipeline/runs?limit=30"),
      api("/api/v1/pipeline/errors"), api("/api/v1/pipeline/benchmark"),
    ]);
    renderStages(status.stages || []);
    renderRuns(runs.stage_runs || [], runs.mlflow_runs || []);
    renderErrors(errors);
    renderBenchmark(bench.results || []);
    loadRetrainStatus();
  } catch (err) {
    toast(err.message, true);
  } finally {
    setBusy(refresh, false);
  }
}

function renderStages(stages) {
  $("#stages").innerHTML = stages.map((s) => {
    const highlights = (STAGE_HIGHLIGHTS[s.stage] && STAGE_HIGHLIGHTS[s.stage](s.metrics || {})) || [];
    const last = s.last_run;
    const badge = { ok: "badge-ok", failed: "badge-danger", never_run: "badge-muted" }[s.status] || "badge-muted";
    return `<div class="stage ${esc(s.status)}">
      <h3>${esc(s.stage)} <span class="badge ${badge}">${esc(s.status.replace("_", " "))}</span></h3>
      ${last ? `<div class="kv">last run ${esc(last.finished_at)} · ${esc(last.duration_seconds)} s</div>` : ""}
      ${highlights.map(([k, v]) => `<div class="kv">${esc(k)}: <b>${esc(v)}</b></div>`).join("")}
    </div>`;
  }).join("") || '<p class="hint">No pipeline information yet.</p>';
}

function renderRuns(stageRuns, mlflowRuns) {
  $("#runs-table tbody").innerHTML = stageRuns.map((r) => `<tr>
      <td>${esc(r.stage)}</td>
      <td>${r.success ? '<span class="badge badge-ok">success</span>' : '<span class="badge badge-danger">failed</span>'}</td>
      <td>${esc(r.duration_seconds)} s</td><td>${r.lines_per_second ? num(Math.round(r.lines_per_second)) + "/s" : "–"}</td>
      <td>${esc(r.finished_at)}</td></tr>`).join("") || '<tr><td colspan="5" class="hint">No runs recorded yet.</td></tr>';
  $("#mlflow-table tbody").innerHTML = mlflowRuns.map((r) => `<tr>
      <td class="mono">${esc(r.run_id.slice(0, 8))}</td>
      <td>${r.status === "FINISHED" ? '<span class="badge badge-ok">finished</span>' : `<span class="badge ${r.status === "FAILED" ? "badge-danger" : "badge-warn"}">${esc(r.status.toLowerCase())}</span>`}</td>
      <td class="mono">${esc(r.git_commit)}</td>
      <td>${r.metrics.lr_test_f1 !== undefined ? r.metrics.lr_test_f1.toFixed(4) : "–"}</td>
      <td>${r.metrics.iforest_test_f1 !== undefined ? r.metrics.iforest_test_f1.toFixed(4) : "–"}</td></tr>`).join("") || '<tr><td colspan="5" class="hint">No MLflow runs found.</td></tr>';
}

function renderErrors(errors) {
  const items = [
    ...(errors.failed_stages || []).map((r) => `Stage <b>${esc(r.stage)}</b> failed at ${esc(r.finished_at)} (exit code ${esc(r.returncode)})`),
    ...(errors.failed_mlflow_runs || []).map((r) => `MLflow run <b>${esc(r.run_id.slice(0, 8))}</b> failed`),
    ...(errors.log_errors || []).slice(0, 15).map((line) => esc(line)),
  ];
  $("#errors").innerHTML = items.length
    ? items.map((t) => `<div class="error-item">${t}</div>`).join("")
    : '<p><span class="badge badge-ok">No errors</span> <span class="hint">No failed stages, failed runs or error log lines.</span></p>';
}

function renderBenchmark(results) {
  if (!results.length) { $("#benchmark").innerHTML = '<p class="hint">Run <code>python -m src.parsing.benchmark</code> to fill this in.</p>'; return; }
  const max = Math.max(...results.map((r) => r.lines_per_second));
  $("#benchmark").innerHTML = ["parse", "match"].map((stage) => `
    <h3 class="hint">${stage} (lines per second)</h3>
    ${results.filter((r) => r.stage === stage).map((r) => `
      <div class="bench-row"><span>${r.cores} core${r.cores > 1 ? "s" : ""}</span>
        <div class="bench-bar ${stage}" style="width:${Math.round(100 * r.lines_per_second / max)}%"></div>
        <span>${num(r.lines_per_second)} <span class="hint">×${r.speedup}</span></span></div>`).join("")}`).join("");
}

async function loadRetrainStatus() {
  try {
    const r = await api("/api/v1/retrain/requests");
    $("#retrain-status").innerHTML = `<p>Retraining requests – pending: <b>${r.pending.length}</b>, processed: <b>${r.processed.length}</b></p>`;
  } catch (err) { $("#retrain-status").textContent = ""; }
}

const retrainBtn = $("#btn-retrain");
retrainBtn.dataset.label = "Request retraining";
retrainBtn.addEventListener("click", async () => {
  setBusy(retrainBtn, true, "Sending…");
  try {
    const r = await api("/api/v1/retrain", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ reason: "manual request from pipeline console" }) });
    toast(`Retraining queued (${r.request}).`);
    loadRetrainStatus();
  } catch (err) { toast(err.message, true); } finally { setBusy(retrainBtn, false); }
});
$("#btn-refresh").addEventListener("click", loadPipeline);

// ------------------------------------------------------------- monitoring
function loadGrafana() {
  const frame = $("#grafana-frame");
  const url = `${CFG.GRAFANA_URL}/d/logsentinel/logsentinel?orgId=1&refresh=5s&kiosk`;
  if (!frame.src) frame.src = url;
  $("#grafana-link").href = `${CFG.GRAFANA_URL}/d/logsentinel/logsentinel`;
}

// ------------------------------------------------------------------ start
(async function init() {
  $("#mlflow-link").href = CFG.MLFLOW_URL || "#";
  try {
    const info = await api("/api/v1/model/info");
    const badge = $("#model-badge");
    if (info.available) {
      badge.textContent = `model: ${info.name} v${info.version} (${info.alias})`;
      badge.className = "badge badge-ok";
    } else {
      badge.textContent = "model: not available";
      badge.className = "badge badge-danger";
    }
  } catch (err) {
    $("#model-badge").textContent = "server not reachable";
    $("#model-badge").className = "badge badge-danger";
  }
})();
