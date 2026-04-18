(function () {
  "use strict";

  const STORAGE_KEY = "southlake_csv_session_v1";
  const STEPS = [
    { id: "input", name: "Input data", desc: "Upload your CSV" },
    { id: "inspect", name: "Data inspection", desc: "Quality & summary" },
    { id: "synthetic", name: "Synthetic data", desc: "Coming next" },
    { id: "review", name: "Review", desc: "Coming next" },
  ];

  let state = {
    step: 0,
    fileName: "",
    headers: [],
    rows: [],
    rawText: "",
    issues: [],
    charts: [],
  };

  const els = {};

  /** Set in initApiBase(): FastAPI origin, or explicit meta URL, or default 127.0.0.1:8765. */
  let resolvedApiBase = "http://127.0.0.1:8765";

  /**
   * If this page is served by FastAPI, use same origin. Otherwise (Live Preview, http.server,
   * file://) POST must go to a real API host — default http://127.0.0.1:8765 or meta api-base URL.
   */
  async function initApiBase() {
    const meta = document.querySelector('meta[name="api-base"]');
    const raw = meta ? meta.getAttribute("content") : "";
    const trimmed = (raw || "").trim();
    const explicitUrl = trimmed && trimmed !== "__AUTO__";
    const proto = window.location.protocol || "";

    if (proto === "http:" || proto === "https:") {
      try {
        const r = await fetch(`${window.location.origin}/api/health`, { method: "GET" });
        if (r.ok) {
          const j = await r.json().catch(() => null);
          if (j && j.ok === true) {
            resolvedApiBase = window.location.origin;
            return;
          }
        }
      } catch {
        /* refused, etc. */
      }
    }

    if (explicitUrl) {
      resolvedApiBase = trimmed.replace(/\/$/, "");
      return;
    }

    resolvedApiBase = "http://127.0.0.1:8765";
  }

  function apiBase() {
    return resolvedApiBase;
  }

  function $(id) {
    return document.getElementById(id);
  }

  function initEls() {
    els.stepper = $("stepper");
    els.dropzone = $("dropzone");
    els.fileInput = $("file-input");
    els.fileMeta = $("file-meta");
    els.uploadPreview = $("upload-preview");
    els.uploadPreviewHead = $("upload-preview-head");
    els.uploadPreviewBody = $("upload-preview-body");
    els.fileName = $("file-name");
    els.fileStats = $("file-stats");
    els.btnContinue = $("btn-continue");
    els.btnReupload = $("btn-reupload");
    els.viewUpload = $("view-upload");
    els.viewInspect = $("view-inspect");
    els.btnBackUpload = $("btn-back-upload");
    els.agentBanner = $("agent-banner");
    els.agentSummary = $("agent-summary");
    els.agentPrivacyNote = $("agent-privacy-note");
    els.btnRetryAi = $("btn-retry-ai");
    els.issueList = $("issue-list");
    els.statsGrid = $("stats-grid");
    els.previewWrap = $("preview-wrap");
    els.previewHead = $("preview-head");
    els.previewBody = $("preview-body");
    els.chartsRow = $("charts-row");
    els.columnTable = $("column-table");
    els.fixTrim = $("fix-trim");
    els.fixDedupe = $("fix-dedupe");
    els.fixDropEmpty = $("fix-drop-empty");
    els.btnApplyFixes = $("btn-apply-fixes");
    els.btnProceedSynthetic = $("btn-proceed-synthetic");
  }

  function toast(msg) {
    const t = document.createElement("div");
    t.className = "toast";
    t.textContent = msg;
    document.body.appendChild(t);
    setTimeout(() => t.remove(), 3200);
  }

  function parseCSV(text) {
    const Papa = window.Papa;
    const res = Papa.parse(text, {
      header: true,
      skipEmptyLines: "greedy",
      dynamicTyping: false,
    });
    if (res.errors && res.errors.length) {
      const fatal = res.errors.find((e) => e.type === "Quotes" || e.code === "TooManyFields");
      if (fatal) throw new Error(fatal.message || "CSV parse error");
    }
    const fields = res.meta.fields || [];
    const rows = (res.data || []).filter((r) => {
      const vals = Object.values(r);
      return vals.some((v) => v !== "" && v != null);
    });
    return { headers: fields, rows };
  }

  function serializeState() {
    return JSON.stringify({
      fileName: state.fileName,
      headers: state.headers,
      rows: state.rows,
      rawText: state.rawText,
    });
  }

  function tryLoadSession() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return false;
      const o = JSON.parse(raw);
      if (!o.headers || !o.rows) return false;
      state.fileName = o.fileName || "dataset.csv";
      state.headers = o.headers;
      state.rows = o.rows;
      state.rawText = o.rawText || "";
      return true;
    } catch {
      return false;
    }
  }

  function saveSession() {
    try {
      localStorage.setItem(STORAGE_KEY, serializeState());
    } catch {
      toast("Could not persist dataset in browser storage.");
    }
  }

  function clearSession() {
    localStorage.removeItem(STORAGE_KEY);
  }

  function inferColumnStats(headers, rows) {
    return headers.map((h) => {
      const vals = rows.map((r) => r[h]).filter((v) => v !== "" && v != null);
      const n = vals.length;
      const numeric = vals
        .map((v) => {
          const x = Number(String(v).replace(/,/g, ""));
          return Number.isFinite(x) ? x : NaN;
        })
        .filter((x) => !Number.isNaN(x));
      const numRatio = n ? numeric.length / n : 0;
      const isNumeric = numRatio > 0.85 && numeric.length > 0;
      const uniq = new Set(vals.map((v) => String(v)));
      const missing = rows.length - vals.length;
      return {
        name: h,
        nonNull: vals.length,
        missing,
        unique: uniq.size,
        inferred: isNumeric ? "numeric" : "text",
        numericSample: isNumeric ? numeric.slice(0, 5000) : [],
      };
    });
  }

  function buildIssues(headers, rows, colStats) {
    const issues = [];
    if (!rows.length) {
      issues.push({
        sev: "high",
        title: "No data rows detected",
        detail: "The file parsed successfully but contains no usable rows after skipping blanks.",
      });
      return issues;
    }
    if (!headers.length) {
      issues.push({
        sev: "high",
        title: "Missing header row",
        detail: "No column names were found. Ensure the first row contains headers.",
      });
      return issues;
    }

    const dupKeys = new Set();
    const seen = new Map();
    rows.forEach((r, i) => {
      const key = headers.map((h) => String(r[h] ?? "")).join("\t");
      if (seen.has(key)) dupKeys.add(key);
      else seen.set(key, i);
    });
    if (dupKeys.size > 0) {
      issues.push({
        sev: "medium",
        title: "Duplicate rows",
        detail: `Approximately ${dupKeys.size} duplicate row pattern(s) detected across all columns.`,
      });
    }

    colStats.forEach((c) => {
      if (c.missing > 0) {
        const pct = ((c.missing / rows.length) * 100).toFixed(1);
        issues.push({
          sev: c.missing / rows.length > 0.25 ? "high" : "medium",
          title: `Missing values in “${c.name}”`,
          detail: `${c.missing} missing (${pct}% of rows).`,
        });
      }
      if (c.inferred === "numeric" && c.numericSample.length) {
        const sorted = [...c.numericSample].sort((a, b) => a - b);
        const q1 = sorted[Math.floor(sorted.length * 0.25)];
        const q3 = sorted[Math.floor(sorted.length * 0.75)];
        const iqr = q3 - q1 || 1;
        const low = q1 - 1.5 * iqr;
        const high = q3 + 1.5 * iqr;
        const outliers = c.numericSample.filter((x) => x < low || x > high).length;
        if (outliers / c.numericSample.length > 0.02) {
          issues.push({
            sev: "low",
            title: `Potential outliers in “${c.name}”`,
            detail: `${outliers} values fall outside a typical IQR band (heuristic screen, not clinical).`,
          });
        }
      }
      if (c.inferred === "text" && c.unique / Math.max(c.nonNull, 1) > 0.95 && c.nonNull > 20) {
        issues.push({
          sev: "low",
          title: `High cardinality in “${c.name}”`,
          detail: "Most values appear unique — useful as an identifier, risky as a direct model feature without handling.",
        });
      }
    });

    const headerSpaces = headers.filter((h) => h !== h.trim() || /^\s|\s$/.test(h));
    if (headerSpaces.length) {
      issues.push({
        sev: "medium",
        title: "Whitespace in column names",
        detail: `${headerSpaces.length} header(s) have leading/trailing spaces or internal spacing issues.`,
      });
    }

    let mixedTypeCols = 0;
    headers.forEach((h) => {
      const types = new Set();
      rows.slice(0, 500).forEach((r) => {
        const v = r[h];
        if (v === "" || v == null) return;
        const n = Number(String(v).replace(/,/g, ""));
        types.add(Number.isFinite(n) && String(v).trim() !== "" ? "num" : "str");
      });
      if (types.size > 1) mixedTypeCols++;
    });
    if (mixedTypeCols) {
      issues.push({
        sev: "medium",
        title: "Mixed-type columns",
        detail: `${mixedTypeCols} column(s) contain both numeric-like and text-like values — review typing before modeling.`,
      });
    }

    if (issues.length === 0) {
      issues.push({
        sev: "low",
        title: "No major hygiene issues flagged",
        detail: "Routine checks passed. Continue to review distributions and domain-specific rules.",
      });
    }
    return issues;
  }

  function sevClass(sev) {
    if (sev === "high" || sev === "medium" || sev === "low") return sev;
    return "low";
  }

  function buildColumnStatsPayload(colStats) {
    return colStats.map((c) => ({
      name: c.name,
      inferred: c.inferred,
      nonNull: c.nonNull,
      missing: c.missing,
      unique: c.unique,
    }));
  }

  function buildSampleRowsForApi(rows, headers, limit) {
    return rows.slice(0, limit).map((r) => {
      const o = {};
      headers.forEach((h) => {
        o[h] = String(r[h] ?? "");
      });
      return o;
    });
  }

  function renderIssueList() {
    els.issueList.innerHTML = state.issues
      .map(
        (i) => `<li class="issue-item">
        <span class="issue-severity ${sevClass(i.sev)}">${escapeHtml(i.sev)}</span>
        <div class="issue-body"><strong>${escapeHtml(i.title)}</strong><span>${escapeHtml(i.detail)}</span></div>
      </li>`
      )
      .join("");
  }

  function renderStepper() {
    els.stepper.innerHTML = STEPS.map((s, i) => {
      let cls = "stepper-item";
      if (i === state.step) cls += " is-active";
      else if (i < state.step) cls += " is-complete";
      else cls += " is-future";
      const num = i < state.step ? "✓" : String(i + 1);
      return `<li class="${cls}" data-step="${i}" role="presentation">
        <span class="step-index">${num}</span>
        <span class="step-label">
          <span class="step-name">${s.name}</span>
          <span class="step-desc">${s.desc}</span>
        </span>
      </li>`;
    }).join("");
  }

  function renderUploadPreview() {
    if (!els.uploadPreview || !els.uploadPreviewHead || !els.uploadPreviewBody) return;
    if (!state.headers.length) {
      els.uploadPreview.hidden = true;
      els.uploadPreviewHead.innerHTML = "";
      els.uploadPreviewBody.innerHTML = "";
      return;
    }
    els.uploadPreview.hidden = false;
    els.uploadPreviewHead.innerHTML = `<tr>${state.headers
      .map((h) => `<th title="${escapeAttr(h)}">${escapeHtml(h)}</th>`)
      .join("")}</tr>`;
    const slice = state.rows.slice(0, 5);
    if (!slice.length) {
      els.uploadPreviewBody.innerHTML = `<tr><td colspan="${state.headers.length}" style="color:var(--text-muted);font-size:0.8125rem;padding:0.75rem">No data rows to show. Check that the file contains rows below the header.</td></tr>`;
    } else {
      els.uploadPreviewBody.innerHTML = slice
        .map(
          (r) =>
            `<tr>${state.headers
              .map((h) => `<td title="${escapeAttr(String(r[h] ?? ""))}">${escapeHtml(String(r[h] ?? ""))}</td>`)
              .join("")}</tr>`
        )
        .join("");
    }
  }

  function setStep(n) {
    if (n === 2 || n === 3) {
      toast("Synthetic generation and review will be available in the next iteration.");
      return;
    }
    state.step = n;
    renderStepper();
    els.viewUpload.classList.toggle("hidden", n !== 0);
    els.viewInspect.classList.toggle("hidden", n !== 1);
    if (n === 1) renderInspect();
    if (n === 0 && state.rows.length) renderUploadPreview();
  }

  function onFileLoaded(fileName, text) {
    const { headers, rows } = parseCSV(text);
    state.fileName = fileName;
    state.headers = headers;
    state.rows = rows;
    state.rawText = text;
    saveSession();
    els.fileMeta.classList.add("is-visible");
    els.fileName.textContent = fileName;
    els.fileStats.textContent = `${rows.length.toLocaleString()} rows · ${headers.length} columns`;
    els.btnContinue.disabled = !rows.length;
    renderUploadPreview();
  }

  function renderInspectStatic(colStats) {
    const nulls = colStats.reduce((a, c) => a + c.missing, 0);

    els.statsGrid.innerHTML = `
      <div class="stat-card"><div class="stat-value">${state.rows.length.toLocaleString()}</div><div class="stat-label">Rows</div></div>
      <div class="stat-card"><div class="stat-value">${state.headers.length}</div><div class="stat-label">Columns</div></div>
      <div class="stat-card"><div class="stat-value">${nulls.toLocaleString()}</div><div class="stat-label">Empty cells</div></div>
      <div class="stat-card"><div class="stat-value">${escapeHtml(state.fileName)}</div><div class="stat-label">File name</div></div>
    `;

    els.previewHead.innerHTML = `<tr>${state.headers.map((h) => `<th title="${escapeAttr(h)}">${escapeHtml(h)}</th>`).join("")}</tr>`;
    const previewRows = state.rows.slice(0, 12);
    els.previewBody.innerHTML = previewRows
      .map(
        (r) =>
          `<tr>${state.headers.map((h) => `<td title="${escapeAttr(String(r[h] ?? ""))}">${escapeHtml(String(r[h] ?? ""))}</td>`).join("")}</tr>`
      )
      .join("");

    els.columnTable.innerHTML = `
      <thead><tr><th>Column</th><th>Inferred type</th><th>Non-null</th><th>Missing</th><th>Unique</th></tr></thead>
      <tbody>
        ${colStats
          .map(
            (c) => `<tr>
          <td><span class="badge">${escapeHtml(c.name)}</span></td>
          <td>${c.inferred}</td>
          <td>${c.nonNull}</td>
          <td>${c.missing}</td>
          <td>${c.unique}</td>
        </tr>`
          )
          .join("")}
      </tbody>`;

    renderCharts(colStats);
  }

  async function renderInspect() {
    const colStats = inferColumnStats(state.headers, state.rows);
    const ruleIssues = buildIssues(state.headers, state.rows, colStats);
    renderInspectStatic(colStats);

    els.agentSummary.textContent =
      "OpenAI is reviewing column profiles, deterministic checks, and a small row sample through the local API. This usually takes a few seconds.";
    els.issueList.innerHTML = `<li class="issue-item"><span class="issue-severity low">…</span><div class="issue-body"><strong>In progress</strong><span>Running model assessment.</span></div></li>`;
    if (els.btnRetryAi) els.btnRetryAi.classList.add("hidden");
    if (els.agentPrivacyNote) els.agentPrivacyNote.classList.add("hidden");
    if (els.agentBanner) els.agentBanner.classList.add("is-loading");

    const payload = {
      file_name: state.fileName,
      row_count: state.rows.length,
      headers: state.headers,
      column_stats: buildColumnStatsPayload(colStats),
      sample_rows: buildSampleRowsForApi(state.rows, state.headers, 25),
      deterministic_findings: ruleIssues.map((i) => ({ sev: i.sev, title: i.title, detail: i.detail })),
    };

    try {
      const res = await fetch(`${apiBase()}/api/quality-analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const raw = await res.json().catch(() => ({}));
      if (!res.ok) {
        let msg = res.statusText;
        if (typeof raw.detail === "string") msg = raw.detail;
        else if (Array.isArray(raw.detail))
          msg = raw.detail.map((d) => (d.msg != null ? d.msg : JSON.stringify(d))).join("; ");
        throw new Error(msg || `Request failed (${res.status})`);
      }
      els.agentSummary.textContent = raw.summary || "Assessment complete.";
      state.issues = (raw.issues || []).map((i) => ({
        sev: sevClass(i.sev),
        title: String(i.title || "Finding"),
        detail: String(i.detail || ""),
      }));
      if (!state.issues.length) state.issues = ruleIssues;
      renderIssueList();
      if (els.agentPrivacyNote) els.agentPrivacyNote.classList.remove("hidden");
      if (els.btnRetryAi) els.btnRetryAi.classList.add("hidden");
    } catch (err) {
      console.error(err);
      const reason = err && err.message ? err.message : String(err);
      toast(reason);
      let hint =
        " Showing rule-based checks only. Open the app at the same URL as FastAPI (run python server.py from the project folder).";
      if (/unsupported method|501/i.test(reason)) {
        hint =
          " You are hitting Python's static file server (e.g. python -m http.server), which does not allow POST. Stop it and run python server.py instead, then reload this page from that address.";
      }
      els.agentSummary.textContent = `Could not reach the AI API (${reason}).${hint}`;
      state.issues = ruleIssues;
      renderIssueList();
      if (els.btnRetryAi) els.btnRetryAi.classList.remove("hidden");
    } finally {
      if (els.agentBanner) els.agentBanner.classList.remove("is-loading");
    }
  }

  function destroyCharts() {
    state.charts.forEach((c) => c.destroy());
    state.charts = [];
    els.chartsRow.innerHTML = "";
  }

  function renderCharts(colStats) {
    destroyCharts();
    const Chart = window.Chart;
    if (!Chart) return;

    const numericCols = colStats.filter((c) => c.inferred === "numeric" && c.numericSample.length > 2).slice(0, 3);
    const catCols = colStats
      .filter((c) => c.inferred === "text" && c.nonNull > 0)
      .slice(0, 2);

    numericCols.forEach((c) => {
      const id = "chart-" + safeId(c.name);
      const wrap = document.createElement("div");
      wrap.className = "chart-card";
      wrap.innerHTML = `<h4>Distribution — ${escapeHtml(c.name)}</h4><canvas id="${id}" height="200"></canvas>`;
      els.chartsRow.appendChild(wrap);
      const sample = c.numericSample;
      const bins = 12;
      const min = Math.min(...sample);
      const max = Math.max(...sample);
      const step = (max - min) / bins || 1;
      const counts = new Array(bins).fill(0);
      sample.forEach((x) => {
        let i = Math.floor((x - min) / step);
        if (i >= bins) i = bins - 1;
        if (i < 0) i = 0;
        counts[i]++;
      });
      const labels = counts.map((_, i) => (min + i * step).toFixed(2));
      const ctx = document.getElementById(id);
      state.charts.push(
        new Chart(ctx, {
          type: "bar",
          data: {
            labels,
            datasets: [
              {
                label: "Count",
                data: counts,
                backgroundColor: "rgba(14, 165, 233, 0.45)",
                borderColor: "rgba(2, 132, 199, 0.9)",
                borderWidth: 1,
                borderRadius: 4,
              },
            ],
          },
          options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
              x: { ticks: { maxRotation: 45, font: { size: 10 } } },
              y: { beginAtZero: true, ticks: { font: { size: 10 } } },
            },
          },
        })
      );
    });

    catCols.forEach((c) => {
      const freq = new Map();
      state.rows.forEach((r) => {
        const v = r[c.name];
        if (v === "" || v == null) return;
        const k = String(v);
        freq.set(k, (freq.get(k) || 0) + 1);
      });
      const top = [...freq.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8);
      const id = "chart-cat-" + safeId(c.name);
      const wrap = document.createElement("div");
      wrap.className = "chart-card";
      wrap.innerHTML = `<h4>Top categories — ${escapeHtml(c.name)}</h4><canvas id="${id}" height="220"></canvas>`;
      els.chartsRow.appendChild(wrap);
      const ctx = document.getElementById(id);
      state.charts.push(
        new Chart(ctx, {
          type: "bar",
          data: {
            labels: top.map(([k]) => (k.length > 24 ? k.slice(0, 22) + "…" : k)),
            datasets: [
              {
                label: "Rows",
                data: top.map(([, v]) => v),
                backgroundColor: "rgba(56, 189, 248, 0.5)",
                borderColor: "rgba(3, 105, 161, 0.85)",
                borderWidth: 1,
                borderRadius: 4,
              },
            ],
          },
          options: {
            indexAxis: "y",
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
              x: { beginAtZero: true, ticks: { font: { size: 10 } } },
              y: { ticks: { font: { size: 10 } } },
            },
          },
        })
      );
    });

    if (!numericCols.length && !catCols.length) {
      els.chartsRow.innerHTML =
        '<p class="panel-lead" style="margin:0">Not enough typed columns to chart automatically. Upload a sample with numeric or categorical fields.</p>';
    }
  }

  function safeId(s) {
    return String(s).replace(/[^a-z0-9]+/gi, "-").slice(0, 40);
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function escapeAttr(s) {
    return escapeHtml(s).replace(/'/g, "&#39;");
  }

  function applyFixes() {
    let rows = state.rows.map((r) => ({ ...r }));
    let headers = [...state.headers];

    if (els.fixTrim.checked) {
      const newHeaders = headers.map((h) => h.trim());
      state.headers = newHeaders;
      rows = rows.map((r) => {
        const o = {};
        headers.forEach((oldH, i) => {
          const nh = newHeaders[i];
          let v = r[oldH];
          if (typeof v === "string") v = v.trim();
          o[nh] = v;
        });
        return o;
      });
      headers = newHeaders;
    }

    if (els.fixDedupe.checked) {
      const seen = new Set();
      rows = rows.filter((r) => {
        const key = state.headers.map((h) => String(r[h] ?? "")).join("\t");
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    if (els.fixDropEmpty.checked) {
      rows = rows.filter((r) =>
        state.headers.some((h) => {
          const v = r[h];
          return v !== "" && v != null;
        })
      );
    }

    state.rows = rows;
    const Papa = window.Papa;
    state.rawText = Papa.unparse({
      fields: state.headers,
      data: rows.map((r) => state.headers.map((h) => r[h] ?? "")),
    });
    saveSession();
    toast("Fixes applied. Dataset updated for this session.");
    renderInspect();
  }

  function bindEvents() {
    els.dropzone.addEventListener("click", () => els.fileInput.click());
    els.dropzone.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        els.fileInput.click();
      }
    });
    els.dropzone.addEventListener("dragover", (e) => {
      e.preventDefault();
      els.dropzone.classList.add("is-dragover");
    });
    els.dropzone.addEventListener("dragleave", () => els.dropzone.classList.remove("is-dragover"));
    els.dropzone.addEventListener("drop", (e) => {
      e.preventDefault();
      els.dropzone.classList.remove("is-dragover");
      const f = e.dataTransfer.files[0];
      if (f && /\.csv$/i.test(f.name)) readFile(f);
      else toast("Please drop a .csv file.");
    });

    els.fileInput.addEventListener("change", () => {
      const f = els.fileInput.files[0];
      if (f) readFile(f);
    });

    els.btnContinue.addEventListener("click", () => {
      if (!state.rows.length) return;
      setStep(1);
    });

    els.btnReupload.addEventListener("click", () => {
      clearSession();
      state.rows = [];
      state.headers = [];
      state.fileName = "";
      state.rawText = "";
      els.fileMeta.classList.remove("is-visible");
      if (els.uploadPreview) {
        els.uploadPreview.hidden = true;
        els.uploadPreviewHead.innerHTML = "";
        els.uploadPreviewBody.innerHTML = "";
      }
      els.btnContinue.disabled = true;
      els.fileInput.value = "";
      toast("Session cleared. Upload a new file.");
    });

    els.btnBackUpload.addEventListener("click", () => setStep(0));

    els.btnApplyFixes.addEventListener("click", applyFixes);

    if (els.btnRetryAi) {
      els.btnRetryAi.addEventListener("click", () => {
        renderInspect();
      });
    }

    els.btnProceedSynthetic.addEventListener("click", () => toast("Synthetic data creation will be added in the next phase."));

    els.stepper.addEventListener("click", (e) => {
      const item = e.target.closest(".stepper-item");
      if (!item) return;
      const i = Number(item.dataset.step);
      if (i === 0) setStep(0);
      if (i === 1 && state.rows.length) setStep(1);
      if (i >= 2) setStep(i);
    });
  }

  function readFile(file) {
    const reader = new FileReader();
    reader.onload = () => {
      try {
        onFileLoaded(file.name, String(reader.result || ""));
      } catch (err) {
        toast(err.message || "Failed to parse CSV.");
      }
    };
    reader.readAsText(file);
  }

  function refreshUploadUI() {
    els.fileMeta.classList.add("is-visible");
    els.fileName.textContent = state.fileName;
    els.fileStats.textContent = `${state.rows.length.toLocaleString()} rows · ${state.headers.length} columns`;
    els.btnContinue.disabled = !state.rows.length;
    renderUploadPreview();
  }

  async function checkApiServer() {
    const banner = $("server-warning");
    if (!banner) return;
    try {
      const res = await fetch(`${apiBase()}/api/health`, { method: "GET" });
      if (res.status === 404) {
        banner.classList.remove("hidden");
        banner.innerHTML =
          "This address is serving <strong>static files only</strong> (for example <code>python -m http.server</code>). The AI API is not available here. Stop that process, run <code>python server.py</code> from the project folder, then open <code>http://127.0.0.1:8765</code> (or your <code>SERVER_PORT</code>).";
        return;
      }
      if (!res.ok) return;
      const data = await res.json().catch(() => ({}));
      const keyLen = typeof data.openai_key_char_length === "number" ? data.openai_key_char_length : 0;
      const noKey = data.openai_configured === false || keyLen === 0;
      if (noKey) {
        banner.classList.remove("hidden");
        const exists = data.env_file_exists === true;
        const hint = data.server_dir ? ` Server is running from: ${data.server_dir}.` : "";
        const envPath = data.env_file_next_to_server_py || ".env next to server.py";
        if (!exists) {
          banner.textContent = `No .env file at ${envPath}.${hint} Create that file with a line OPENAI_API_KEY=sk-... (no spaces around =) and restart the server.`;
        } else {
          banner.textContent = `OPENAI_API_KEY is still empty.${hint} Common cause: another .env in your shell's current folder (cwd) had an empty OPENAI_API_KEY line and overwrote this file — that is fixed in the latest server.py; restart the server. Otherwise check for a typo in the variable name, or an invalid/revoked key (invalid keys still show as "configured" here but fail when calling OpenAI).`;
        }
      }
    } catch {
      banner.classList.remove("hidden");
      banner.textContent = `Cannot reach the API at ${apiBase()}. In the project folder run: python server.py (then keep this tab open or reload).`;
    }
  }

  async function boot() {
    initEls();
    await initApiBase();
    bindEvents();
    renderStepper();
    await checkApiServer();
    if (tryLoadSession() && state.rows.length) {
      const Papa = window.Papa;
      if (!state.rawText && state.headers.length) {
        state.rawText = Papa.unparse({
          fields: state.headers,
          data: state.rows.map((r) => state.headers.map((h) => r[h] ?? "")),
        });
      }
      refreshUploadUI();
      els.btnContinue.disabled = false;
    }
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", () => void boot());
  else void boot();
})();
