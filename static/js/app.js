(function () {
  "use strict";

  const STORAGE_KEY = "southlake_csv_session_v1";
  const STEPS = [
    { id: "input", name: "Input data", desc: "Upload your CSV" },
    { id: "inspect", name: "Data inspection", desc: "Quality & summary" },
    { id: "metadata", name: "Metadata", desc: "Schema & change summary" },
    { id: "synthetic", name: "Synthetic data", desc: "Generate" },
    { id: "review", name: "Review", desc: "Validate" },
    { id: "analyze", name: "Analyze", desc: "Explore results" },
  ];

  const METADATA_NOTE_SECTIONS = ["dashboard", "ai", "summary", "hygiene", "columns", "correlations", "distributions", "json"];

  const METADATA_SECTION_NOTE_LABELS = {
    dashboard: "Dashboard",
    ai: "AI Metadata Agent",
    summary: "Summary",
    hygiene: "Hygiene context",
    columns: "Columns",
    correlations: "Numeric correlations",
    distributions: "Distributions",
    json: "Technical JSON export",
  };

  const METADATA_SCHEMA_VERSION = "1.5";

  const COLUMN_SYNTH_EDIT_KEYS = [
    "synthDist",
    "synthMin",
    "synthMax",
    "synthMean",
    "synthVariance",
    "synthNumHistCustom",
    "synthCatMode",
    "synthCatMergePct",
    "synthCatCustom",
  ];

  /** Bin count for numeric histogram UI / synthNumHistCustom (matches Chart.js metadata charts). */
  const NUMERIC_DIST_HIST_BINS = 12;

  let state = {
    step: 0,
    fileName: "",
    headers: [],
    rows: [],
    rawText: "",
    issues: [],
    charts: [],
    metaCharts: [],
    columnInclude: null,
    columnMetadataEdits: {},
    /** Map pairKey(colA,colB) -> synthetic Pearson r in [-1,1] */
    correlationEdits: {},
    /** Within step 2: "editor" | "changesReview" */
    metadataPane: "editor",
    /** Per accordion section on metadata screen: rationale notes */
    metadataSectionNotes: {},
    /** Per section on metadata change summary: rationale for that grouping */
    metadataChangeReviewNotes: {},
    /** Last coach run: { runId, summary, items: [{ id, title, detail, importance, suggested_action, related_columns }] } */
    metadataAiLastRun: null,
    /** Accepted coach recommendations persisted for synthesis + change summary */
    metadataAiAccepted: [],
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
    els.fileName = $("file-name");
    els.fileStats = $("file-stats");
    els.btnContinue = $("btn-continue");
    els.btnReupload = $("btn-reupload");
    els.viewUpload = $("view-upload");
    els.viewInspect = $("view-inspect");
    els.viewMetadata = $("view-metadata");
    els.viewMetadataReview = $("view-metadata-review");
    els.metadataChangesRoot = $("metadata-changes-root");
    els.viewSynthetic = $("view-synthetic");
    els.viewReview = $("view-review");
    els.viewAnalyze = $("view-analyze");
    els.btnBackUpload = $("btn-back-upload");
    els.agentBanner = $("agent-banner");
    els.agentSummary = $("agent-summary");
    els.agentPrivacyNote = $("agent-privacy-note");
    els.btnRetryAi = $("btn-retry-ai");
    els.issueList = $("issue-list");
    els.statsGrid = $("stats-grid");
    els.chartsRow = $("charts-row");
    els.columnTable = $("column-table");
    els.previewWrap = $("preview-wrap");
    els.previewHead = $("preview-head");
    els.previewBody = $("preview-body");
    els.fixTrim = $("fix-trim");
    els.fixDedupe = $("fix-dedupe");
    els.fixDropEmpty = $("fix-drop-empty");
    els.btnApplyFixes = $("btn-apply-fixes");
    els.btnProceedMetadata = $("btn-proceed-metadata");
    els.btnBackInspectMeta = $("btn-back-inspect-meta");
    els.btnProceedMetadataReview = $("btn-proceed-metadata-review");
    els.btnBackMetadataEditor = $("btn-back-metadata-editor");
    els.btnProceedSyntheticFromReview = $("btn-proceed-synthetic-from-review");
    els.btnBackFromSynthetic = $("btn-back-to-metadata");
    els.btnProceedReview = $("btn-proceed-review");
    els.btnBackSynthetic = $("btn-back-synthetic");
    els.btnPrintMetadataChanges = $("btn-print-metadata-changes");
    els.btnProceedAnalyze = $("btn-proceed-analyze");
    els.btnBackAnalyze = $("btn-back-analyze");
    els.metadataHygieneList = $("metadata-hygiene-list");
    els.metadataDashboardStats = $("metadata-dashboard-stats");
    els.metadataDashboardColumns = $("metadata-dashboard-columns");
    els.metadataJson = $("metadata-json");
    els.metadataChartsRow = $("metadata-charts-row");
    els.metadataDistLegend = $("metadata-dist-legend");
    els.metadataDistributionEditor = $("metadata-distribution-editor");
    els.metadataCorrelation = $("metadata-correlation");
    els.metadataAiSuggestBtn = $("btn-metadata-ai-suggest");
    els.metadataAiSuggestStatus = $("metadata-ai-suggest-status");
    els.metadataAiSuggestBody = $("metadata-ai-suggest-body");
    els.metadataEditModal = $("metadata-edit-modal");
    els.metadataEditTitle = $("metadata-edit-title");
    els.metadataEditLabel = $("metadata-edit-label");
    els.metadataEditType = $("metadata-edit-type");
    els.metadataEditNote = $("metadata-edit-note");
    els.metadataEditSave = $("metadata-edit-save");
    els.metadataEditCancel = $("metadata-edit-cancel");
    els.metadataEditBackdrop = $("metadata-edit-backdrop");
    els.metadataEditNumericFieldset = $("metadata-edit-numeric-fieldset");
    els.metadataEditNumericObserved = $("metadata-edit-numeric-observed");
    els.metadataEditDistribution = $("metadata-edit-distribution");
    els.metadataEditSynthMin = $("metadata-edit-synth-min");
    els.metadataEditSynthMax = $("metadata-edit-synth-max");
    els.metadataEditSynthMean = $("metadata-edit-synth-mean");
    els.metadataEditSynthVariance = $("metadata-edit-synth-variance");
    els.metadataEditCategoricalFieldset = $("metadata-edit-categorical-fieldset");
    els.metadataEditCategoricalObserved = $("metadata-edit-categorical-observed");
    els.metadataEditCatMode = $("metadata-edit-cat-mode");
    els.metadataEditCatMergePct = $("metadata-edit-cat-merge-pct");
    els.metadataEditCatCustom = $("metadata-edit-cat-custom");
    els.metadataEditCatMergeWrap = $("metadata-edit-cat-merge-wrap");
    els.metadataEditCatCustomWrap = $("metadata-edit-cat-custom-wrap");
    els.correlationEditModal = $("correlation-edit-modal");
    els.correlationEditBackdrop = $("correlation-edit-backdrop");
    els.correlationEditPairLabel = $("correlation-edit-pair-label");
    els.correlationEditObserved = $("correlation-edit-observed");
    els.correlationEditTarget = $("correlation-edit-target");
    els.correlationEditSave = $("correlation-edit-save");
    els.correlationEditCancel = $("correlation-edit-cancel");
    els.correlationEditClear = $("correlation-edit-clear");
    els.btnResetAllSynth = $("btn-reset-all-synth");
    els.btnResetColumnsSynth = $("btn-reset-columns-synth");
    els.btnResetCorrSynth = $("btn-reset-corr-synth");
    els.btnRevertDistributions = $("btn-revert-distributions");
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
      columnInclude: state.columnInclude,
      columnMetadataEdits: state.columnMetadataEdits || {},
      correlationEdits: state.correlationEdits || {},
      metadataPane: state.metadataPane === "changesReview" ? "changesReview" : "editor",
      metadataSectionNotes: state.metadataSectionNotes && typeof state.metadataSectionNotes === "object" ? state.metadataSectionNotes : {},
      metadataChangeReviewNotes:
        state.metadataChangeReviewNotes && typeof state.metadataChangeReviewNotes === "object" ? state.metadataChangeReviewNotes : {},
      metadataAiLastRun: state.metadataAiLastRun && typeof state.metadataAiLastRun === "object" ? state.metadataAiLastRun : null,
      metadataAiAccepted: Array.isArray(state.metadataAiAccepted) ? state.metadataAiAccepted : [],
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
      state.columnInclude = o.columnInclude && typeof o.columnInclude === "object" ? o.columnInclude : null;
      state.columnMetadataEdits =
        o.columnMetadataEdits && typeof o.columnMetadataEdits === "object" ? o.columnMetadataEdits : {};
      state.correlationEdits = o.correlationEdits && typeof o.correlationEdits === "object" ? o.correlationEdits : {};
      state.metadataPane = o.metadataPane === "changesReview" ? "changesReview" : "editor";
      state.metadataSectionNotes =
        o.metadataSectionNotes && typeof o.metadataSectionNotes === "object" ? o.metadataSectionNotes : {};
      state.metadataChangeReviewNotes =
        o.metadataChangeReviewNotes && typeof o.metadataChangeReviewNotes === "object" ? o.metadataChangeReviewNotes : {};
      state.metadataAiLastRun =
        o.metadataAiLastRun && typeof o.metadataAiLastRun === "object" && Array.isArray(o.metadataAiLastRun.items)
          ? {
              runId: o.metadataAiLastRun.runId,
              summary: String(o.metadataAiLastRun.summary || ""),
              items: o.metadataAiLastRun.items,
            }
          : null;
      state.metadataAiAccepted = Array.isArray(o.metadataAiAccepted) ? o.metadataAiAccepted : [];
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

  function getColumnIncludeMap() {
    const m = {};
    state.headers.forEach((h) => {
      m[h] = !(state.columnInclude && state.columnInclude[h] === false);
    });
    return m;
  }

  function getEditForCol(colName) {
    if (!state.columnMetadataEdits || typeof state.columnMetadataEdits !== "object") return {};
    return state.columnMetadataEdits[colName] || {};
  }

  function effectiveColumnKind(colStat, ed) {
    if (ed.treatAsType === "numeric") return "numeric";
    if (ed.treatAsType === "text") return "text";
    return colStat.inferred;
  }

  function parseOptionalNumber(raw) {
    if (raw == null) return null;
    const s = String(raw).trim().replace(/,/g, "");
    if (s === "") return null;
    const x = Number(s);
    return Number.isFinite(x) ? x : null;
  }

  function computeNumericSummaryFromSamples(arr) {
    if (!arr || !arr.length) return null;
    const sum = arr.reduce((a, b) => a + b, 0);
    const mean = sum / arr.length;
    const sorted = [...arr].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    const median = sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    let sq = 0;
    arr.forEach((x) => {
      sq += (x - mean) * (x - mean);
    });
    const variance = arr.length > 1 ? sq / (arr.length - 1) : 0;
    const std = Math.sqrt(Math.max(0, variance));
    return {
      min: sorted[0],
      max: sorted[sorted.length - 1],
      mean: Number(mean.toFixed(6)),
      median: Number(median.toFixed(6)),
      std: Number(std.toFixed(6)),
      variance: Number(variance.toFixed(6)),
    };
  }

  function topCategoriesObserved(rows, colName, limit) {
    const freq = new Map();
    rows.forEach((r) => {
      const v = r[colName];
      if (v === "" || v == null) return;
      const k = String(v);
      freq.set(k, (freq.get(k) || 0) + 1);
    });
    const top = [...freq.entries()].sort((a, b) => b[1] - a[1]).slice(0, limit);
    return top.map(([value, count]) => ({
      value: value.length > 120 ? value.slice(0, 118) + "…" : value,
      count,
      proportion: rows.length ? Number((count / rows.length).toFixed(4)) : 0,
    }));
  }

  function pairCorrelationKey(a, b) {
    return a < b ? `${a}\x00${b}` : `${b}\x00${a}`;
  }

  function buildSyntheticNumericTargetsFromEdits(ed, observedSummary) {
    const t = {};
    if (ed.synthDist && ed.synthDist !== "auto") t.distribution_shape = ed.synthDist;
    const range = {};
    const mn = parseOptionalNumber(ed.synthMin);
    const mx = parseOptionalNumber(ed.synthMax);
    if (mn != null) range.min = mn;
    if (mx != null) range.max = mx;
    if (Object.keys(range).length) t.range = range;
    const mean = parseOptionalNumber(ed.synthMean);
    const variance = parseOptionalNumber(ed.synthVariance);
    const moments = {};
    if (mean != null) moments.mean = Number(mean.toFixed(6));
    if (variance != null) moments.variance = Number(variance.toFixed(6));
    if (Object.keys(moments).length) t.moments = moments;
    const histParsed = parseNumericHistogramProportionsJson(ed.synthNumHistCustom, { silent: true });
    if (histParsed && histParsed.length) {
      t.custom_discretized_histogram = {
        binning: "equal_width_on_profiled_numeric_sample",
        bin_count: histParsed.length,
        bins: histParsed.map((row) => ({
          bin_label: row.label,
          proportion: row.proportion,
        })),
      };
    }
    if (!Object.keys(t).length) return undefined;
    if (observedSummary) t.numeric_summary_observed_reference = observedSummary;
    return t;
  }

  function parseNumericHistogramProportionsJson(text, opts) {
    const silent = opts && opts.silent;
    if (!text || !String(text).trim()) return null;
    let arr;
    try {
      arr = JSON.parse(String(text).trim());
    } catch {
      return null;
    }
    if (!Array.isArray(arr) || !arr.length) return null;
    const out = [];
    for (let i = 0; i < arr.length; i++) {
      const row = arr[i];
      if (!row || typeof row !== "object") return null;
      const label = row.label != null ? String(row.label) : "";
      const p = parseOptionalNumber(row.proportion);
      if (p == null || p < 0 || p > 1) return null;
      out.push({ label: label.length > 40 ? label.slice(0, 38) + "…" : label, proportion: Number(p.toFixed(6)) });
    }
    const sum = out.reduce((a, x) => a + x.proportion, 0);
    if (!silent && Math.abs(sum - 1) > 0.08) {
      toast("Histogram bin targets should sum to about 1.0 (current sum " + sum.toFixed(3) + ").");
    }
    return out;
  }

  /** Short tick text for numeric histogram SVG (full-precision label stays in data for JSON matching). */
  function formatDistNumBinAxisDisplay(labelStr) {
    const v = Number(String(labelStr).replace(/,/g, ""));
    if (!Number.isFinite(v)) {
      const s = String(labelStr);
      return s.length > 9 ? `${s.slice(0, 8)}…` : s;
    }
    return String(Math.round(v));
  }

  function buildObservedNumericHistogramBins(colStat) {
    const sample = colStat.numericSample;
    if (!sample || sample.length < 3) return null;
    const bins = NUMERIC_DIST_HIST_BINS;
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
    const total = counts.reduce((a, b) => a + b, 0) || 1;
    return labels.map((label, i) => ({
      label,
      proportion: counts[i] / total,
      original: counts[i] / total,
    }));
  }

  function numericHistogramMatchesBaseline(baseBins, proportions) {
    if (!baseBins || !proportions || baseBins.length !== proportions.length) return false;
    for (let i = 0; i < baseBins.length; i++) {
      if (Math.abs((proportions[i] || 0) - baseBins[i].original) > 0.002) return false;
    }
    return true;
  }

  function parseCustomCategoryProportionsJson(text, opts) {
    const silent = opts && opts.silent;
    if (!text || !String(text).trim()) return null;
    let arr;
    try {
      arr = JSON.parse(String(text).trim());
    } catch {
      return null;
    }
    if (!Array.isArray(arr) || !arr.length) return null;
    const out = [];
    for (let i = 0; i < arr.length; i++) {
      const row = arr[i];
      if (!row || typeof row !== "object") return null;
      const value = row.value != null ? String(row.value) : "";
      const p = parseOptionalNumber(row.proportion);
      if (p == null || p < 0 || p > 1) return null;
      out.push({ value: value.length > 200 ? value.slice(0, 198) + "…" : value, proportion: Number(p.toFixed(6)) });
    }
    const sum = out.reduce((a, x) => a + x.proportion, 0);
    if (!silent && Math.abs(sum - 1) > 0.08) {
      toast("Custom category proportions should sum to about 1.0 (current sum " + sum.toFixed(3) + ").");
    }
    return out;
  }

  function buildSyntheticCategoricalTargetsFromEdits(ed, observedTop) {
    const mode = ed.synthCatMode;
    if (!mode || mode === "auto") return undefined;
    if (mode === "custom") {
      const parsed = parseCustomCategoryProportionsJson(ed.synthCatCustom, { silent: true });
      if (!parsed) return undefined;
      return {
        strategy: mode,
        observed_top_categories_reference: observedTop,
        custom_category_proportions: parsed,
      };
    }
    const base = { strategy: mode, observed_top_categories_reference: observedTop };
    if (mode === "merge_rare") {
      const p = parseOptionalNumber(ed.synthCatMergePct);
      if (p != null) base.merge_rare_below_proportion = Math.min(1, Math.max(0, p));
    }
    return base;
  }

  function defaultHowUsed(c, rows, highCard) {
    if (c.inferred === "numeric" && c.numericSample.length) {
      return "Synthetic draws match this column’s marginal distribution (histogram / mean & spread). Joint relationships with other included fields can be layered in a full generator.";
    }
    return highCard
      ? "High cardinality: often treated as identifier-like; synthetic generators may hash, bucket, or exclude from categorical sampling."
      : "Category frequencies inform synthetic sampling for this field so marginals align with the source.";
  }

  function enrichColumnsForMetadata(headers, rows, colStats) {
    const inc = getColumnIncludeMap();
    return colStats.map((c) => {
      const ed = getEditForCol(c.name);
      const label_for_synthesis =
        ed.displayLabel && String(ed.displayLabel).trim() ? String(ed.displayLabel).trim() : c.name;
      const userSchemaType = ed.treatAsType === "numeric" || ed.treatAsType === "text" ? ed.treatAsType : null;
      const userNote = ed.synthesisNote && String(ed.synthesisNote).trim() ? String(ed.synthesisNote).trim() : null;
      const highCard = c.unique >= Math.max(c.nonNull * 0.9, 10);
      const defaultHow = defaultHowUsed(c, rows, highCard);
      const kind = effectiveColumnKind(c, ed);

      const base = {
        name: c.name,
        label_for_synthesis,
        include_in_synthetic_schema: inc[c.name] !== false,
        computed_profile_dtype: c.inferred,
        user_schema_dtype_override: userSchemaType || undefined,
        effective_synthesis_dtype: kind,
        non_null_count: c.nonNull,
        missing_count: c.missing,
        missing_rate: rows.length ? Number((c.missing / rows.length).toFixed(4)) : 0,
        distinct_count: c.unique,
        synthesis_note_user: userNote || undefined,
        how_used_for_synthesis: userNote || defaultHow,
      };

      const aiCoachForCol = acceptedAiGuidanceForColumn(c.name);
      if (aiCoachForCol.length) base.accepted_ai_coach_guidance = aiCoachForCol;

      const observedNumFull =
        c.inferred === "numeric" && c.numericSample.length ? computeNumericSummaryFromSamples(c.numericSample) : null;
      const observedCatRef = topCategoriesObserved(rows, c.name, 10);

      if (kind === "numeric") {
        const o = { ...base };
        if (observedNumFull) {
          o.numeric_summary = {
            min: observedNumFull.min,
            max: observedNumFull.max,
            mean: observedNumFull.mean,
            median: observedNumFull.median,
            std: observedNumFull.std,
          };
        }
        const synthNum = buildSyntheticNumericTargetsFromEdits(ed, observedNumFull || undefined);
        if (synthNum) o.synthetic_numeric_targets = synthNum;
        return o;
      }

      const top6 = observedCatRef.slice(0, 6);
      const o = {
        ...base,
        categorical_summary: {
          top_categories: top6,
        },
      };
      const synthCat = buildSyntheticCategoricalTargetsFromEdits(ed, observedCatRef);
      if (synthCat) o.synthetic_categorical_targets = synthCat;
      return o;
    });
  }

  function acceptedAiGuidanceForColumn(colName) {
    const acc = state.metadataAiAccepted;
    if (!Array.isArray(acc) || !acc.length) return [];
    const out = [];
    for (const rec of acc) {
      const cols = Array.isArray(rec.related_columns) ? rec.related_columns : [];
      if (!cols.includes(colName)) continue;
      out.push({
        recommendation_id: rec.id,
        title: String(rec.title || "").slice(0, 200),
        detail: String(rec.detail || "").slice(0, 500),
        importance: rec.importance === "high" || rec.importance === "low" ? rec.importance : "medium",
        suggested_action: rec.suggested_action ? String(rec.suggested_action).slice(0, 400) : undefined,
        accepted_at_utc: rec.accepted_at_utc || undefined,
      });
    }
    return out;
  }

  function columnsTouchedByAcceptedAi() {
    const acc = state.metadataAiAccepted;
    if (!Array.isArray(acc) || !acc.length) return [];
    const set = new Set();
    for (const rec of acc) {
      const cols = Array.isArray(rec.related_columns) ? rec.related_columns : [];
      cols.forEach((c) => {
        if (c && state.headers.includes(c)) set.add(c);
      });
    }
    return [...set];
  }

  function briefAcceptedAiColumnHint(colName) {
    const hits = acceptedAiGuidanceForColumn(colName);
    if (!hits.length) return "";
    if (hits.length === 1) {
      const t = hits[0].title || "Accepted tip";
      return `AI coach: ${t.length > 42 ? `${t.slice(0, 40)}…` : t}`;
    }
    return `AI coach: ${hits.length} accepted tips`;
  }

  function parseNumericForCorrelation(value) {
    if (value === "" || value == null) return null;
    const x = Number(String(value).replace(/,/g, ""));
    return Number.isFinite(x) ? x : null;
  }

  /** Pearson r on aligned pairs (same index in xs and ys). */
  function pearsonPairwiseComplete(xs, ys) {
    const n = xs.length;
    if (n < 2) return { r: null, n };
    let sumX = 0;
    let sumY = 0;
    let sumXX = 0;
    let sumYY = 0;
    let sumXY = 0;
    for (let i = 0; i < n; i++) {
      const x = xs[i];
      const y = ys[i];
      sumX += x;
      sumY += y;
      sumXX += x * x;
      sumYY += y * y;
      sumXY += x * y;
    }
    const num = n * sumXY - sumX * sumY;
    const den = Math.sqrt((n * sumXX - sumX * sumX) * (n * sumYY - sumY * sumY));
    if (den === 0 || !Number.isFinite(den)) return { r: null, n };
    const raw = num / den;
    const r = Number.isFinite(raw) ? Math.max(-1, Math.min(1, raw)) : null;
    return { r, n };
  }

  const NUMERIC_CORR_MAX_ROWS = 25000;
  const NUMERIC_CORR_MAX_COLS = 22;

  function applyCorrelationOverridesToMatrix(sourceMatrix, columns, edits) {
    const matrix = sourceMatrix.map((row) => row.slice());
    const overrides_list = [];
    if (!edits || typeof edits !== "object") return { matrix, overrides_list };
    const k = columns.length;
    for (let i = 0; i < k; i++) {
      for (let j = i + 1; j < k; j++) {
        const key = pairCorrelationKey(columns[i], columns[j]);
        if (edits[key] == null || edits[key] === "") continue;
        const target = Number(edits[key]);
        if (!Number.isFinite(target)) continue;
        const clamped = Number(Math.max(-1, Math.min(1, target)).toFixed(4));
        const before = matrix[i][j];
        matrix[i][j] = clamped;
        matrix[j][i] = clamped;
        overrides_list.push({
          column_a: columns[i],
          column_b: columns[j],
          pearson_r_observed: before,
          pearson_r_for_synthesis: clamped,
        });
      }
    }
    return { matrix, overrides_list };
  }

  function topPairsFromMatrix(matrix, columns, pairNsMap, limit) {
    const pairs = [];
    const k = columns.length;
    for (let i = 0; i < k; i++) {
      for (let j = i + 1; j < k; j++) {
        const r = matrix[i][j];
        if (r == null || Number.isNaN(r)) continue;
        const key = pairCorrelationKey(columns[i], columns[j]);
        pairs.push({
          column_a: columns[i],
          column_b: columns[j],
          pearson_r: r,
          pairwise_complete_rows: pairNsMap.get(key) || 0,
        });
      }
    }
    pairs.sort((a, b) => Math.abs(b.pearson_r) - Math.abs(a.pearson_r));
    return pairs.slice(0, limit);
  }

  function buildNumericCorrelationBlock(headers, rows, colStats) {
    const numericNames = headers.filter((h) => {
      const c = colStats.find((s) => s.name === h);
      return c && c.inferred === "numeric";
    });
    const numeric_column_count = numericNames.length;
    const rowLimit = Math.min(rows.length, NUMERIC_CORR_MAX_ROWS);
    if (numeric_column_count < 2) {
      return {
        method: "pearson_pairwise_complete",
        numeric_column_count,
        matrix_columns: [],
        matrix: [],
        matrix_source: [],
        matrix_truncated: false,
        rows_scanned: rowLimit,
        top_pairs: [],
      };
    }
    const matrix_truncated = numericNames.length > NUMERIC_CORR_MAX_COLS;
    const matrix_columns = numericNames.slice(0, NUMERIC_CORR_MAX_COLS);
    const series = matrix_columns.map((h) => {
      const arr = new Array(rowLimit);
      for (let i = 0; i < rowLimit; i++) {
        arr[i] = parseNumericForCorrelation(rows[i][h]);
      }
      return arr;
    });
    const k = matrix_columns.length;
    const matrix_source = Array.from({ length: k }, () => Array(k).fill(null));
    const pairNsMap = new Map();
    for (let i = 0; i < k; i++) {
      matrix_source[i][i] = 1;
      const ai = series[i];
      for (let j = i + 1; j < k; j++) {
        const aj = series[j];
        const xs = [];
        const ys = [];
        for (let r = 0; r < rowLimit; r++) {
          const x = ai[r];
          const y = aj[r];
          if (x != null && y != null) {
            xs.push(x);
            ys.push(y);
          }
        }
        const { r, n } = pearsonPairwiseComplete(xs, ys);
        const rounded = r == null ? null : Number(r.toFixed(4));
        matrix_source[i][j] = rounded;
        matrix_source[j][i] = rounded;
        if (rounded != null) {
          pairNsMap.set(pairCorrelationKey(matrix_columns[i], matrix_columns[j]), n);
        }
      }
    }
    const edits = state.correlationEdits || {};
    const { matrix, overrides_list } = applyCorrelationOverridesToMatrix(matrix_source, matrix_columns, edits);
    const top_pairs = topPairsFromMatrix(matrix, matrix_columns, pairNsMap, 18);
    return {
      method: "pearson_pairwise_complete",
      numeric_column_count,
      matrix_columns,
      matrix,
      matrix_source,
      matrix_truncated,
      rows_scanned: rowLimit,
      top_pairs,
      user_correlation_overrides: overrides_list.length ? overrides_list : undefined,
    };
  }

  function corrHeatBackground(r) {
    if (r == null || Number.isNaN(r)) return "var(--border)";
    const a = Math.min(1, Math.abs(r));
    if (r >= 0) return `rgba(2, 132, 199, ${0.14 + a * 0.58})`;
    return `rgba(220, 38, 38, ${0.14 + a * 0.58})`;
  }

  function formatCorrCell(r) {
    return r == null || Number.isNaN(r) ? "—" : Number(r).toFixed(2);
  }

  function buildCorrelationTableHead(labels) {
    let headRow = `<tr><th class="corr-corner" scope="col"></th>`;
    labels.forEach((L) => {
      headRow += `<th class="corr-axis" scope="col"><span title="${escapeAttr(L.full)}">${escapeHtml(L.short)}</span></th>`;
    });
    return `${headRow}</tr>`;
  }

  /** Read-only table from displayMat (typically observed source). */
  function buildCorrelationTableBodyObserved(cols, labels, displayMat) {
    const k = displayMat.length;
    let body = "";
    for (let i = 0; i < k; i++) {
      body += `<tr><th class="corr-axis" scope="row"><span title="${escapeAttr(labels[i].full)}">${escapeHtml(labels[i].short)}</span></th>`;
      for (let j = 0; j < k; j++) {
        const r = displayMat[i][j];
        const txt = formatCorrCell(r);
        const bg = corrHeatBackground(r);
        const cls = i === j ? "corr-cell corr-cell-diag corr-cell-readonly" : "corr-cell corr-cell-readonly";
        const tip = i === j ? "Diagonal (1)" : `Observed Pearson r = ${txt}`;
        body += `<td class="${cls}" style="background:${bg}" title="${escapeAttr(tip)}">${txt}</td>`;
      }
      body += `</tr>`;
    }
    return body;
  }

  /** Editable synthetic table; colors from synthetic r, delta vs observedMat. */
  function buildCorrelationTableBodySynthetic(cols, labels, synthMat, observedMat) {
    const k = synthMat.length;
    let body = "";
    for (let i = 0; i < k; i++) {
      body += `<tr><th class="corr-axis" scope="row"><span title="${escapeAttr(labels[i].full)}">${escapeHtml(labels[i].short)}</span></th>`;
      for (let j = 0; j < k; j++) {
        const r = synthMat[i][j];
        const r0 = observedMat && observedMat[i] && observedMat[i][j] != null ? observedMat[i][j] : r;
        const txt = formatCorrCell(r);
        const bg = corrHeatBackground(r);
        if (i === j) {
          body += `<td class="corr-cell corr-cell-diag corr-cell-readonly" style="background:${bg}">${txt}</td>`;
        } else {
          const delta =
            r != null &&
            r0 != null &&
            !Number.isNaN(r) &&
            !Number.isNaN(r0) &&
            Math.abs(Number(r) - Number(r0)) > 0.0005;
          const obsTxt = formatCorrCell(r0);
          const tip = `Synthetic target r = ${txt}; observed r = ${obsTxt}. Click to edit.`;
          const cls = `corr-cell corr-cell-editable${delta ? " corr-cell-delta" : ""}`;
          body += `<td class="${cls}" style="background:${bg}" data-corr-a="${escapeAttr(cols[i])}" data-corr-b="${escapeAttr(cols[j])}" title="${escapeAttr(tip)}" role="button" tabindex="0">${txt}</td>`;
        }
      }
      body += `</tr>`;
    }
    return body;
  }

  function buildCorrelationEditSummaryList(cols, srcMat, nameToEnriched) {
    const ed = state.correlationEdits || {};
    const keys = Object.keys(ed);
    if (!keys.length) return "";
    const lis = keys
      .map((key) => {
        const parts = key.split("\x00");
        if (parts.length !== 2) return "";
        const [ca, cb] = parts;
        const ia = cols.indexOf(ca);
        const ib = cols.indexOf(cb);
        let obs = "—";
        if (ia >= 0 && ib >= 0 && srcMat[ia] && srcMat[ia][ib] != null && !Number.isNaN(srcMat[ia][ib])) {
          obs = Number(srcMat[ia][ib]).toFixed(4);
        }
        const syn = ed[key];
        const synStr = syn != null && Number.isFinite(Number(syn)) ? Number(syn).toFixed(4) : String(syn);
        const la = (nameToEnriched.get(ca) && nameToEnriched.get(ca).label_for_synthesis) || ca;
        const lb = (nameToEnriched.get(cb) && nameToEnriched.get(cb).label_for_synthesis) || cb;
        return `<li class="corr-edits-summary-item"><span class="corr-edits-pair">${escapeHtml(la)} ↔ ${escapeHtml(lb)}</span> — observed <em>r</em> <span class="meta-diff-from">${escapeHtml(obs)}</span> <span class="meta-diff-arrow" aria-hidden="true">→</span> synthetic target <span class="meta-diff-to">${escapeHtml(synStr)}</span></li>`;
      })
      .filter(Boolean)
      .join("");
    if (!lis) return "";
    return `<div class="corr-edits-summary"><h4 class="corr-edits-summary-title">Edited correlation targets</h4><ul class="corr-edits-summary-list">${lis}</ul></div>`;
  }

  function renderMetadataCorrelation(el, block, nameToEnriched) {
    if (!el) return;
    if (!block || block.numeric_column_count < 2) {
      const msg =
        block && block.numeric_column_count === 1
          ? "Only one numeric column was detected — add another numeric field to see correlations."
          : "No pairwise correlations to show — profiling did not find two or more numeric columns.";
      el.innerHTML = `<p class="corr-empty">${escapeHtml(msg)}</p>`;
      return;
    }
    const cols = block.matrix_columns || [];
    const mat = block.matrix || [];
    const srcMat = block.matrix_source && block.matrix_source.length ? block.matrix_source : mat;
    const labels = cols.map((name) => {
      const meta = nameToEnriched.get(name);
      const full = (meta && meta.label_for_synthesis) || name;
      const short = full.length > 16 ? `${full.slice(0, 14)}…` : full;
      return { name, short, full };
    });
    const thead = buildCorrelationTableHead(labels);
    const bodySynth = buildCorrelationTableBodySynthetic(cols, labels, mat, srcMat);
    const editSummary = buildCorrelationEditSummaryList(cols, srcMat, nameToEnriched);
    const topPairs = (block.top_pairs || []).slice(0, 12);
    const topList =
      topPairs.length > 0
        ? `<ol class="corr-top-list">${topPairs
            .map((p) => {
              const la = (nameToEnriched.get(p.column_a) && nameToEnriched.get(p.column_a).label_for_synthesis) || p.column_a;
              const lb = (nameToEnriched.get(p.column_b) && nameToEnriched.get(p.column_b).label_for_synthesis) || p.column_b;
              const ia = cols.indexOf(p.column_a);
              const ib = cols.indexOf(p.column_b);
              let rObs = null;
              if (ia >= 0 && ib >= 0 && srcMat[ia] && srcMat[ia][ib] != null) rObs = srcMat[ia][ib];
              const rSynth = p.pearson_r;
              let rSpan;
              if (rObs != null && !Number.isNaN(rObs) && rSynth != null && !Number.isNaN(rSynth) && Math.abs(Number(rObs) - Number(rSynth)) > 0.0005) {
                const so = rObs >= 0 ? "+" : "";
                const ss = rSynth >= 0 ? "+" : "";
                rSpan = `${so}${Number(rObs).toFixed(3)} → ${ss}${Number(rSynth).toFixed(3)}`;
              } else {
                const ss = rSynth >= 0 ? "+" : "";
                rSpan = `${ss}${Number(rSynth).toFixed(3)}`;
              }
              return `<li><span class="corr-pair-r">${escapeHtml(rSpan)}</span> <span class="corr-pair-names">${escapeHtml(la)} ↔ ${escapeHtml(lb)}</span> <span class="corr-pair-n">(${Number(p.pairwise_complete_rows).toLocaleString()} rows)</span></li>`;
            })
            .join("")}</ol>`
        : `<p class="corr-empty corr-empty-tight">No stable pairwise estimates (very few rows share numeric values in both columns).</p>`;
    const foot = [];
    foot.push(`Scanned up to ${Number(block.rows_scanned).toLocaleString()} rows (from the start of the file).`);
    if (block.matrix_truncated) {
      foot.push(
        `The matrix shows the first ${cols.length} numeric column(s) in file order (${block.numeric_column_count} numeric total).`
      );
    }
    el.innerHTML = `
      <div class="corr-legend" aria-hidden="true">
        <span class="corr-legend-item"><span class="corr-swatch neg"></span> Negative r</span>
        <span class="corr-legend-item"><span class="corr-swatch neu"></span> Weak / diagonal</span>
        <span class="corr-legend-item"><span class="corr-swatch pos"></span> Positive r</span>
      </div>
      <p class="corr-single-caption">One matrix of <strong>synthetic targets</strong> (what generation uses). Off-diagonal cells you change are highlighted; hover a cell for observed <em>r</em> vs target.</p>
      <div class="corr-matrix-single">
        <h4 class="corr-matrix-label corr-matrix-label-solo">Synthetic correlation targets</h4>
        <div class="corr-table-scroll">
          <table class="corr-table" aria-label="Synthetic Pearson correlation targets"><thead>${thead}</thead><tbody>${bodySynth}</tbody></table>
        </div>
      </div>
      ${editSummary}
      <p class="corr-footnote">${escapeHtml(foot.join(" "))}</p>
      <div class="corr-top-wrap">
        <h4 class="corr-top-title">Strongest linear relationships (by |synthetic r|)</h4>
        <p class="corr-top-sub">When a pair was edited, the list shows <strong>observed → synthetic</strong>; otherwise a single value (both match).</p>
        ${topList}
      </div>`;
  }

  function buildSyntheticMetadataPayload(headers, rows, colStatsPre) {
    const colStats = colStatsPre || inferColumnStats(headers, rows);
    const columns = enrichColumnsForMetadata(headers, rows, colStats);
    const inc = getColumnIncludeMap();
    const excluded = headers.filter((h) => inc[h] === false);
    const numeric_correlation_pearson = buildNumericCorrelationBlock(headers, rows, colStats);
    return {
      schema_version: METADATA_SCHEMA_VERSION,
      generated_at_utc: new Date().toISOString(),
      source_file_name: state.fileName,
      dataset_summary: {
        row_count: rows.length,
        column_count: headers.length,
        columns_included_in_schema: headers.filter((h) => inc[h] !== false).length,
        column_names_excluded_from_schema: excluded,
      },
      hygiene_signals_in_metadata: (state.issues || []).slice(0, 20).map((i) => ({
        severity: i.sev,
        title: i.title,
        detail: i.detail,
      })),
      columns,
      numeric_correlation_pearson,
      metadata_step_notes: (() => {
        const o = {};
        METADATA_NOTE_SECTIONS.forEach((k) => {
          const v = state.metadataSectionNotes && String(state.metadataSectionNotes[k] || "").trim();
          if (v) o[k] = v;
        });
        return Object.keys(o).length ? o : undefined;
      })(),
      accepted_ai_metadata_guidance: (() => {
        const acc = state.metadataAiAccepted;
        if (!Array.isArray(acc) || !acc.length) return undefined;
        return acc.map((rec) => ({
          id: rec.id,
          accepted_at_utc: rec.accepted_at_utc,
          title: String(rec.title || "").slice(0, 200),
          detail: String(rec.detail || "").slice(0, 800),
          importance: rec.importance === "high" || rec.importance === "low" ? rec.importance : "medium",
          suggested_action: rec.suggested_action ? String(rec.suggested_action).slice(0, 400) : undefined,
          related_columns: Array.isArray(rec.related_columns) ? rec.related_columns.filter((c) => headers.includes(c)) : [],
          agent_summary_snapshot: rec.agent_summary_snapshot ? String(rec.agent_summary_snapshot).slice(0, 500) : undefined,
        }));
      })(),
      transparency: {
        how_source_becomes_metadata: [
          "The CSV is parsed locally. We never send the full file to a model unless you run the optional inspection API call (sample rows only).",
          "Each column is profiled: type guess, counts, missingness, distinctness, and either numeric summaries or top category frequencies.",
          "This object is the descriptive metadata package: it is what a synthetic data engine uses to reproduce shape and marginals without copying raw rows.",
          "Use the dashboard to include or exclude columns, then Edit to adjust labels, schema type intent, and notes — those adjustments are written into the JSON package.",
          "Pairwise Pearson correlations summarize linear co-movement between numeric columns on rows where both values parse as numbers (see numeric_correlation_pearson).",
          "matrix holds synthesis targets; matrix_source keeps observed r for the same column order. Click heatmap cells to override linear association targets for synthetic data.",
          "Per-column Edit metadata can set synthetic numeric shape/range/moments and categorical mix strategies — see synthetic_numeric_targets and synthetic_categorical_targets on each column.",
          "metadata_step_notes captures optional rationale typed into each accordion on the metadata screen.",
          "accepted_ai_metadata_guidance lists coach recommendations you explicitly accepted; per-column copies appear as accepted_ai_coach_guidance for generators that honor field-level intent.",
          "For numeric columns, synthetic_numeric_targets.custom_discretized_histogram can carry rebalanced bin mass from the Distributions UI (same equal-width bins as the profiled sample histogram).",
        ],
      },
    };
  }

  let metadataViewEventsBound = false;
  let metadataModalColumn = null;
  let metadataModalColStat = null;
  let corrModalColA = null;
  let corrModalColB = null;

  /** Active pointer-drag on a numeric histogram bin (metadata distributions). */
  let numHistDrag = null;

  function bindMetadataViewEvents() {
    if (metadataViewEventsBound || !els.viewMetadata) return;
    metadataViewEventsBound = true;
    els.viewMetadata.addEventListener("change", (e) => {
      const t = e.target;
      if (t.matches && t.matches("select.dist-num-shape-select[data-num-shape-col]")) {
        const col = t.getAttribute("data-num-shape-col");
        if (!col || !state.headers.includes(col)) return;
        const v = t.value;
        if (!state.columnMetadataEdits) state.columnMetadataEdits = {};
        const nextEd = { ...getEditForCol(col) };
        if (!v || v === "auto") delete nextEd.synthDist;
        else nextEd.synthDist = v;
        pruneEmptyColumnEdit(col, nextEd);
        saveSession();
        renderMetadata();
        return;
      }
      if (t.matches && t.matches("select.dist-cat-strategy-select[data-cat-strat-col]")) {
        const col = t.getAttribute("data-cat-strat-col");
        if (!col || !state.headers.includes(col)) return;
        applyDistributionSectionCatStrategy(col, t.value || "auto");
        return;
      }
      if (t.matches && t.matches("input.dist-cat-merge-input[data-cat-merge-col]")) {
        const col = t.getAttribute("data-cat-merge-col");
        if (!col || !state.headers.includes(col)) return;
        if (getEditForCol(col).synthCatMode !== "merge_rare") return;
        applyDistributionSectionCatStrategy(col, "merge_rare", t.value);
        return;
      }
      if (!t.classList || !t.classList.contains("meta-include-cb")) return;
      const col = t.getAttribute("data-col");
      if (!col) return;
      if (!state.columnInclude) state.columnInclude = {};
      state.columnInclude[col] = t.checked;
      saveSession();
      renderMetadata();
    });
    function handleDistributionSliderFromEvent(e) {
      const t = e.target;
      if (!t || !t.matches || !t.matches("input.dist-edit-slider[data-hist-kind][data-hist-slider-col][data-hist-slider-idx]")) return;
      if (t.disabled) return;
      const kind = t.getAttribute("data-hist-kind");
      const col = t.getAttribute("data-hist-slider-col");
      const idxRaw = t.getAttribute("data-hist-slider-idx");
      if (!col || idxRaw == null) return;
      const idx = Number(idxRaw);
      const val = Number(t.value);
      if (!Number.isFinite(idx) || !Number.isFinite(val)) return;
      if (kind === "num") applyNumericHistogramSliderChange(col, idx, val / 100);
      else {
        let catBuckets = 7;
        const art = t.closest("[data-dist-cat-buckets]");
        if (art) {
          const raw = art.getAttribute("data-dist-cat-buckets");
          const n = Number(raw);
          if (Number.isFinite(n) && n >= 3) catBuckets = n;
        }
        applyDistributionSliderChange(col, idx, val / 100, catBuckets);
      }
    }
    els.viewMetadata.addEventListener("input", handleDistributionSliderFromEvent);
    els.viewMetadata.addEventListener("change", handleDistributionSliderFromEvent);

    els.viewMetadata.addEventListener("pointerdown", (e) => {
      const hit = e.target.closest(".dist-num-hist-hit");
      if (!hit) return;
      e.preventDefault();
      const col = hit.getAttribute("data-num-hist-col");
      const idx = Number(hit.getAttribute("data-num-hist-idx"));
      const pRaw = hit.getAttribute("data-num-hist-p");
      const startP = Number(pRaw);
      if (!col || !Number.isFinite(idx) || !Number.isFinite(startP)) return;
      const wrap = hit.closest(".dist-num-interactive-chart");
      if (!wrap) return;
      const plotH = Number(wrap.getAttribute("data-plot-h")) || 168;
      numHistDrag = { pointerId: e.pointerId, col, idx, startY: e.clientY, startP, plotH };
      try {
        els.viewMetadata.setPointerCapture(e.pointerId);
      } catch {
        /* ignore */
      }
    });
    els.viewMetadata.addEventListener("pointermove", (e) => {
      if (!numHistDrag || e.pointerId !== numHistDrag.pointerId) return;
      e.preventDefault();
      const dy = e.clientY - numHistDrag.startY;
      const plotH = numHistDrag.plotH;
      const newTarget = Math.max(0, Math.min(1, numHistDrag.startP - dy / plotH));
      applyNumericHistogramSliderChange(numHistDrag.col, numHistDrag.idx, newTarget);
    });
    els.viewMetadata.addEventListener("pointerup", (e) => {
      if (!numHistDrag || e.pointerId !== numHistDrag.pointerId) return;
      try {
        els.viewMetadata.releasePointerCapture(e.pointerId);
      } catch {
        /* ignore */
      }
      numHistDrag = null;
    });
    els.viewMetadata.addEventListener("pointercancel", (e) => {
      if (!numHistDrag || e.pointerId !== numHistDrag.pointerId) return;
      numHistDrag = null;
    });

    els.viewMetadata.addEventListener("click", (e) => {
      const cell = e.target.closest(".corr-cell-editable");
      if (cell) {
        const a = cell.getAttribute("data-corr-a");
        const b = cell.getAttribute("data-corr-b");
        if (a && b) openCorrelationEditModal(a, b);
        return;
      }
      const btn = e.target.closest(".meta-dash-edit-btn");
      if (!btn) return;
      const col = btn.getAttribute("data-col");
      if (col) openMetadataEditModal(col);
    });
    els.viewMetadata.addEventListener("keydown", (e) => {
      if (e.key !== "Enter" && e.key !== " ") return;
      const cell = e.target.closest(".corr-cell-editable");
      if (!cell) return;
      e.preventDefault();
      const a = cell.getAttribute("data-corr-a");
      const b = cell.getAttribute("data-corr-b");
      if (a && b) openCorrelationEditModal(a, b);
    });
  }

  let metadataSectionNotesBound = false;
  function bindMetadataSectionNotesOnce() {
    if (metadataSectionNotesBound || !els.viewMetadata) return;
    metadataSectionNotesBound = true;
    els.viewMetadata.addEventListener("input", (e) => {
      const ta = e.target.closest("[data-meta-note-section]");
      if (!ta) return;
      const key = ta.getAttribute("data-meta-note-section");
      if (!key || !METADATA_NOTE_SECTIONS.includes(key)) return;
      if (!state.metadataSectionNotes) state.metadataSectionNotes = {};
      state.metadataSectionNotes[key] = ta.value;
      saveSession();
    });
  }

  function syncMetadataSectionNotesInputs() {
    if (!els.viewMetadata) return;
    METADATA_NOTE_SECTIONS.forEach((k) => {
      const el = els.viewMetadata.querySelector(`[data-meta-note-section="${k}"]`);
      if (!el) return;
      const v = (state.metadataSectionNotes && state.metadataSectionNotes[k]) || "";
      if (el.value !== v) el.value = v;
    });
  }

  function syncMetadataSynthFieldsets() {
    if (!els.metadataEditNumericFieldset || !metadataModalColumn || !metadataModalColStat) return;
    const ed = { ...getEditForCol(metadataModalColumn), treatAsType: els.metadataEditType.value === "auto" ? "" : els.metadataEditType.value };
    const kind = effectiveColumnKind(metadataModalColStat, ed);
    els.metadataEditNumericFieldset.hidden = kind !== "numeric";
    els.metadataEditCategoricalFieldset.hidden = kind !== "text";
    const mode = els.metadataEditCatMode ? els.metadataEditCatMode.value : "auto";
    if (els.metadataEditCatMergeWrap) els.metadataEditCatMergeWrap.hidden = mode !== "merge_rare";
    if (els.metadataEditCatCustomWrap) els.metadataEditCatCustomWrap.hidden = mode !== "custom";
  }

  function closeMetadataEditModal() {
    metadataModalColumn = null;
    metadataModalColStat = null;
    if (els.metadataEditModal) els.metadataEditModal.classList.add("hidden");
  }

  function openMetadataEditModal(colName) {
    if (!els.metadataEditModal || !els.metadataEditLabel) return;
    metadataModalColumn = colName;
    metadataModalColStat = inferColumnStats(state.headers, state.rows).find((x) => x.name === colName);
    if (!metadataModalColStat) return;
    const ed = getEditForCol(colName);
    if (els.metadataEditTitle) els.metadataEditTitle.textContent = colName;
    els.metadataEditLabel.value = ed.displayLabel && ed.displayLabel.trim() ? ed.displayLabel : colName;
    els.metadataEditType.value = ed.treatAsType === "numeric" || ed.treatAsType === "text" ? ed.treatAsType : "auto";
    els.metadataEditNote.value = ed.synthesisNote || "";
    if (els.metadataEditDistribution) els.metadataEditDistribution.value = ed.synthDist && ed.synthDist !== "auto" ? ed.synthDist : "auto";
    if (els.metadataEditSynthMin) els.metadataEditSynthMin.value = ed.synthMin != null ? String(ed.synthMin) : "";
    if (els.metadataEditSynthMax) els.metadataEditSynthMax.value = ed.synthMax != null ? String(ed.synthMax) : "";
    if (els.metadataEditSynthMean) els.metadataEditSynthMean.value = ed.synthMean != null ? String(ed.synthMean) : "";
    if (els.metadataEditSynthVariance) els.metadataEditSynthVariance.value = ed.synthVariance != null ? String(ed.synthVariance) : "";
    if (els.metadataEditCatMode) els.metadataEditCatMode.value = ed.synthCatMode && ed.synthCatMode !== "auto" ? ed.synthCatMode : "auto";
    if (els.metadataEditCatMergePct) els.metadataEditCatMergePct.value = ed.synthCatMergePct != null ? String(ed.synthCatMergePct) : "";
    if (els.metadataEditCatCustom) els.metadataEditCatCustom.value = ed.synthCatCustom != null ? String(ed.synthCatCustom) : "";

    const observedNum =
      metadataModalColStat.inferred === "numeric" && metadataModalColStat.numericSample.length
        ? computeNumericSummaryFromSamples(metadataModalColStat.numericSample)
        : null;
    if (els.metadataEditNumericObserved) {
      els.metadataEditNumericObserved.textContent = observedNum
        ? `Observed from file: min ${observedNum.min}, max ${observedNum.max}, mean ${observedNum.mean}, std ${observedNum.std}.`
        : "No numeric sample from the file for this column (e.g. schema forced to numeric on mostly text values). You can still set targets for the generator.";
    }
    const catTop = topCategoriesObserved(state.rows, colName, 5);
    if (els.metadataEditCategoricalObserved) {
      els.metadataEditCategoricalObserved.textContent = catTop.length
        ? `Top observed values: ${catTop.map((t) => `${t.value} (${(t.proportion * 100).toFixed(1)}%)`).join(" · ")}.`
        : "No non-empty categorical values detected.";
    }
    syncMetadataSynthFieldsets();
    els.metadataEditModal.classList.remove("hidden");
    els.metadataEditModal.focus();
    els.metadataEditLabel.focus();
  }

  function columnEditHasSynthOverrides(ed) {
    if (!ed || typeof ed !== "object") return false;
    if (ed.synthDist && ed.synthDist !== "auto") return true;
    if (["synthMin", "synthMax", "synthMean", "synthVariance"].some((k) => String(ed[k] || "").trim())) return true;
    if (String(ed.synthNumHistCustom || "").trim()) return true;
    if (ed.synthCatMode && ed.synthCatMode !== "auto") return true;
    return false;
  }

  function hasColumnSynthOverrides() {
    const m = state.columnMetadataEdits;
    if (!m || typeof m !== "object") return false;
    return Object.keys(m).some((col) => columnEditHasSynthOverrides(m[col]));
  }

  function hasCorrelationSynthOverrides() {
    const c = state.correlationEdits;
    if (!c || typeof c !== "object") return false;
    return Object.keys(c).length > 0;
  }

  function hasAnySyntheticOverrides() {
    return hasColumnSynthOverrides() || hasCorrelationSynthOverrides();
  }

  function hasColumnIncludeExclusion() {
    return !!(state.columnInclude && state.headers.some((h) => state.columnInclude[h] === false));
  }

  function hasColumnMetadataEditsKeys() {
    return !!(state.columnMetadataEdits && Object.keys(state.columnMetadataEdits).length > 0);
  }

  function hasMetadataPackageDrift() {
    return (
      hasColumnIncludeExclusion() ||
      hasColumnMetadataEditsKeys() ||
      hasCorrelationSynthOverrides() ||
      hasColumnSynthOverrides() ||
      hasAcceptedAiMetadataGuidance()
    );
  }

  function hasAcceptedAiMetadataGuidance() {
    return Array.isArray(state.metadataAiAccepted) && state.metadataAiAccepted.length > 0;
  }

  function metadataSectionShowRationaleNote(sectionId) {
    const incExc = hasColumnIncludeExclusion();
    const colEd = hasColumnMetadataEditsKeys();
    const corr = hasCorrelationSynthOverrides();
    const colSynth = hasColumnSynthOverrides();
    const anySynth = hasAnySyntheticOverrides();
    const aiCoach =
      hasAcceptedAiMetadataGuidance() ||
      !!(state.metadataAiLastRun && state.metadataAiLastRun.items && state.metadataAiLastRun.items.length);
    if (sectionId === "dashboard") return anySynth;
    if (sectionId === "ai") return aiCoach;
    if (sectionId === "summary") return incExc || colEd;
    if (sectionId === "hygiene") return false;
    if (sectionId === "columns") return incExc || colEd;
    if (sectionId === "correlations") return corr;
    if (sectionId === "distributions") return hasDistributionSectionOverrides();
    if (sectionId === "json") return hasMetadataPackageDrift();
    return false;
  }

  function updateMetadataSectionNotesVisibility() {
    if (!els.viewMetadata) return;
    METADATA_NOTE_SECTIONS.forEach((id) => {
      const wrap = els.viewMetadata.querySelector(`[data-meta-note-wrap="${id}"]`);
      if (!wrap) return;
      wrap.classList.toggle("hidden", !metadataSectionShowRationaleNote(id));
    });
  }

  function changeReviewSectionHasChanges(sectionId) {
    const incExc = hasColumnIncludeExclusion();
    const colEd = hasColumnMetadataEditsKeys();
    const corr = hasCorrelationSynthOverrides();
    const colSynth = hasColumnSynthOverrides();
    const anySynth = hasAnySyntheticOverrides();
    const names = collectColumnsWithActivity(state.headers, getColumnIncludeMap(), state.columnMetadataEdits || {});
    if (sectionId === "dashboard") return anySynth;
    if (sectionId === "ai") return hasAcceptedAiMetadataGuidance();
    if (sectionId === "summary") return incExc || colEd;
    if (sectionId === "hygiene") return false;
    if (sectionId === "columns") return names.length > 0;
    if (sectionId === "correlations") return corr;
    if (sectionId === "distributions") return hasDistributionSectionOverrides();
    if (sectionId === "json") return hasMetadataPackageDrift();
    return false;
  }

  function countCorrelationOverridesForColumn(colName) {
    const ed = state.correlationEdits;
    if (!ed || typeof ed !== "object") return 0;
    let n = 0;
    for (const k of Object.keys(ed)) {
      const parts = k.split("\x00");
      if (parts.length === 2 && (parts[0] === colName || parts[1] === colName)) n++;
    }
    return n;
  }

  function getColStatOrFallback(colName, colStats) {
    return (
      colStats.find((s) => s.name === colName) || {
        name: colName,
        inferred: "text",
        numericSample: [],
        nonNull: 0,
        missing: 0,
        unique: 0,
      }
    );
  }

  function collectColumnsWithActivity(headers, inc, edits) {
    const set = new Set();
    headers.forEach((h) => {
      if (inc[h] === false) set.add(h);
    });
    Object.keys(edits || {}).forEach((h) => set.add(h));
    const ced = state.correlationEdits || {};
    Object.keys(ced).forEach((k) => {
      const parts = k.split("\x00");
      if (parts.length === 2) {
        set.add(parts[0]);
        set.add(parts[1]);
      }
    });
    columnsTouchedByAcceptedAi().forEach((h) => set.add(h));
    return [...set].sort((a, b) => a.localeCompare(b));
  }

  function buildColumnEditDiffItems(colName, colStat, ed, includedInSchema) {
    const items = [];
    if (!includedInSchema) {
      items.push({ field: "Schema inclusion", from: "Included", to: "Excluded from synthesis metadata" });
    }
    if (!ed || typeof ed !== "object") return items;
    const dlab = ed.displayLabel && String(ed.displayLabel).trim();
    if (dlab && dlab !== colName) {
      items.push({ field: "Display label", from: colName, to: dlab });
    }
    if (ed.treatAsType === "numeric" || ed.treatAsType === "text") {
      items.push({
        field: "Schema type intent",
        from: `Auto (profile: ${colStat.inferred})`,
        to: ed.treatAsType,
      });
    }
    if (ed.synthesisNote && String(ed.synthesisNote).trim()) {
      const sn = String(ed.synthesisNote).trim();
      items.push({
        field: "Notes for synthesis",
        from: "(none)",
        to: sn.length > 280 ? `${sn.slice(0, 280)}…` : sn,
      });
    }
    if (ed.synthDist && ed.synthDist !== "auto") {
      items.push({ field: "Synthetic distribution shape", from: "auto", to: String(ed.synthDist) });
    }
    if (String(ed.synthMin || "").trim()) {
      items.push({ field: "Synthetic range min", from: "(unset)", to: String(ed.synthMin).trim() });
    }
    if (String(ed.synthMax || "").trim()) {
      items.push({ field: "Synthetic range max", from: "(unset)", to: String(ed.synthMax).trim() });
    }
    if (String(ed.synthMean || "").trim()) {
      items.push({ field: "Synthetic mean", from: "(unset)", to: String(ed.synthMean).trim() });
    }
    if (String(ed.synthVariance || "").trim()) {
      items.push({ field: "Synthetic variance", from: "(unset)", to: String(ed.synthVariance).trim() });
    }
    if (ed.synthCatMode && ed.synthCatMode !== "auto") {
      items.push({ field: "Category strategy", from: "auto", to: String(ed.synthCatMode) });
    }
    if (String(ed.synthCatMergePct || "").trim()) {
      items.push({ field: "Merge rare below proportion", from: "(unset)", to: String(ed.synthCatMergePct).trim() });
    }
    if (String(ed.synthCatCustom || "").trim()) {
      const full = String(ed.synthCatCustom).trim();
      const snip = full.slice(0, 120);
      items.push({
        field: "Custom category mix (JSON)",
        from: "(unset)",
        to: snip.length < full.length ? `${snip}…` : snip,
      });
    }
    if (String(ed.synthNumHistCustom || "").trim()) {
      const full = String(ed.synthNumHistCustom).trim();
      const snip = full.slice(0, 120);
      items.push({
        field: "Numeric histogram (custom bin targets)",
        from: "(observed binning only)",
        to: snip.length < full.length ? `${snip}…` : snip,
      });
    }
    const aiHits = acceptedAiGuidanceForColumn(colName);
    for (const h of aiHits) {
      const to = `${h.title}${h.detail ? ` — ${h.detail.length > 160 ? `${h.detail.slice(0, 158)}…` : h.detail}` : ""}`;
      items.push({ field: "AI coach (accepted)", from: "(no accepted guidance)", to });
    }
    return items;
  }

  function briefColumnChangeSummary(colName, colStat, ed, includedInSchema) {
    const parts = [];
    if (!includedInSchema) parts.push("Excluded from schema");
    if (ed && typeof ed === "object") {
      const dlab = ed.displayLabel && String(ed.displayLabel).trim();
      if (dlab && dlab !== colName) parts.push(`Label → ${dlab.length > 24 ? `${dlab.slice(0, 22)}…` : dlab}`);
      if (ed.treatAsType === "numeric" || ed.treatAsType === "text") parts.push(`Type → ${ed.treatAsType}`);
      if (ed.synthesisNote && String(ed.synthesisNote).trim()) parts.push("Synthesis note");
      if (columnEditHasSynthOverrides(ed)) parts.push("Synthetic targets");
    }
    const nc = countCorrelationOverridesForColumn(colName);
    if (nc) parts.push(nc === 1 ? "1 correlation override" : `${nc} correlation overrides`);
    const aiHint = briefAcceptedAiColumnHint(colName);
    if (aiHint) parts.push(aiHint);
    return parts.join(" · ");
  }

  function changeReviewCarriedNoteHtml(sectionId) {
    const raw = (state.metadataSectionNotes && state.metadataSectionNotes[sectionId]) || "";
    const t = raw.trim();
    if (!t) return "";
    return `<div class="meta-changes-carried-note"><span class="meta-changes-carried-label">From metadata step</span><p class="meta-changes-carried-body">${escapeHtml(t)}</p></div>`;
  }

  function changeReviewSectionHtml(sectionId, title, innerBodyHtml) {
    const carried = changeReviewCarriedNoteHtml(sectionId);
    const noteWrap = `<div class="meta-section-note meta-changes-review-note"><label class="meta-section-note-label" for="chg-review-note-${sectionId}">Notes on why you made changes in this section (optional)</label><textarea id="chg-review-note-${sectionId}" class="meta-section-note-input" data-change-review-note="${escapeAttr(sectionId)}" rows="2" placeholder="Summarize rationale for reviewers or auditors…"></textarea></div>`;
    return `<section class="panel meta-changes-block meta-changes-section" data-change-section="${escapeAttr(sectionId)}"><h3 class="panel-title">${escapeHtml(title)}</h3>${carried}<div class="meta-changes-section-body">${innerBodyHtml}</div>${noteWrap}</section>`;
  }

  function buildColumnsChangeReviewBody(colStats) {
    const inc = getColumnIncludeMap();
    const edits = state.columnMetadataEdits || {};
    const names = collectColumnsWithActivity(state.headers, inc, edits);
    const blocks = [];
    if (state.headers.some((h) => inc[h] === false)) {
      blocks.push(
        `<div class="meta-changes-revert-row no-print"><button type="button" class="btn btn-ghost btn-sm" data-revert="exclusions">Revert all exclusions (include every column)</button></div>`
      );
    }
    if (!names.length) {
      blocks.push(`<p class="panel-lead meta-changes-lead-tight">No column inclusions, labels, or field metadata were changed.</p>`);
      return blocks.join("");
    }
    for (const col of names) {
      const st = getColStatOrFallback(col, colStats);
      const ed = getEditForCol(col);
      const included = inc[col] !== false;
      let items = buildColumnEditDiffItems(col, st, ed, included);
      const nc = countCorrelationOverridesForColumn(col);
      if (!items.length && nc > 0) {
        items.push({
          field: "Correlation targets",
          from: "(no label/type edits on this column)",
          to: `${nc} pair override${nc > 1 ? "s" : ""} (see Correlations)`,
        });
      }
      if (!items.length) continue;
      const lis = items
        .map(
          (it) =>
            `<li class="meta-diff-row"><span class="meta-diff-field">${escapeHtml(it.field)}</span> <span class="meta-diff-pair"><span class="meta-diff-from">${escapeHtml(it.from)}</span><span class="meta-diff-arrow" aria-hidden="true">→</span><span class="meta-diff-to">${escapeHtml(it.to)}</span></span></li>`
        )
        .join("");
      blocks.push(
        `<div class="meta-changes-col-block"><h4 class="meta-changes-col-title"><code>${escapeHtml(col)}</code></h4><ul class="meta-changes-list meta-diff-list">${lis}</ul><div class="meta-changes-revert-row no-print"><button type="button" class="btn btn-ghost btn-sm" data-revert-column="${escapeAttr(col)}">Revert changes for this column</button></div></div>`
      );
    }
    return blocks.join("");
  }

  function buildCorrelationsChangeReviewBody(colStats) {
    const corrEd = state.correlationEdits || {};
    const corrKeys = Object.keys(corrEd);
    if (!corrKeys.length) {
      return `<p class="panel-lead meta-changes-lead-tight">No Pearson correlation targets were overridden.</p>`;
    }
    const block = buildNumericCorrelationBlock(state.headers, state.rows, colStats);
    const cols = block.matrix_columns || [];
    const src = block.matrix_source || [];
    const rows = corrKeys
      .map((key) => {
        const parts = key.split("\x00");
        if (parts.length !== 2) return "";
        const [ca, cb] = parts;
        const ia = cols.indexOf(ca);
        const ib = cols.indexOf(cb);
        let obs = "—";
        if (ia >= 0 && ib >= 0 && src[ia] && src[ia][ib] != null && !Number.isNaN(src[ia][ib])) {
          obs = Number(src[ia][ib]).toFixed(4);
        }
        const syn = corrEd[key];
        const synStr = syn != null && Number.isFinite(Number(syn)) ? Number(syn).toFixed(4) : String(syn);
        return `<tr><td><code>${escapeHtml(ca)}</code></td><td><code>${escapeHtml(cb)}</code></td><td class="meta-diff-from-cell">${escapeHtml(obs)}</td><td class="meta-diff-to-cell"><strong>${escapeHtml(synStr)}</strong></td><td class="no-print"><button type="button" class="btn btn-ghost btn-sm" data-revert-corr-a="${escapeAttr(ca)}" data-revert-corr-b="${escapeAttr(cb)}">Revert</button></td></tr>`;
      })
      .filter(Boolean)
      .join("");
    if (!rows) {
      return `<p class="panel-lead meta-changes-lead-tight">Correlation overrides exist but are outside the current numeric matrix window.</p>`;
    }
    return `<div class="meta-changes-revert-row no-print"><button type="button" class="btn btn-ghost btn-sm" data-revert="correlations">Revert all correlation targets</button></div><p class="panel-lead">Each row shows the <strong>observed</strong> Pearson <em>r</em> from your file and the <strong>synthetic target</strong> stored for generation.</p><div class="table-wrap"><table class="data-table meta-changes-table"><thead><tr><th>Column A</th><th>Column B</th><th>Before (observed <em>r</em>)</th><th>After (synthetic target)</th><th class="no-print"></th></tr></thead><tbody>${rows}</tbody></table></div>`;
  }

  function syncChangeReviewNoteTextareas() {
    if (!els.metadataChangesRoot) return;
    els.metadataChangesRoot.querySelectorAll("[data-change-review-note]").forEach((ta) => {
      const k = ta.getAttribute("data-change-review-note");
      if (!k) return;
      const v = (state.metadataChangeReviewNotes && state.metadataChangeReviewNotes[k]) || "";
      if (ta.value !== v) ta.value = v;
    });
  }

  function stripSyntheticFieldsFromColumnEdit(o) {
    const x = { ...(o || {}) };
    COLUMN_SYNTH_EDIT_KEYS.forEach((k) => {
      delete x[k];
    });
    return x;
  }

  function applyStripSyntheticFromAllColumnEdits() {
    if (!state.columnMetadataEdits || typeof state.columnMetadataEdits !== "object") return;
    Object.keys(state.columnMetadataEdits).forEach((col) => {
      const stripped = stripSyntheticFieldsFromColumnEdit(state.columnMetadataEdits[col]);
      pruneEmptyColumnEdit(col, stripped);
    });
  }

  function resetColumnSyntheticOverrides() {
    applyStripSyntheticFromAllColumnEdits();
    saveSession();
    renderMetadata();
    toast("Column synthetic overrides cleared.");
  }

  function resetCorrelationSyntheticOverrides() {
    state.correlationEdits = {};
    saveSession();
    renderMetadata();
    toast("Correlation synthetic overrides cleared.");
  }

  function resetAllSyntheticOverrides() {
    applyStripSyntheticFromAllColumnEdits();
    state.correlationEdits = {};
    saveSession();
    renderMetadata();
    toast("All synthetic overrides cleared.");
  }

  function repositionSyntheticResetButtons() {
    const col = hasColumnSynthOverrides();
    const corr = hasCorrelationSynthOverrides();
    const any = col || corr;

    const dash = $("meta-section-dashboard");
    const colDet = $("meta-section-columns");
    const corrDet = $("meta-section-correlations");

    const openAreaAll = $("meta-reset-all-open-area");
    const sumSlotAll = $("meta-reset-all-summary-slot");
    const openSlotAll = $("meta-reset-all-open-slot");
    const btnAll = els.btnResetAllSynth;
    if (btnAll) {
      if (!any) {
        btnAll.classList.add("hidden");
        if (openAreaAll) openAreaAll.classList.add("hidden");
      } else {
        btnAll.classList.remove("hidden");
        if (dash && !dash.open && sumSlotAll) {
          sumSlotAll.appendChild(btnAll);
          if (openAreaAll) openAreaAll.classList.add("hidden");
        } else if (dash && dash.open && openSlotAll) {
          openSlotAll.appendChild(btnAll);
          if (openAreaAll) openAreaAll.classList.remove("hidden");
        }
      }
    }

    const btnCol = els.btnResetColumnsSynth;
    const colSum = $("meta-columns-reset-summary-slot");
    const colOpen = $("meta-columns-reset-open-slot");
    const toolCol = $("meta-columns-open-toolbar");
    if (btnCol) {
      if (!col) {
        btnCol.classList.add("hidden");
        if (toolCol) toolCol.classList.add("hidden");
      } else {
        btnCol.classList.remove("hidden");
        if (colDet && !colDet.open && colSum) {
          colSum.appendChild(btnCol);
          if (toolCol) toolCol.classList.add("hidden");
        } else if (colDet && colDet.open && colOpen) {
          colOpen.appendChild(btnCol);
          if (toolCol) toolCol.classList.remove("hidden");
        }
      }
    }

    const btnCorr = els.btnResetCorrSynth;
    const corrSum = $("meta-corr-reset-summary-slot");
    const corrOpen = $("meta-corr-reset-open-slot");
    const toolCorr = $("meta-corr-open-toolbar");
    if (btnCorr) {
      if (!corr) {
        btnCorr.classList.add("hidden");
        if (toolCorr) toolCorr.classList.add("hidden");
      } else {
        btnCorr.classList.remove("hidden");
        if (corrDet && !corrDet.open && corrSum) {
          corrSum.appendChild(btnCorr);
          if (toolCorr) toolCorr.classList.add("hidden");
        } else if (corrDet && corrDet.open && corrOpen) {
          corrOpen.appendChild(btnCorr);
          if (toolCorr) toolCorr.classList.remove("hidden");
        }
      }
    }
  }

  let metadataSectionToggleBound = false;

  function bindMetadataSectionDetailsToggleOnce() {
    if (metadataSectionToggleBound || !els.viewMetadata) return;
    metadataSectionToggleBound = true;
    els.viewMetadata.addEventListener("toggle", (e) => {
      if (e.target && e.target.classList && e.target.classList.contains("meta-section-panel")) {
        repositionSyntheticResetButtons();
      }
    });
  }

  function bindSyntheticResetButtonsOnce() {
    if (els.btnResetAllSynth && !els.btnResetAllSynth.dataset.bound) {
      els.btnResetAllSynth.dataset.bound = "1";
      els.btnResetAllSynth.addEventListener("click", () => resetAllSyntheticOverrides());
    }
    if (els.btnResetColumnsSynth && !els.btnResetColumnsSynth.dataset.bound) {
      els.btnResetColumnsSynth.dataset.bound = "1";
      els.btnResetColumnsSynth.addEventListener("click", () => resetColumnSyntheticOverrides());
    }
    if (els.btnResetCorrSynth && !els.btnResetCorrSynth.dataset.bound) {
      els.btnResetCorrSynth.dataset.bound = "1";
      els.btnResetCorrSynth.addEventListener("click", () => resetCorrelationSyntheticOverrides());
    }
  }

  function pruneEmptyColumnEdit(col, o) {
    const x = { ...o };
    if (!x.displayLabel) delete x.displayLabel;
    if (!x.treatAsType) delete x.treatAsType;
    if (!x.synthesisNote) delete x.synthesisNote;
    if (!x.synthDist || x.synthDist === "auto") delete x.synthDist;
    if (!String(x.synthMin || "").trim()) delete x.synthMin;
    if (!String(x.synthMax || "").trim()) delete x.synthMax;
    if (!String(x.synthMean || "").trim()) delete x.synthMean;
    if (!String(x.synthVariance || "").trim()) delete x.synthVariance;
    if (!String(x.synthNumHistCustom || "").trim()) delete x.synthNumHistCustom;
    if (!x.synthCatMode || x.synthCatMode === "auto") {
      delete x.synthCatMode;
      delete x.synthCatMergePct;
      delete x.synthCatCustom;
    } else {
      if (x.synthCatMode !== "merge_rare" || !String(x.synthCatMergePct || "").trim()) delete x.synthCatMergePct;
      if (x.synthCatMode !== "custom") delete x.synthCatCustom;
    }
    const keys = Object.keys(x).filter((k) => x[k] != null && x[k] !== "");
    if (!keys.length) delete state.columnMetadataEdits[col];
    else state.columnMetadataEdits[col] = x;
  }

  function saveMetadataEditModal() {
    const col = metadataModalColumn;
    if (!col || !els.metadataEditLabel) return;
    if (!state.columnMetadataEdits) state.columnMetadataEdits = {};
    const label = els.metadataEditLabel.value.trim() || col;
    const typeVal = els.metadataEditType.value;
    const note = els.metadataEditNote.value.trim();
    const catMode = els.metadataEditCatMode ? els.metadataEditCatMode.value : "auto";
    if (catMode === "custom") {
      const raw = els.metadataEditCatCustom ? els.metadataEditCatCustom.value.trim() : "";
      if (!raw) {
        toast("Custom category mode needs a JSON array of {value, proportion}, or switch strategy to Auto.");
        return;
      }
      if (!parseCustomCategoryProportionsJson(raw)) {
        toast("Fix custom proportions JSON (array of {value, proportion}).");
        return;
      }
    }
    const next = {
      displayLabel: label === col ? "" : label,
      treatAsType: typeVal === "auto" ? "" : typeVal,
      synthesisNote: note,
    };
    if (els.metadataEditDistribution) {
      const d = els.metadataEditDistribution.value;
      if (d && d !== "auto") next.synthDist = d;
    }
    if (els.metadataEditSynthMin && els.metadataEditSynthMin.value.trim()) next.synthMin = els.metadataEditSynthMin.value.trim();
    if (els.metadataEditSynthMax && els.metadataEditSynthMax.value.trim()) next.synthMax = els.metadataEditSynthMax.value.trim();
    if (els.metadataEditSynthMean && els.metadataEditSynthMean.value.trim()) next.synthMean = els.metadataEditSynthMean.value.trim();
    if (els.metadataEditSynthVariance && els.metadataEditSynthVariance.value.trim()) next.synthVariance = els.metadataEditSynthVariance.value.trim();
    if (els.metadataEditCatMode && catMode !== "auto") next.synthCatMode = catMode;
    if (els.metadataEditCatMergePct && catMode === "merge_rare" && els.metadataEditCatMergePct.value.trim()) {
      next.synthCatMergePct = els.metadataEditCatMergePct.value.trim();
    }
    if (els.metadataEditCatCustom && catMode === "custom" && els.metadataEditCatCustom.value.trim()) {
      next.synthCatCustom = els.metadataEditCatCustom.value.trim();
    }
    pruneEmptyColumnEdit(col, next);
    saveSession();
    closeMetadataEditModal();
    renderMetadata();
  }

  function closeCorrelationEditModal() {
    corrModalColA = null;
    corrModalColB = null;
    if (els.correlationEditModal) els.correlationEditModal.classList.add("hidden");
  }

  function openCorrelationEditModal(colA, colB) {
    if (!els.correlationEditModal || !els.correlationEditTarget) return;
    const colStats = inferColumnStats(state.headers, state.rows);
    const block = buildNumericCorrelationBlock(state.headers, state.rows, colStats);
    const cols = block.matrix_columns || [];
    const ia = cols.indexOf(colA);
    const ib = cols.indexOf(colB);
    if (ia < 0 || ib < 0) {
      toast("That pair is not in the current correlation matrix (column limit or not numeric).");
      return;
    }
    corrModalColA = colA;
    corrModalColB = colB;
    const obs = block.matrix_source[ia][ib];
    const key = pairCorrelationKey(colA, colB);
    const edVal = (state.correlationEdits || {})[key];
    const synth = block.matrix[ia][ib];
    const forInput = edVal != null && edVal !== "" ? Number(edVal) : synth;
    if (els.correlationEditPairLabel) {
      els.correlationEditPairLabel.textContent = `${colA} ↔ ${colB}`;
    }
    if (els.correlationEditObserved) {
      els.correlationEditObserved.textContent =
        obs == null || Number.isNaN(obs) ? "—" : Number(obs).toFixed(4);
    }
    els.correlationEditTarget.value = forInput != null && !Number.isNaN(forInput) ? String(forInput) : "";
    els.correlationEditModal.classList.remove("hidden");
    els.correlationEditModal.focus();
    els.correlationEditTarget.focus();
  }

  function saveCorrelationEditModal() {
    if (!corrModalColA || !corrModalColB || !els.correlationEditTarget) return;
    const key = pairCorrelationKey(corrModalColA, corrModalColB);
    const colStats = inferColumnStats(state.headers, state.rows);
    const block = buildNumericCorrelationBlock(state.headers, state.rows, colStats);
    const cols = block.matrix_columns || [];
    const ia = cols.indexOf(corrModalColA);
    const ib = cols.indexOf(corrModalColB);
    if (ia < 0 || ib < 0) {
      closeCorrelationEditModal();
      return;
    }
    const obs = block.matrix_source[ia][ib];
    const raw = els.correlationEditTarget.value.trim();
    const v = raw === "" ? NaN : Number(raw);
    if (!Number.isFinite(v) || v < -1 || v > 1) {
      toast("Enter a number between -1 and 1.");
      return;
    }
    const rounded = Number(Math.max(-1, Math.min(1, v)).toFixed(4));
    if (!state.correlationEdits) state.correlationEdits = {};
    if (obs != null && !Number.isNaN(obs) && Math.abs(rounded - Number(obs)) < 0.0005) {
      delete state.correlationEdits[key];
    } else {
      state.correlationEdits[key] = rounded;
    }
    saveSession();
    closeCorrelationEditModal();
    renderMetadata();
  }

  function clearCorrelationEditModal() {
    if (!corrModalColA || !corrModalColB) return;
    const key = pairCorrelationKey(corrModalColA, corrModalColB);
    if (state.correlationEdits) delete state.correlationEdits[key];
    saveSession();
    closeCorrelationEditModal();
    renderMetadata();
  }

  /** Max rows in category-mix UI (top values + optional Other); scales with cardinality, capped for performance. */
  function getCategoryMixMaxBuckets(colStat) {
    const HARD_MAX = 36;
    const SOFT_MIN = 7;
    const u = colStat && Number.isFinite(colStat.unique) ? colStat.unique : 0;
    if (u <= 0) return SOFT_MIN;
    return Math.min(HARD_MAX, Math.max(SOFT_MIN, u + 1));
  }

  function buildObservedCategoryDistribution(colName, maxBuckets) {
    const freq = new Map();
    let nonNull = 0;
    state.rows.forEach((r) => {
      const v = r[colName];
      if (v === "" || v == null) return;
      nonNull++;
      const k = String(v);
      freq.set(k, (freq.get(k) || 0) + 1);
    });
    if (!nonNull) return [];
    const top = [...freq.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, Math.max(2, maxBuckets - 1));
    const topTotal = top.reduce((a, x) => a + x[1], 0);
    const items = top.map(([value, count]) => ({ label: value, proportion: count / nonNull, original: count / nonNull }));
    const otherCount = Math.max(0, nonNull - topTotal);
    if (otherCount > 0) {
      items.push({ label: "Other", proportion: otherCount / nonNull, original: otherCount / nonNull });
    }
    const sum = items.reduce((a, x) => a + x.proportion, 0) || 1;
    items.forEach((x) => {
      x.proportion = x.proportion / sum;
      x.original = x.original / sum;
    });
    return items;
  }

  function rebalanceDistributionShares(current, changedIdx, target) {
    const n = current.length;
    const next = current.slice();
    if (n <= 1 || changedIdx < 0 || changedIdx >= n) return next;
    const clampedTarget = Math.max(0, Math.min(1, target));
    const old = next[changedIdx];
    let delta = clampedTarget - old;
    next[changedIdx] = clampedTarget;
    if (Math.abs(delta) < 1e-9) return next;
    const others = [];
    for (let i = 0; i < n; i++) {
      if (i !== changedIdx) others.push(i);
    }
    if (delta > 0) {
      // Raise one bucket by taking mass from the others proportionally.
      const pool = others.reduce((a, i) => a + next[i], 0);
      if (pool <= 0) {
        next[changedIdx] = old;
        return next;
      }
      const take = Math.min(delta, pool);
      others.forEach((i) => {
        const share = next[i] / pool;
        next[i] = Math.max(0, next[i] - take * share);
      });
      next[changedIdx] = old + take;
    } else {
      // Lower one bucket by redistributing to others proportionally.
      const give = -delta;
      const pool = others.reduce((a, i) => a + next[i], 0);
      if (pool > 0) {
        others.forEach((i) => {
          const share = next[i] / pool;
          next[i] = next[i] + give * share;
        });
      } else {
        const per = give / others.length;
        others.forEach((i) => {
          next[i] = per;
        });
      }
    }
    const s = next.reduce((a, x) => a + x, 0) || 1;
    return next.map((x) => Math.max(0, x / s));
  }

  function applyDistributionSliderChange(colName, idx, targetShare, maxBuckets) {
    const mb =
      maxBuckets != null && Number.isFinite(maxBuckets) ? Math.min(48, Math.max(7, Math.floor(maxBuckets))) : 7;
    const base = buildObservedCategoryDistribution(colName, mb);
    if (base.length < 2) return;
    const ed = getEditForCol(colName);
    const parsed = parseCustomCategoryProportionsJson(ed.synthCatCustom, { silent: true });
    const byLabel = new Map();
    (parsed || []).forEach((r) => {
      byLabel.set(String(r.value), Number(r.proportion));
    });
    const current = base.map((b) => {
      const p = byLabel.get(b.label);
      return Number.isFinite(p) && p >= 0 ? p : b.proportion;
    });
    const normalizedCurrent = (() => {
      const s = current.reduce((a, x) => a + x, 0) || 1;
      return current.map((x) => Math.max(0, x / s));
    })();
    const next = rebalanceDistributionShares(normalizedCurrent, idx, targetShare);
    const payload = base.map((b, i) => ({
      value: b.label,
      proportion: Number(next[i].toFixed(6)),
    }));
    const sumP = payload.reduce((a, x) => a + x.proportion, 0) || 1;
    payload.forEach((x) => {
      x.proportion = Number((x.proportion / sumP).toFixed(6));
    });
    if (!state.columnMetadataEdits) state.columnMetadataEdits = {};
    const nextEd = { ...getEditForCol(colName), synthCatMode: "custom", synthCatCustom: JSON.stringify(payload) };
    pruneEmptyColumnEdit(colName, nextEd);
    saveSession();
    renderMetadata();
  }

  function applyDistributionSectionCatStrategy(col, mode, mergePctOptional) {
    if (!col || !state.headers.includes(col)) return;
    const m = mode || "auto";
    const prev = getEditForCol(col);
    const nextEd = { ...prev };
    if (m === "auto") {
      delete nextEd.synthCatMode;
      delete nextEd.synthCatCustom;
      delete nextEd.synthCatMergePct;
    } else if (m === "uniform_balance") {
      nextEd.synthCatMode = "uniform_balance";
      delete nextEd.synthCatCustom;
      delete nextEd.synthCatMergePct;
    } else if (m === "merge_rare") {
      nextEd.synthCatMode = "merge_rare";
      delete nextEd.synthCatCustom;
      const pct =
        mergePctOptional != null && String(mergePctOptional).trim()
          ? String(mergePctOptional).trim()
          : String(prev.synthCatMergePct || "0.02").trim() || "0.02";
      nextEd.synthCatMergePct = pct;
    } else if (m === "custom") {
      nextEd.synthCatMode = "custom";
      const colStats = inferColumnStats(state.headers, state.rows);
      const st = colStats.find((x) => x.name === col);
      const base = buildObservedCategoryDistribution(col, getCategoryMixMaxBuckets(st || { unique: 0 }));
      const existing = parseCustomCategoryProportionsJson(nextEd.synthCatCustom, { silent: true });
      if (!existing || !existing.length) {
        const sumP = base.reduce((a, b) => a + b.proportion, 0) || 1;
        const payload = base.map((b) => ({
          value: b.label,
          proportion: Number((b.proportion / sumP).toFixed(6)),
        }));
        nextEd.synthCatCustom = JSON.stringify(payload);
      }
    }
    if (!state.columnMetadataEdits) state.columnMetadataEdits = {};
    pruneEmptyColumnEdit(col, nextEd);
    saveSession();
    renderMetadata();
  }

  function buildNumericDistShapeSelectHtml(colName, currentVal) {
    const cur = currentVal && currentVal !== "auto" ? currentVal : "auto";
    const opts = [
      ["auto", "Match observed (auto)"],
      ["normal", "Normal-like"],
      ["skew_right", "Right-skewed"],
      ["skew_left", "Left-skewed"],
      ["uniform", "Roughly uniform"],
      ["multimodal", "Multimodal / mixture"],
    ]
      .map(
        ([v, lab]) =>
          `<option value="${escapeAttr(v)}"${v === cur ? " selected" : ""}>${escapeHtml(lab)}</option>`
      )
      .join("");
    return `<div class="dist-num-shape-row">
      <label class="dist-num-shape-label">Distribution shape
        <select class="modal-input dist-num-shape-select" data-num-shape-col="${escapeAttr(colName)}">${opts}</select>
      </label>
    </div>`;
  }

  function buildNumericHistogramInteractiveHtml(colName, colStat) {
    const base = buildObservedNumericHistogramBins(colStat);
    if (!base || base.length < 2) return "";
    const ed = getEditForCol(colName);
    const parsed = parseNumericHistogramProportionsJson(ed.synthNumHistCustom, { silent: true });
    const byLabel = new Map();
    (parsed || []).forEach((r) => {
      byLabel.set(String(r.label), Number(r.proportion));
    });
    const current = base.map((b) => {
      const p = byLabel.get(b.label);
      return Number.isFinite(p) && p >= 0 ? p : b.proportion;
    });
    const sumC = current.reduce((a, x) => a + x, 0) || 1;
    const norm = current.map((x) => Math.max(0, x / sumC));
    const maxScale = Math.max(...base.map((b) => b.original), ...norm, 1e-9) * 1.06;

    const W = 400;
    const H = 220;
    const padL = 44;
    const padR = 12;
    const padTop = 12;
    const padBot = 40;
    const plotW = W - padL - padR;
    const plotH = H - padTop - padBot;
    const bottom = H - padBot;
    const n = base.length;
    const slotW = plotW / n;

    const parts = [];
    parts.push(
      `<svg class="dist-num-svg" viewBox="0 0 ${W} ${H}" width="100%" height="220" preserveAspectRatio="xMidYMid meet" role="img" aria-label="Histogram for ${escapeAttr(
        colName
      )}">`
    );
    parts.push(
      `<line class="dist-num-axis" x1="${padL}" y1="${bottom}" x2="${W - padR}" y2="${bottom}" stroke="currentColor" stroke-opacity="0.25" stroke-width="1" />`
    );

    for (let i = 0; i < n; i++) {
      const b = base[i];
      const x = padL + i * slotW;
      const bw = slotW * 0.82;
      const bx = x + (slotW - bw) / 2;
      const origH = (b.original / maxScale) * plotH;
      const curH = (norm[i] / maxScale) * plotH;
      const barH = Math.max(curH, 3);
      const origTop = bottom - origH;
      const curTop = bottom - barH;
      const lab = formatDistNumBinAxisDisplay(b.label);
      parts.push(`<rect class="dist-num-hist-baseline" x="${bx.toFixed(2)}" y="${origTop.toFixed(2)}" width="${bw.toFixed(2)}" height="${origH.toFixed(2)}" rx="3" />`);
      parts.push(
        `<rect class="dist-num-hist-bar" x="${bx.toFixed(2)}" y="${curTop.toFixed(2)}" width="${bw.toFixed(2)}" height="${barH.toFixed(2)}" rx="3" pointer-events="none" aria-hidden="true" />`
      );
      parts.push(
        `<rect class="dist-num-hist-hit" x="${x.toFixed(2)}" y="${padTop}" width="${slotW.toFixed(2)}" height="${plotH.toFixed(2)}" fill="transparent" data-num-hist-col="${escapeAttr(
          colName
        )}" data-num-hist-idx="${i}" data-num-hist-p="${norm[i]}" />`
      );
      parts.push(
        `<text class="dist-num-bin-label" x="${(x + slotW / 2).toFixed(2)}" y="${H - 10}" text-anchor="middle" font-size="9">${escapeHtml(lab)}</text>`
      );
    }
    parts.push(`</svg>`);

    const curDist = ed.synthDist && ed.synthDist !== "auto" ? ed.synthDist : "auto";
    const shapeRow = buildNumericDistShapeSelectHtml(colName, curDist);

    return `<article class="chart-card dist-edit-card dist-num-card">
      <h4>Numeric histogram — ${escapeHtml(colName)}</h4>
      <p class="dist-edit-help"><strong>Drag</strong> a bar up or down (grab anywhere in its column). Amber bars show the <strong>observed baseline</strong>; blue shows your <strong>synthetic</strong> target. Other bins rebalance so the total stays 100%.</p>
      ${shapeRow}
      <div class="dist-num-interactive-chart" data-plot-h="${plotH}" style="touch-action:none">${parts.join("")}</div>
    </article>`;
  }

  function applyNumericHistogramSliderChange(colName, idx, targetShare) {
    const colStats = inferColumnStats(state.headers, state.rows);
    const colStat = colStats.find((x) => x.name === colName);
    if (!colStat) return;
    const base = buildObservedNumericHistogramBins(colStat);
    if (!base || base.length < 2) return;
    const ed = getEditForCol(colName);
    const parsed = parseNumericHistogramProportionsJson(ed.synthNumHistCustom, { silent: true });
    const byLabel = new Map();
    (parsed || []).forEach((r) => {
      byLabel.set(String(r.label), Number(r.proportion));
    });
    const current = base.map((b) => {
      const p = byLabel.get(b.label);
      return Number.isFinite(p) && p >= 0 ? p : b.proportion;
    });
    const normalizedCurrent = (() => {
      const s = current.reduce((a, x) => a + x, 0) || 1;
      return current.map((x) => Math.max(0, x / s));
    })();
    const next = rebalanceDistributionShares(normalizedCurrent, idx, targetShare);
    const payload = base.map((b, i) => ({
      label: b.label,
      proportion: Number(next[i].toFixed(6)),
    }));
    const sumP = payload.reduce((a, x) => a + x.proportion, 0) || 1;
    payload.forEach((x) => {
      x.proportion = Number((x.proportion / sumP).toFixed(6));
    });
    const props = payload.map((x) => x.proportion);
    if (!state.columnMetadataEdits) state.columnMetadataEdits = {};
    const nextEd = { ...getEditForCol(colName) };
    if (numericHistogramMatchesBaseline(base, props)) {
      delete nextEd.synthNumHistCustom;
    } else {
      nextEd.synthNumHistCustom = JSON.stringify(payload);
    }
    pruneEmptyColumnEdit(colName, nextEd);
    saveSession();
    renderMetadata();
  }

  function distEditControlRowHtml(colName, idx, fullLabel, shortLabel, curPct, origPct, kind, opts) {
    const disabled = opts && opts.disabled;
    const disAttr = disabled ? " disabled" : "";
    return `<div class="dist-edit-row${disabled ? " dist-edit-row--disabled" : ""}">
      <div class="dist-edit-label-block">
        <span class="dist-edit-label" title="${escapeAttr(fullLabel)}">${escapeHtml(shortLabel)}</span>
        <span class="dist-edit-values"><span class="dist-edit-current">${curPct.toFixed(1)}%</span> <span class="dist-edit-original">baseline ${origPct.toFixed(1)}%</span></span>
      </div>
      <div class="dist-edit-viz" title="Blue bar = current synthetic share; amber to dashed line = observed baseline.">
        <div class="dist-edit-viz-track" aria-hidden="true">
          <div class="dist-edit-viz-baseline" style="width:${origPct.toFixed(2)}%"></div>
          <div class="dist-edit-viz-fill" style="width:${curPct.toFixed(2)}%"></div>
        </div>
      </div>
      <div class="dist-edit-slider-cell">
        <input class="dist-edit-slider" type="range" min="0" max="100" step="0.1" value="${curPct.toFixed(1)}" data-hist-kind="${escapeAttr(
      kind
    )}" data-hist-slider-col="${escapeAttr(colName)}" data-hist-slider-idx="${idx}" aria-label="Adjust synthetic share for ${escapeAttr(
      fullLabel
    )} in ${escapeAttr(colName)}"${disAttr} />
      </div>
    </div>`;
  }

  /** Prefer domain columns like cost/price so they stay in the Distributions UI when many numerics exist. */
  function orderColStatsForDistributions(arr) {
    const headerIndex = (name) => {
      const i = state.headers.indexOf(name);
      return i < 0 ? 9999 : i;
    };
    const pri = (name) => {
      const n = String(name).toLowerCase().replace(/\s+/g, "_");
      if (n === "cost" || n.endsWith("_cost") || n.startsWith("cost_")) return 0;
      if (n.includes("cost")) return 1;
      if (n.includes("price") || n.includes("pricing")) return 2;
      if (n.includes("revenue") || n.includes("sales")) return 3;
      if (n.includes("amount") || n.includes("total") || n.includes("fee")) return 4;
      if (n.includes("qty") || n.includes("quantity")) return 5;
      return 50;
    };
    return [...arr].sort((a, b) => {
      const d = pri(a.name) - pri(b.name);
      if (d !== 0) return d;
      return headerIndex(a.name) - headerIndex(b.name);
    });
  }

  function getMetadataDistributionNumericColumns(colStats, limit) {
    const filtered = colStats.filter(
      (c) => effectiveColumnKind(c, getEditForCol(c.name)) === "numeric" && c.numericSample && c.numericSample.length > 2
    );
    return orderColStatsForDistributions(filtered).slice(0, limit);
  }

  function getMetadataDistributionTextColumns(colStats, limit) {
    return colStats
      .filter((c) => effectiveColumnKind(c, getEditForCol(c.name)) === "text" && c.nonNull > 0)
      .slice(0, limit);
  }

  function hasDistributionSectionOverrides() {
    const m = state.columnMetadataEdits;
    if (!m || typeof m !== "object") return false;
    return Object.keys(m).some((col) => {
      const ed = m[col];
      if (!ed || typeof ed !== "object") return false;
      if (ed.synthDist && ed.synthDist !== "auto") return true;
      if (String(ed.synthNumHistCustom || "").trim()) return true;
      if (ed.synthCatMode && ed.synthCatMode !== "auto") return true;
      return false;
    });
  }

  function revertDistributionOverrides() {
    if (!hasDistributionSectionOverrides()) {
      toast("No distribution edits to revert.");
      return;
    }
    const m = state.columnMetadataEdits;
    if (!m || typeof m !== "object") return;
    Object.keys(m).forEach((col) => {
      if (!m[col]) return;
      const ed = { ...m[col] };
      let touched = false;
      if (ed.synthDist && ed.synthDist !== "auto") {
        delete ed.synthDist;
        touched = true;
      }
      if (String(ed.synthNumHistCustom || "").trim()) {
        delete ed.synthNumHistCustom;
        touched = true;
      }
      if (ed.synthCatMode && ed.synthCatMode !== "auto") {
        delete ed.synthCatMode;
        delete ed.synthCatCustom;
        delete ed.synthCatMergePct;
        touched = true;
      }
      if (touched) pruneEmptyColumnEdit(col, ed);
    });
    numHistDrag = null;
    saveSession();
    renderMetadata();
    if (state.step === 2 && state.metadataPane === "changesReview") renderMetadataChangesReview();
    toast("Distributions reset to observed profiling defaults.");
  }

  function updateDistributionsRevertButtonVisibility() {
    if (!els.btnRevertDistributions) return;
    const show = hasDistributionSectionOverrides();
    els.btnRevertDistributions.classList.toggle("hidden", !show);
    const distDet = $("meta-section-distributions");
    if (distDet) distDet.classList.toggle("meta-section-panel--dist-edited", show);
  }

  let distributionsRevertBound = false;
  function bindRevertDistributionsOnce() {
    if (distributionsRevertBound || !els.btnRevertDistributions) return;
    distributionsRevertBound = true;
    els.btnRevertDistributions.addEventListener("click", () => revertDistributionOverrides());
  }

  function renderMetadataDistributionEditor(colStats, precomputedNumericCols) {
    if (!els.metadataDistributionEditor) return;
    const blocks = [];
    const numCols = precomputedNumericCols || getMetadataDistributionNumericColumns(colStats, 3);
    numCols.forEach((c) => {
      const html = buildNumericHistogramInteractiveHtml(c.name, c);
      if (html) blocks.push(html);
    });

    const catCols = getMetadataDistributionTextColumns(colStats, 2);
    catCols.forEach((c) => {
      const catBuckets = getCategoryMixMaxBuckets(c);
      const base = buildObservedCategoryDistribution(c.name, catBuckets);
      if (base.length < 2) return;
      const ed = getEditForCol(c.name);
      const catModeRaw = ed.synthCatMode && ed.synthCatMode !== "auto" ? ed.synthCatMode : "auto";
      const slidersDisabled = catModeRaw === "merge_rare" || catModeRaw === "uniform_balance";
      const stratOpts = [
        ["auto", "Match observed proportions"],
        ["uniform_balance", "Roughly balance categories"],
        ["merge_rare", "Merge rare below a threshold"],
        ["custom", "Custom proportions (sliders)"],
      ]
        .map(
          ([v, lab]) =>
            `<option value="${escapeAttr(v)}"${v === catModeRaw ? " selected" : ""}>${escapeHtml(lab)}</option>`
        )
        .join("");
      const mergeHidden = catModeRaw !== "merge_rare" ? " hidden" : "";
      const mergeValRaw =
        ed.synthCatMergePct != null && String(ed.synthCatMergePct).trim()
          ? String(ed.synthCatMergePct).trim()
          : "0.02";
      const strategyBlock = `<div class="dist-cat-strategy-row">
        <label class="dist-cat-strategy-label">Category strategy
          <select class="modal-input dist-cat-strategy-select" data-cat-strat-col="${escapeAttr(c.name)}">${stratOpts}</select>
        </label>
        <div class="dist-cat-merge-row${mergeHidden}">
          <label class="dist-cat-merge-label">Merge rare below (proportion 0–1)
            <input type="text" class="modal-input dist-cat-merge-input" inputmode="decimal" autocomplete="off" data-cat-merge-col="${escapeAttr(
              c.name
            )}" value="${escapeAttr(mergeValRaw)}" placeholder="0.02" />
          </label>
        </div>
      </div>`;
      const parsed = parseCustomCategoryProportionsJson(ed.synthCatCustom, { silent: true });
      const byLabel = new Map();
      (parsed || []).forEach((r) => byLabel.set(String(r.value), Number(r.proportion)));
      const current = base.map((b) => {
        const p = byLabel.get(b.label);
        return Number.isFinite(p) && p >= 0 ? p : b.proportion;
      });
      const sumCurrent = current.reduce((a, x) => a + x, 0) || 1;
      const normalizedCurrent = current.map((x) => Math.max(0, x / sumCurrent));
      const sliderRows = base
        .map((b, i) => {
          const cur = normalizedCurrent[i];
          const curPct = Math.max(0, Math.min(100, cur * 100));
          const origPct = Math.max(0, Math.min(100, b.original * 100));
          const short = b.label.length > 32 ? `${b.label.slice(0, 30)}…` : b.label;
          return distEditControlRowHtml(c.name, i, b.label, short, curPct, origPct, "cat", { disabled: slidersDisabled });
        })
        .join("");
      const hasOtherBucket = base.length > 0 && base[base.length - 1].label === "Other";
      const namedSlots = hasOtherBucket ? base.length - 1 : base.length;
      const foldNote =
        !slidersDisabled && c.unique > namedSlots
          ? ` Less frequent levels roll into <strong>Other</strong> (file has ${c.unique.toLocaleString()} distinct values).`
          : "";
      const scrollNote = base.length > 10 ? " Long lists scroll inside the shaded area." : "";
      const helpText = slidersDisabled
        ? `<p class="dist-edit-help">Strategy is <strong>${
            catModeRaw === "merge_rare" ? "merge rare" : "balanced mix"
          }</strong> (not edited with sliders). Choose <strong>Custom proportions (sliders)</strong> to drag category shares. Rows below still show observed vs. current stored targets when applicable.${scrollNote}</p>`
        : `<p class="dist-edit-help">Each row shows the <strong>observed baseline</strong> (amber band to the dashed line) and your <strong>current synthetic target</strong> (blue). Drag the slider beside it; other categories rebalance so the total stays 100%.${foldNote}${scrollNote}</p>`;
      const manyClass = base.length > 10 ? " dist-edit-list--many" : "";
      blocks.push(`<article class="chart-card dist-edit-card" data-dist-cat-buckets="${catBuckets}">
        <h4>Category mix — ${escapeHtml(c.name)}</h4>
        <p class="dist-edit-meta">${c.unique.toLocaleString()} distinct · ${base.length} row${
        base.length === 1 ? "" : "s"
      } in editor</p>
        ${strategyBlock}
        ${helpText}
        <div class="dist-edit-list${manyClass}">${sliderRows}</div>
      </article>`);
    });

    const legendInner = `<span class="dist-legend-item"><span class="dist-legend-swatch dist-legend-swatch--baseline" aria-hidden="true"></span> Observed baseline</span>
      <span class="dist-legend-item"><span class="dist-legend-swatch dist-legend-swatch--synth" aria-hidden="true"></span> Synthetic target</span>`;
    if (els.metadataDistLegend) {
      if (blocks.length) {
        els.metadataDistLegend.innerHTML = legendInner;
        els.metadataDistLegend.classList.remove("hidden");
        els.metadataDistLegend.setAttribute("role", "group");
        els.metadataDistLegend.setAttribute("aria-label", "Distribution legend");
      } else {
        els.metadataDistLegend.innerHTML = "";
        els.metadataDistLegend.classList.add("hidden");
        els.metadataDistLegend.removeAttribute("role");
        els.metadataDistLegend.removeAttribute("aria-label");
      }
    }
    els.metadataDistributionEditor.innerHTML = blocks.join("");
  }

  function bindMetadataModalOnce() {
    if (!els.metadataEditSave || els.metadataEditSave.dataset.bound) return;
    if (!els.metadataEditCancel || !els.metadataEditBackdrop || !els.metadataEditModal) return;
    els.metadataEditSave.dataset.bound = "1";
    els.metadataEditSave.addEventListener("click", saveMetadataEditModal);
    els.metadataEditCancel.addEventListener("click", closeMetadataEditModal);
    els.metadataEditBackdrop.addEventListener("click", closeMetadataEditModal);
    els.metadataEditModal.addEventListener("keydown", (e) => {
      if (e.key === "Escape") closeMetadataEditModal();
    });
    if (els.metadataEditType && !els.metadataEditType.dataset.synthBound) {
      els.metadataEditType.dataset.synthBound = "1";
      els.metadataEditType.addEventListener("change", syncMetadataSynthFieldsets);
    }
    if (els.metadataEditCatMode && !els.metadataEditCatMode.dataset.synthBound) {
      els.metadataEditCatMode.dataset.synthBound = "1";
      els.metadataEditCatMode.addEventListener("change", syncMetadataSynthFieldsets);
    }
  }

  function bindCorrelationEditModalOnce() {
    if (!els.correlationEditSave || els.correlationEditSave.dataset.bound) return;
    if (!els.correlationEditCancel || !els.correlationEditClear || !els.correlationEditBackdrop || !els.correlationEditModal) return;
    els.correlationEditSave.dataset.bound = "1";
    els.correlationEditSave.addEventListener("click", saveCorrelationEditModal);
    els.correlationEditCancel.addEventListener("click", closeCorrelationEditModal);
    els.correlationEditClear.addEventListener("click", clearCorrelationEditModal);
    els.correlationEditBackdrop.addEventListener("click", closeCorrelationEditModal);
    els.correlationEditModal.addEventListener("keydown", (e) => {
      if (e.key === "Escape") closeCorrelationEditModal();
    });
  }

  function renderMetadata() {
    bindMetadataViewEvents();
    bindMetadataModalOnce();
    bindCorrelationEditModalOnce();
    bindSyntheticResetButtonsOnce();
    bindMetadataSectionDetailsToggleOnce();
    if (!state.rows.length || !els.metadataHygieneList || !els.metadataDashboardStats || !els.metadataDashboardColumns || !els.metadataJson) {
      repositionSyntheticResetButtons();
      if (els.btnRevertDistributions) els.btnRevertDistributions.classList.add("hidden");
      const distDet = $("meta-section-distributions");
      if (distDet) distDet.classList.remove("meta-section-panel--dist-edited");
      if (els.metadataDistLegend) {
        els.metadataDistLegend.innerHTML = "";
        els.metadataDistLegend.classList.add("hidden");
      }
      return;
    }

    const colStats = inferColumnStats(state.headers, state.rows);
    const enriched = enrichColumnsForMetadata(state.headers, state.rows, colStats);
    const pkg = buildSyntheticMetadataPayload(state.headers, state.rows, colStats);
    const inc = getColumnIncludeMap();
    const nulls = colStats.reduce((a, c) => a + c.missing, 0);
    const highSev = (state.issues || []).filter((i) => i.sev === "high").length;
    const includedN = state.headers.filter((h) => inc[h] !== false).length;

    els.metadataDashboardStats.innerHTML = `
      <div class="stat-card"><div class="stat-value">${state.rows.length.toLocaleString()}</div><div class="stat-label">Rows in file</div></div>
      <div class="stat-card"><div class="stat-value">${state.headers.length}</div><div class="stat-label">Columns detected</div></div>
      <div class="stat-card"><div class="stat-value">${includedN}</div><div class="stat-label">Included in synthetic schema</div></div>
      <div class="stat-card"><div class="stat-value">${(state.headers.length - includedN).toLocaleString()}</div><div class="stat-label">Excluded from schema</div></div>
      <div class="stat-card"><div class="stat-value">${nulls.toLocaleString()}</div><div class="stat-label">Empty cells</div></div>
      <div class="stat-card"><div class="stat-value">${highSev}</div><div class="stat-label">High-severity hygiene flags</div></div>
    `;

    els.metadataDashboardColumns.innerHTML = enriched
      .map((c) => {
        const summary =
          c.numeric_summary != null
            ? `Range ${c.numeric_summary.min} → ${c.numeric_summary.max} · average ${c.numeric_summary.mean}`
            : (c.categorical_summary?.top_categories || [])
                .slice(0, 2)
                .map((t) => `${String(t.value).slice(0, 20)} (${t.count})`)
                .join(" · ") || "—";
        const synthLine =
          c.synthetic_numeric_targets || c.synthetic_categorical_targets
            ? `<p class="meta-dash-synth-hint">Synthetic targets: use <strong>Edit metadata</strong> to adjust (exported in JSON).</p>`
            : "";
        const checked = inc[c.name] ? "checked" : "";
        const dtypeLine = c.user_schema_dtype_override
          ? `Computed <strong>${escapeHtml(c.computed_profile_dtype)}</strong> · you set schema as <strong>${escapeHtml(c.user_schema_dtype_override)}</strong>`
          : `Type profile: <strong>${escapeHtml(c.computed_profile_dtype)}</strong>`;
        const st = getColStatOrFallback(c.name, colStats);
        const ed = getEditForCol(c.name);
        const changeHint = briefColumnChangeSummary(c.name, st, ed, inc[c.name] !== false);
        const modClass = changeHint ? " meta-dash-card--modified" : "";
        const hintBlock = changeHint
          ? `<p class="meta-dash-change-hint" role="status"><span class="meta-dash-change-indicator" aria-hidden="true"></span><span class="meta-dash-change-hint-text">${escapeHtml(changeHint)}</span></p>`
          : "";
        return `<article class="meta-dash-card${modClass}">
          <div class="meta-dash-card-head">
            <div>
              <h4 class="meta-dash-card-title">${escapeHtml(c.label_for_synthesis)}</h4>
              <p class="meta-dash-card-sub">${escapeHtml(c.name)}</p>
            </div>
            <label class="meta-dash-include"><input type="checkbox" class="meta-include-cb" data-col="${escapeAttr(c.name)}" ${checked} /> Include</label>
          </div>
          <p class="meta-dash-dtype">${dtypeLine}</p>
          <dl class="meta-dash-dl">
            <div><dt>Non-null</dt><dd>${c.non_null_count.toLocaleString()}</dd></div>
            <div><dt>Missing</dt><dd>${c.missing_count.toLocaleString()}</dd></div>
            <div><dt>Distinct values</dt><dd>${c.distinct_count.toLocaleString()}</dd></div>
          </dl>
          <p class="meta-dash-summary">${escapeHtml(summary)}</p>
          ${synthLine}
          ${hintBlock}
          <p class="meta-dash-role">${escapeHtml(String(c.how_used_for_synthesis || "").slice(0, 220))}${(c.how_used_for_synthesis || "").length > 220 ? "…" : ""}</p>
          <button type="button" class="btn btn-secondary meta-dash-edit-btn" data-col="${escapeAttr(c.name)}">Edit metadata</button>
        </article>`;
      })
      .join("");

    const nameToEnriched = new Map(enriched.map((c) => [c.name, c]));
    renderMetadataCorrelation(els.metadataCorrelation, pkg.numeric_correlation_pearson, nameToEnriched);

    els.metadataHygieneList.innerHTML = (state.issues && state.issues.length
      ? state.issues
      : [{ sev: "low", title: "No issues cached", detail: "Return to inspection to refresh the hygiene pass." }]
    )
      .slice(0, 12)
      .map(
        (i) => `<li class="issue-item">
        <span class="issue-severity ${sevClass(i.sev)}">${escapeHtml(i.sev)}</span>
        <div class="issue-body"><strong>${escapeHtml(i.title)}</strong><span>${escapeHtml(i.detail)}</span></div>
      </li>`
      )
      .join("");

    els.metadataJson.textContent = JSON.stringify(pkg, null, 2);
    const distNumericInteractive = getMetadataDistributionNumericColumns(colStats, 3);
    renderChartsInto(colStats, els.metadataChartsRow, state.metaCharts, "mchart", {
      skipCategorical: true,
      skipNumeric: false,
      metadataNumericOverflowCharts: true,
      metadataInteractiveNumericCols: distNumericInteractive,
    });
    renderMetadataDistributionEditor(colStats, distNumericInteractive);
    repositionSyntheticResetButtons();
    bindMetadataSectionNotesOnce();
    bindMetadataAiSuggestOnce();
    bindMetadataAiAcceptOnce();
    bindRevertDistributionsOnce();
    updateDistributionsRevertButtonVisibility();
    syncMetadataSectionNotesInputs();
    updateMetadataSectionNotesVisibility();
    refreshMetadataAiSuggestionsFromState();
  }

  function renderMetadataChangesReview() {
    if (!els.metadataChangesRoot || !state.rows.length) return;
    const colStats = inferColumnStats(state.headers, state.rows);
    const pkg = buildSyntheticMetadataPayload(state.headers, state.rows, colStats);
    els.metadataChangesRoot.innerHTML = buildMetadataChangesReviewHtml(pkg, colStats);
    const jp = $("metadata-changes-json-pre");
    if (jp) jp.textContent = JSON.stringify(pkg, null, 2);
    syncChangeReviewNoteTextareas();
  }

  function buildAiCoachChangeReviewBody() {
    const acc = state.metadataAiAccepted || [];
    if (!acc.length) {
      return `<p class="panel-lead meta-changes-lead-tight">No accepted coach recommendations.</p>`;
    }
    const blocks = acc
      .map((rec) => {
        const imp = rec.importance === "high" || rec.importance === "low" ? rec.importance : "medium";
        const cols = (rec.related_columns || []).filter((c) => state.headers.includes(c));
        const colsHtml = cols.length
          ? `<p class="meta-changes-ai-cols"><strong>Related columns:</strong> ${cols.map((c) => `<code>${escapeHtml(c)}</code>`).join(", ")}</p>`
          : `<p class="meta-changes-ai-cols"><em>General schema / multi-column guidance</em></p>`;
        const act =
          rec.suggested_action && String(rec.suggested_action).trim()
            ? `<p class="meta-changes-ai-action"><strong>Suggested next step:</strong> ${escapeHtml(String(rec.suggested_action).trim())}</p>`
            : "";
        const when = rec.accepted_at_utc
          ? escapeHtml(new Date(rec.accepted_at_utc).toLocaleString())
          : "\u2014";
        return `<div class="meta-changes-col-block meta-changes-ai-rec">
          <h4 class="meta-changes-col-title"><span class="metadata-ai-importance metadata-ai-importance--${imp}">${escapeHtml(imp)}</span> ${escapeHtml(String(rec.title || "Recommendation"))}</h4>
          ${colsHtml}
          <p class="panel-lead meta-changes-lead-tight">${escapeHtml(String(rec.detail || ""))}</p>
          ${act}
          <p class="meta-changes-ai-meta">Accepted ${when}</p>
          <div class="meta-changes-revert-row no-print"><button type="button" class="btn btn-ghost btn-sm" data-revert-ai-id="${escapeAttr(rec.id)}">Remove acceptance</button></div>
        </div>`;
      })
      .join("");
    return `<p class="panel-lead">These coach tips are included in your synthesis metadata until you remove them. Column-scoped items also appear under <strong>Columns</strong>.</p>${blocks}`;
  }

  function buildMetadataChangesReviewHtml(pkg, colStats) {
    const inner = [];
    let anySection = false;

    for (const sid of METADATA_NOTE_SECTIONS) {
      if (!changeReviewSectionHasChanges(sid)) continue;
      anySection = true;
      const title = METADATA_SECTION_NOTE_LABELS[sid] || sid;
      let body = "";
      if (sid === "columns") {
        body = buildColumnsChangeReviewBody(colStats);
      } else if (sid === "correlations") {
        body = buildCorrelationsChangeReviewBody(colStats);
      } else if (sid === "dashboard") {
        body = `<p class="panel-lead meta-changes-lead-tight">Synthetic numeric, categorical, or correlation targets were customized from profiling defaults. Column- and pair-level detail appears under <strong>Columns</strong> and <strong>Correlations</strong>.</p>`;
      } else if (sid === "ai") {
        body = buildAiCoachChangeReviewBody();
      } else if (sid === "summary") {
        body = `<p class="panel-lead meta-changes-lead-tight">Summary figures on the metadata screen reflect your current include/exclude choices and column metadata edits.</p>`;
      } else if (sid === "distributions") {
        body = `<p class="panel-lead meta-changes-lead-tight">You changed <strong>histogram bins</strong>, <strong>numeric distribution shape</strong>, and/or <strong>category strategy or mix</strong> in the Distributions accordion. Use <strong>Revert changes</strong> (top right of that section) to clear those targets back to profiling defaults. Per-column detail also appears under <strong>Columns</strong>.</p>`;
      } else if (sid === "json") {
        body = `<p class="panel-lead meta-changes-lead-tight">The live JSON export on the metadata screen reflects all overrides listed in the sections above.</p>`;
      }
      inner.push(changeReviewSectionHtml(sid, title, body));
    }

    if (!anySection) {
      inner.push(
        `<div class="panel meta-changes-block"><p class="panel-lead meta-changes-lead-tight">No manual overrides are recorded yet. When you exclude columns, edit field metadata, adjust synthetic targets, change correlation targets, or accept AI Metadata Agent recommendations, only the relevant sections will appear here.</p></div>`
      );
    } else {
      inner.push(
        `<details class="meta-section-panel meta-changes-json-details no-print"><summary class="meta-section-summary">Full synthesis metadata JSON (after your changes)</summary><div class="meta-section-body"><pre class="metadata-json meta-changes-json-pre" id="metadata-changes-json-pre" aria-label="Metadata JSON after edits"></pre></div></details>`
      );
    }

    const printBanner = `<div class="meta-changes-print-banner"><h1 class="meta-changes-print-title">Metadata change report</h1><p class="meta-changes-print-meta">${escapeHtml(state.fileName || "dataset.csv")} · ${escapeHtml(new Date().toLocaleString())}</p></div>`;
    return `${printBanner}${inner.join("")}`;
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

  function buildMetadataSuggestPayload() {
    const colStats = inferColumnStats(state.headers, state.rows);
    const inc = getColumnIncludeMap();
    const edits = state.columnMetadataEdits || {};
    const editSummaries = {};
    Object.keys(edits)
      .sort()
      .forEach((col) => {
        const e = edits[col];
        if (!e || typeof e !== "object") return;
        const sn = e.synthesisNote && String(e.synthesisNote).trim();
        editSummaries[col] = {
          display_label: e.displayLabel && String(e.displayLabel).trim() ? String(e.displayLabel).trim() : undefined,
          treat_as_type: e.treatAsType === "numeric" || e.treatAsType === "text" ? e.treatAsType : undefined,
          has_synthetic_targets: columnEditHasSynthOverrides(e),
          synthesis_note_snippet: sn ? sn.slice(0, 200) : undefined,
        };
      });
    const corr = state.correlationEdits || {};
    const corrList = [];
    Object.keys(corr).forEach((k) => {
      const parts = k.split("\x00");
      if (parts.length !== 2) return;
      corrList.push({
        column_a: parts[0],
        column_b: parts[1],
        synthetic_target_r: corr[k],
      });
    });
    return {
      file_name: state.fileName,
      row_count: state.rows.length,
      headers: state.headers.slice(0, 80),
      column_stats: buildColumnStatsPayload(colStats),
      sample_rows: buildSampleRowsForApi(state.rows, state.headers, 25),
      hygiene_issues: (state.issues || []).slice(0, 20).map((i) => ({
        sev: i.sev,
        title: i.title,
        detail: i.detail,
      })),
      schema_context: {
        columns_excluded_from_schema: state.headers.filter((h) => inc[h] === false),
        column_metadata_edits: editSummaries,
        correlation_targets: corrList.slice(0, 40),
        accepted_ai_coach_titles: (state.metadataAiAccepted || []).map((r) => String(r.title || "").slice(0, 120)).filter(Boolean),
      },
    };
  }

  function clearMetadataAiSuggestionsUi() {
    if (els.metadataAiSuggestStatus) els.metadataAiSuggestStatus.textContent = "";
    if (els.metadataAiSuggestBody) {
      els.metadataAiSuggestBody.classList.add("hidden");
      els.metadataAiSuggestBody.innerHTML = "";
    }
  }

  function resetMetadataAiCoachState() {
    state.metadataAiLastRun = null;
    state.metadataAiAccepted = [];
    clearMetadataAiSuggestionsUi();
  }

  function normalizeMetadataAiSuggestionItems(rawSuggestions, runId) {
    const list = Array.isArray(rawSuggestions) ? rawSuggestions.slice(0, 4) : [];
    return list.map((s, i) => {
      const impRaw = String(s.importance || "medium").toLowerCase();
      const imp = impRaw === "high" || impRaw === "low" ? impRaw : "medium";
      const related = Array.isArray(s.related_columns)
        ? s.related_columns
            .map((c) => String(c || "").trim())
            .filter((c) => c && state.headers.includes(c))
            .slice(0, 3)
        : [];
      return {
        id: `ai-${runId}-${i}`,
        title: String(s.title || "Recommendation").slice(0, 200),
        detail: String(s.detail || "").slice(0, 800),
        importance: imp,
        suggested_action: s.suggested_action ? String(s.suggested_action).slice(0, 400) : "",
        related_columns: related,
      };
    });
  }

  function isAiSuggestionIdAccepted(id) {
    return (state.metadataAiAccepted || []).some((x) => x.id === id);
  }

  function renderMetadataAiSuggestBody() {
    if (!els.metadataAiSuggestBody) return;
    const run = state.metadataAiLastRun;
    if (!run || !Array.isArray(run.items)) {
      if (hasAcceptedAiMetadataGuidance()) {
        const n = state.metadataAiAccepted.length;
        els.metadataAiSuggestBody.innerHTML = `<div class="metadata-ai-accepted-only-banner" role="status">
          <p class="metadata-ai-accepted-only-text"><strong>${n}</strong> coach recommendation(s) are saved for synthesis. Run <strong>Get recommendations</strong> for a fresh list, or open <strong>Review changes</strong> to manage them.</p>
        </div>`;
        els.metadataAiSuggestBody.classList.remove("hidden");
      } else {
        els.metadataAiSuggestBody.classList.add("hidden");
        els.metadataAiSuggestBody.innerHTML = "";
      }
      return;
    }

    const summary = String(run.summary || "").trim() || "Here is a quick read on your metadata.";
    const summaryBlock = `<div class="metadata-ai-summary-card"><p class="metadata-ai-summary-text">${escapeHtml(summary)}</p></div>`;
    const items = run.items;
    if (!items.length) {
      els.metadataAiSuggestBody.innerHTML = `${summaryBlock}<div class="metadata-ai-empty-state" role="status">
        <p class="metadata-ai-empty-title">No priority updates right now</p>
        <p class="metadata-ai-empty-detail">The agent did not flag must-do metadata changes. You can refine fields manually or ask again after you adjust your data or schema.</p>
      </div>`;
      els.metadataAiSuggestBody.classList.remove("hidden");
      return;
    }

    const cards = items
      .map((s, i) => {
        const n = i + 1;
        const title = escapeHtml(String(s.title || "Recommendation"));
        const detail = escapeHtml(String(s.detail || ""));
        const imp = s.importance === "high" || s.importance === "low" ? s.importance : "medium";
        const act = s.suggested_action ? escapeHtml(String(s.suggested_action)) : "";
        const cols = Array.isArray(s.related_columns)
          ? s.related_columns
              .filter(Boolean)
              .slice(0, 3)
              .map((c) => `<span class="metadata-ai-pill">${escapeHtml(String(c))}</span>`)
              .join("")
          : "";
        const colsRow = cols ? `<div class="metadata-ai-pills" aria-label="Related columns">${cols}</div>` : "";
        const actRow = act
          ? `<p class="metadata-ai-next"><span class="metadata-ai-next-label">Next step</span> ${act}</p>`
          : "";
        const accepted = isAiSuggestionIdAccepted(s.id);
        const cardExtra = accepted ? " metadata-ai-card--accepted" : "";
        const btnClass = accepted ? "btn btn-sm metadata-ai-accept-btn metadata-ai-accept-btn--accepted" : "btn btn-sm btn-secondary metadata-ai-accept-btn";
        const checkMark = accepted ? "\u2713 " : "";
        const acceptRow = `<div class="metadata-ai-card-actions">
          <button type="button" class="${btnClass}" data-ai-suggestion-accept="${escapeAttr(s.id)}" aria-pressed="${accepted ? "true" : "false"}">
            <span class="metadata-ai-accept-check" aria-hidden="true">${checkMark}</span>
            <span class="metadata-ai-accept-label">${accepted ? "Accepted" : "Accept"}</span>
          </button>
        </div>`;
        return `<article class="metadata-ai-card metadata-ai-card--${imp}${cardExtra}">
          <div class="metadata-ai-card-top">
            <span class="metadata-ai-card-index" aria-hidden="true">${n}</span>
            <div class="metadata-ai-card-title-wrap">
              <h4 class="metadata-ai-card-title">${title}</h4>
              <span class="metadata-ai-importance metadata-ai-importance--${imp}">${escapeHtml(imp)}</span>
            </div>
          </div>
          ${colsRow}
          <p class="metadata-ai-card-detail">${detail}</p>
          ${actRow}
          ${acceptRow}
        </article>`;
      })
      .join("");
    els.metadataAiSuggestBody.innerHTML = `${summaryBlock}<div class="metadata-ai-card-grid">${cards}</div>`;
    els.metadataAiSuggestBody.classList.remove("hidden");
  }

  function refreshMetadataAiSuggestionsFromState() {
    renderMetadataAiSuggestBody();
  }

  function toggleAiSuggestionAccept(id) {
    if (!state.metadataAiAccepted) state.metadataAiAccepted = [];
    const idx = state.metadataAiAccepted.findIndex((x) => x.id === id);
    if (idx >= 0) {
      state.metadataAiAccepted.splice(idx, 1);
      toast("Recommendation removed from synthesis metadata.");
    } else {
      const item =
        state.metadataAiLastRun && Array.isArray(state.metadataAiLastRun.items)
          ? state.metadataAiLastRun.items.find((x) => x.id === id)
          : null;
      if (!item) {
        toast("That recommendation is not on this list anymore. Remove it from Review changes if it is still saved.");
        return;
      }
      state.metadataAiAccepted.push({
        id: item.id,
        accepted_at_utc: new Date().toISOString(),
        title: item.title,
        detail: item.detail,
        importance: item.importance,
        suggested_action: item.suggested_action || "",
        related_columns: Array.isArray(item.related_columns) ? item.related_columns.slice() : [],
        agent_summary_snapshot: state.metadataAiLastRun.summary || "",
      });
      toast("Saved for synthesis and the change summary.");
    }
    saveSession();
    renderMetadataAiSuggestBody();
    updateMetadataSectionNotesVisibility();
    if (state.step === 2 && state.metadataPane === "editor") renderMetadata();
    else if (state.step === 2 && state.metadataPane === "changesReview") renderMetadataChangesReview();
  }

  let metadataAiSuggestBound = false;
  function bindMetadataAiSuggestOnce() {
    if (metadataAiSuggestBound || !els.metadataAiSuggestBtn) return;
    metadataAiSuggestBound = true;
    els.metadataAiSuggestBtn.addEventListener("click", () => void runMetadataAiSuggestions());
  }

  let metadataAiAcceptBound = false;
  function bindMetadataAiAcceptOnce() {
    if (metadataAiAcceptBound || !els.viewMetadata) return;
    metadataAiAcceptBound = true;
    els.viewMetadata.addEventListener("click", (e) => {
      const btn = e.target.closest("[data-ai-suggestion-accept]");
      if (!btn) return;
      e.preventDefault();
      const id = btn.getAttribute("data-ai-suggestion-accept");
      if (!id) return;
      toggleAiSuggestionAccept(id);
    });
  }

  async function runMetadataAiSuggestions() {
    if (!state.rows.length) return;
    if (!els.metadataAiSuggestBtn) return;
    els.metadataAiSuggestBtn.disabled = true;
    if (els.metadataAiSuggestStatus) els.metadataAiSuggestStatus.textContent = "Your agent is reviewing your dataset and metadata…";
    if (els.metadataAiSuggestBody) {
      els.metadataAiSuggestBody.classList.add("hidden");
      els.metadataAiSuggestBody.innerHTML = "";
    }
    try {
      const payload = buildMetadataSuggestPayload();
      const res = await fetch(`${apiBase()}/api/metadata-suggest`, {
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
      const runId = `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
      const summary = String(raw.summary || "").trim() || "Here is a quick read on your metadata.";
      const items = normalizeMetadataAiSuggestionItems(raw.suggestions, runId);
      state.metadataAiLastRun = { runId, summary, items };
      saveSession();
      renderMetadataAiSuggestBody();
      updateMetadataSectionNotesVisibility();
      if (els.metadataAiSuggestStatus) els.metadataAiSuggestStatus.textContent = "";
    } catch (err) {
      console.error(err);
      let reason = err && err.message ? err.message : String(err);
      if (/OPENAI_API_KEY|openai.*not set/i.test(reason)) {
        reason = "The AI Metadata Agent could not run. Setup may still be in progress — please try again later or contact support.";
      } else if (/OpenAI request failed/i.test(reason)) {
        reason = "The agent hit a temporary issue. Please try again in a moment.";
      }
      toast(reason);
      if (els.metadataAiSuggestStatus) els.metadataAiSuggestStatus.textContent = reason;
    } finally {
      els.metadataAiSuggestBtn.disabled = false;
    }
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

  function updateMetadataSplitViews() {
    const n = state.step;
    const isMeta = n === 2;
    const ed = state.metadataPane === "editor";
    if (els.viewMetadata) els.viewMetadata.classList.toggle("hidden", !isMeta || !ed);
    if (els.viewMetadataReview) els.viewMetadataReview.classList.toggle("hidden", !isMeta || ed);
  }

  function goMetadataReview() {
    if (!state.rows.length) return;
    state.metadataPane = "changesReview";
    saveSession();
    updateMetadataSplitViews();
    renderMetadataChangesReview();
  }

  function goMetadataEditor() {
    state.metadataPane = "editor";
    saveSession();
    updateMetadataSplitViews();
    renderMetadata();
  }

  function backToMetadataFromSynthetic() {
    state.metadataPane = "editor";
    saveSession();
    setStep(2);
  }

  function printMetadataChangeSummary() {
    document.body.classList.add("printing-changes-report");
    const cleanup = () => {
      document.body.classList.remove("printing-changes-report");
      window.removeEventListener("afterprint", cleanup);
    };
    window.addEventListener("afterprint", cleanup);
    setTimeout(() => {
      window.print();
      setTimeout(cleanup, 800);
    }, 30);
  }

  function revertSchemaExclusionsAll() {
    state.columnInclude = null;
    saveSession();
    renderMetadataChangesReview();
    toast("All columns are included in the synthetic schema again.");
  }

  function revertColumnMetadataSlice(col) {
    if (state.columnMetadataEdits) delete state.columnMetadataEdits[col];
    if (state.columnInclude && state.columnInclude[col] === false) {
      delete state.columnInclude[col];
      if (Object.keys(state.columnInclude).length === 0) state.columnInclude = null;
    }
    if (state.metadataAiAccepted && state.metadataAiAccepted.length) {
      state.metadataAiAccepted = state.metadataAiAccepted.filter(
        (r) => !(Array.isArray(r.related_columns) && r.related_columns.includes(col))
      );
    }
    saveSession();
    renderMetadataChangesReview();
    if (state.step === 2 && state.metadataPane === "editor") {
      renderMetadataAiSuggestBody();
      updateMetadataSectionNotesVisibility();
    }
    toast(`Reverted changes for ${col}.`);
  }

  function revertOneCorrelationPair(ca, cb) {
    const key = pairCorrelationKey(ca, cb);
    if (state.correlationEdits) delete state.correlationEdits[key];
    saveSession();
    renderMetadataChangesReview();
    toast("Correlation override removed for that pair.");
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

  function setStep(n) {
    if (n < 0 || n > 5) return;
    if (n >= 1 && !state.rows.length) return;
    const prev = state.step;
    state.step = n;
    if (prev === 2 && n !== 2) state.metadataPane = "editor";
    renderStepper();
    els.viewUpload.classList.toggle("hidden", n !== 0);
    els.viewInspect.classList.toggle("hidden", n !== 1);
    updateMetadataSplitViews();
    if (els.viewSynthetic) els.viewSynthetic.classList.toggle("hidden", n !== 3);
    if (els.viewReview) els.viewReview.classList.toggle("hidden", n !== 4);
    if (els.viewAnalyze) els.viewAnalyze.classList.toggle("hidden", n !== 5);
    if (n === 1) renderInspect();
    if (n === 2) {
      if (state.metadataPane === "editor") renderMetadata();
      else renderMetadataChangesReview();
    }
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
    state.columnInclude = null;
    state.columnMetadataEdits = {};
    state.correlationEdits = {};
    state.metadataPane = "editor";
    state.metadataSectionNotes = {};
    state.metadataChangeReviewNotes = {};
    resetMetadataAiCoachState();
  }

  function renderInspectStatic(colStats) {
    const nulls = colStats.reduce((a, c) => a + c.missing, 0);

    els.statsGrid.innerHTML = `
      <div class="stat-card"><div class="stat-value">${state.rows.length.toLocaleString()}</div><div class="stat-label">Rows</div></div>
      <div class="stat-card"><div class="stat-value">${state.headers.length}</div><div class="stat-label">Columns</div></div>
      <div class="stat-card"><div class="stat-value">${nulls.toLocaleString()}</div><div class="stat-label">Empty cells</div></div>
      <div class="stat-card"><div class="stat-value">${escapeHtml(state.fileName)}</div><div class="stat-label">File name</div></div>
    `;

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

    if (els.previewHead && els.previewBody) {
      els.previewHead.innerHTML = `<tr>${state.headers.map((h) => `<th title="${escapeAttr(h)}">${escapeHtml(h)}</th>`).join("")}</tr>`;
      const previewRows = state.rows.slice(0, 12);
      els.previewBody.innerHTML = previewRows
        .map(
          (r) =>
            `<tr>${state.headers.map((h) => `<td title="${escapeAttr(String(r[h] ?? ""))}">${escapeHtml(String(r[h] ?? ""))}</td>`).join("")}</tr>`
        )
        .join("");
    }

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

  function destroyChartList(list) {
    list.forEach((c) => c.destroy());
    list.length = 0;
  }

  function renderChartsInto(colStats, containerEl, chartList, idPrefix, opts) {
    if (!containerEl) return;
    const options = opts && typeof opts === "object" ? opts : {};
    const skipCategorical = options.skipCategorical === true;
    const skipNumeric = options.skipNumeric === true;
    const metaOverflow = options.metadataNumericOverflowCharts === true;
    const interactiveNums = options.metadataInteractiveNumericCols;
    const excludeNames =
      metaOverflow && Array.isArray(interactiveNums) ? new Set(interactiveNums.map((c) => c.name)) : null;
    const useEffectiveNumeric = metaOverflow === true;
    const numericChartLimit =
      typeof options.numericChartLimit === "number" && Number.isFinite(options.numericChartLimit)
        ? options.numericChartLimit
        : metaOverflow
          ? 2
          : 3;
    destroyChartList(chartList);
    containerEl.innerHTML = "";
    const Chart = window.Chart;
    if (!Chart) return;

    let numericCols = [];
    if (!skipNumeric) {
      const filtered = colStats.filter((c) => {
        if (!c.numericSample || c.numericSample.length <= 2) return false;
        const isNum = useEffectiveNumeric
          ? effectiveColumnKind(c, getEditForCol(c.name)) === "numeric"
          : c.inferred === "numeric";
        if (!isNum) return false;
        if (excludeNames && excludeNames.has(c.name)) return false;
        return true;
      });
      numericCols = orderColStatsForDistributions(filtered).slice(0, numericChartLimit);
    }
    const catCols = skipCategorical
      ? []
      : colStats.filter((c) => c.inferred === "text" && c.nonNull > 0).slice(0, 2);

    numericCols.forEach((c) => {
      const id = `${idPrefix}-num-${safeId(c.name)}`;
      const wrap = document.createElement("div");
      wrap.className = "chart-card";
      wrap.innerHTML = `<h4>Distribution — ${escapeHtml(c.name)}</h4><canvas id="${id}" height="200"></canvas>`;
      containerEl.appendChild(wrap);
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
      const labels = counts.map((_, i) => formatDistNumBinAxisDisplay(min + i * step));
      const ctx = document.getElementById(id);
      chartList.push(
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
      const id = `${idPrefix}-cat-${safeId(c.name)}`;
      const wrap = document.createElement("div");
      wrap.className = "chart-card";
      wrap.innerHTML = `<h4>Top categories — ${escapeHtml(c.name)}</h4><canvas id="${id}" height="220"></canvas>`;
      containerEl.appendChild(wrap);
      const ctx = document.getElementById(id);
      chartList.push(
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
      if (skipNumeric && skipCategorical) {
        containerEl.innerHTML =
          '<p class="panel-lead" style="margin:0">Interactive numeric and categorical distributions are edited in the section below.</p>';
      } else if (metaOverflow) {
        containerEl.innerHTML =
          '<p class="panel-lead" style="margin:0">Observed histograms for numeric columns that are not shown in the interactive editors below appear here when your dataset has additional suitable numeric fields.</p>';
      } else if (skipCategorical) {
        containerEl.innerHTML =
          '<p class="panel-lead" style="margin:0">No numeric columns to chart here. Use the category mix controls below for text fields.</p>';
      } else {
        containerEl.innerHTML =
          '<p class="panel-lead" style="margin:0">Not enough typed columns to chart automatically. Upload a sample with numeric or categorical fields.</p>';
      }
    }
  }

  function renderCharts(colStats) {
    renderChartsInto(colStats, els.chartsRow, state.charts, "chart");
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
      state.columnInclude = null;
      state.columnMetadataEdits = {};
      state.correlationEdits = {};
      state.metadataPane = "editor";
      state.metadataSectionNotes = {};
      state.metadataChangeReviewNotes = {};
      resetMetadataAiCoachState();
      state.issues = [];
      els.fileMeta.classList.remove("is-visible");
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

    if (els.btnProceedMetadata) {
      els.btnProceedMetadata.addEventListener("click", () => {
        if (!state.rows.length) return;
        setStep(2);
      });
    }

    if (els.btnBackInspectMeta) {
      els.btnBackInspectMeta.addEventListener("click", () => setStep(1));
    }

    if (els.btnProceedMetadataReview) {
      els.btnProceedMetadataReview.addEventListener("click", () => {
        if (!state.rows.length) return;
        goMetadataReview();
      });
    }

    if (els.btnBackMetadataEditor) {
      els.btnBackMetadataEditor.addEventListener("click", () => goMetadataEditor());
    }

    if (els.btnPrintMetadataChanges) {
      els.btnPrintMetadataChanges.addEventListener("click", () => printMetadataChangeSummary());
    }

    if (els.viewMetadataReview) {
      els.viewMetadataReview.addEventListener("click", (e) => {
        const btn = e.target.closest("[data-revert],[data-revert-column],[data-revert-corr-a],[data-revert-ai-id]");
        if (!btn) return;
        const col = btn.getAttribute("data-revert-column");
        const ca = btn.getAttribute("data-revert-corr-a");
        const cb = btn.getAttribute("data-revert-corr-b");
        const rv = btn.getAttribute("data-revert");
        const aiId = btn.getAttribute("data-revert-ai-id");
        if (aiId) {
          if (!state.metadataAiAccepted) state.metadataAiAccepted = [];
          state.metadataAiAccepted = state.metadataAiAccepted.filter((x) => x.id !== aiId);
          saveSession();
          renderMetadataChangesReview();
          renderMetadataAiSuggestBody();
          updateMetadataSectionNotesVisibility();
          if (state.step === 2 && state.metadataPane === "editor") renderMetadata();
          toast("Coach recommendation removed from synthesis metadata.");
          return;
        }
        if (col) {
          revertColumnMetadataSlice(col);
          return;
        }
        if (ca && cb) {
          revertOneCorrelationPair(ca, cb);
          return;
        }
        if (rv === "exclusions") {
          revertSchemaExclusionsAll();
          return;
        }
        if (rv === "correlations") {
          state.correlationEdits = {};
          saveSession();
          renderMetadataChangesReview();
          toast("All correlation targets reverted.");
        }
      });
      if (!els.viewMetadataReview.dataset.changeReviewInputBound) {
        els.viewMetadataReview.dataset.changeReviewInputBound = "1";
        els.viewMetadataReview.addEventListener("input", (e) => {
          const ta = e.target.closest("[data-change-review-note]");
          if (!ta) return;
          const key = ta.getAttribute("data-change-review-note");
          if (!key || !METADATA_NOTE_SECTIONS.includes(key)) return;
          if (!state.metadataChangeReviewNotes) state.metadataChangeReviewNotes = {};
          state.metadataChangeReviewNotes[key] = ta.value;
          saveSession();
        });
      }
    }

    if (els.btnProceedSyntheticFromReview) {
      els.btnProceedSyntheticFromReview.addEventListener("click", () => {
        if (!state.rows.length) return;
        state.metadataPane = "editor";
        saveSession();
        setStep(3);
      });
    }

    if (els.btnBackFromSynthetic) {
      els.btnBackFromSynthetic.addEventListener("click", () => {
        if (!state.rows.length) return;
        backToMetadataFromSynthetic();
      });
    }

    if (els.btnProceedReview) {
      els.btnProceedReview.addEventListener("click", () => {
        if (!state.rows.length) return;
        setStep(4);
      });
    }

    if (els.btnBackSynthetic) {
      els.btnBackSynthetic.addEventListener("click", () => setStep(3));
    }

    if (els.btnProceedAnalyze) {
      els.btnProceedAnalyze.addEventListener("click", () => {
        if (!state.rows.length) return;
        setStep(5);
      });
    }

    if (els.btnBackAnalyze) {
      els.btnBackAnalyze.addEventListener("click", () => setStep(4));
    }

    els.stepper.addEventListener("click", (e) => {
      const item = e.target.closest(".stepper-item");
      if (!item) return;
      const i = Number(item.dataset.step);
      if (i === 0) setStep(0);
      if (i === 1 && state.rows.length) setStep(1);
      if (i === 2 && state.rows.length) {
        state.metadataPane = "editor";
        saveSession();
        setStep(2);
      }
      if (i === 3 && state.rows.length) setStep(3);
      if (i === 4 && state.rows.length) setStep(4);
      if (i === 5 && state.rows.length) setStep(5);
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
