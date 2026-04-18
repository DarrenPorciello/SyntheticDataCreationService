(function () {
  "use strict";

  const STORAGE_KEY = "southlake_csv_session_v1";
  const STEPS = [
    { id: "input", name: "Input data", desc: "Upload your CSV" },
    { id: "inspect", name: "Data inspection", desc: "Quality & summary" },
    { id: "metadata", name: "Metadata", desc: "Schema for synthesis" },
    { id: "synthetic", name: "Synthetic data", desc: "Generate" },
    { id: "review", name: "Review", desc: "Validate" },
  ];

  const METADATA_SCHEMA_VERSION = "1.2";

  const COLUMN_SYNTH_EDIT_KEYS = [
    "synthDist",
    "synthMin",
    "synthMax",
    "synthMean",
    "synthVariance",
    "synthCatMode",
    "synthCatMergePct",
    "synthCatCustom",
  ];

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
    els.viewSynthetic = $("view-synthetic");
    els.viewReview = $("view-review");
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
    els.btnProceedSynthetic = $("btn-proceed-synthetic");
    els.btnBackMetadata = $("btn-back-metadata");
    els.btnProceedReview = $("btn-proceed-review");
    els.btnBackSynthetic = $("btn-back-synthetic");
    els.metadataHygieneList = $("metadata-hygiene-list");
    els.metadataDashboardStats = $("metadata-dashboard-stats");
    els.metadataDashboardColumns = $("metadata-dashboard-columns");
    els.metadataJson = $("metadata-json");
    els.metadataChartsRow = $("metadata-charts-row");
    els.metadataCorrelation = $("metadata-correlation");
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
    els.metadataSynthResetAllWrap = $("metadata-synth-reset-all-wrap");
    els.btnResetAllSynth = $("btn-reset-all-synth");
    els.metadataColumnsSynthResetWrap = $("metadata-columns-synth-reset-wrap");
    els.btnResetColumnsSynth = $("btn-reset-columns-synth");
    els.metadataCorrSynthResetWrap = $("metadata-corr-synth-reset-wrap");
    els.btnResetCorrSynth = $("btn-reset-corr-synth");
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
    if (!Object.keys(t).length) return undefined;
    if (observedSummary) t.numeric_summary_observed_reference = observedSummary;
    return t;
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
    const bodyObs = buildCorrelationTableBodyObserved(cols, labels, srcMat);
    const bodySynth = buildCorrelationTableBodySynthetic(cols, labels, mat, srcMat);
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
        `Both tables show the first ${cols.length} numeric column(s) in file order (${block.numeric_column_count} numeric total).`
      );
    }
    el.innerHTML = `
      <div class="corr-legend" aria-hidden="true">
        <span class="corr-legend-item"><span class="corr-swatch neg"></span> Negative r</span>
        <span class="corr-legend-item"><span class="corr-swatch neu"></span> Weak / diagonal</span>
        <span class="corr-legend-item"><span class="corr-swatch pos"></span> Positive r</span>
      </div>
      <p class="corr-dual-caption">Compare <strong>observed</strong> (from your data) with <strong>synthetic targets</strong> (used for generation). Cell colors follow the value in each table.</p>
      <div class="corr-matrices-row">
        <div class="corr-matrix-block">
          <h4 class="corr-matrix-label">Observed (from file)</h4>
          <div class="corr-table-scroll">
            <table class="corr-table" aria-label="Observed Pearson correlations"><thead>${thead}</thead><tbody>${bodyObs}</tbody></table>
          </div>
        </div>
        <div class="corr-matrix-block">
          <h4 class="corr-matrix-label">Synthetic targets (for generation)</h4>
          <div class="corr-table-scroll">
            <table class="corr-table" aria-label="Synthetic target Pearson correlations"><thead>${thead}</thead><tbody>${bodySynth}</tbody></table>
          </div>
        </div>
      </div>
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
      transparency: {
        how_source_becomes_metadata: [
          "The CSV is parsed locally. We never send the full file to a model unless you run the optional inspection API call (sample rows only).",
          "Each column is profiled: type guess, counts, missingness, distinctness, and either numeric summaries or top category frequencies.",
          "This object is the descriptive metadata package: it is what a synthetic data engine uses to reproduce shape and marginals without copying raw rows.",
          "Use the dashboard to include or exclude columns, then Edit to adjust labels, schema type intent, and notes — those adjustments are written into the JSON package.",
          "Pairwise Pearson correlations summarize linear co-movement between numeric columns on rows where both values parse as numbers (see numeric_correlation_pearson).",
          "matrix holds synthesis targets; matrix_source keeps observed r for the same column order. Click heatmap cells to override linear association targets for synthetic data.",
          "Per-column Edit metadata can set synthetic numeric shape/range/moments and categorical mix strategies — see synthetic_numeric_targets and synthetic_categorical_targets on each column.",
        ],
      },
    };
  }

  let metadataViewEventsBound = false;
  let metadataModalColumn = null;
  let metadataModalColStat = null;
  let corrModalColA = null;
  let corrModalColB = null;

  function bindMetadataViewEvents() {
    if (metadataViewEventsBound || !els.viewMetadata) return;
    metadataViewEventsBound = true;
    els.viewMetadata.addEventListener("change", (e) => {
      const t = e.target;
      if (!t.classList || !t.classList.contains("meta-include-cb")) return;
      const col = t.getAttribute("data-col");
      if (!col) return;
      if (!state.columnInclude) state.columnInclude = {};
      state.columnInclude[col] = t.checked;
      saveSession();
      renderMetadata();
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

  function updateSyntheticResetUiVisibility() {
    const col = hasColumnSynthOverrides();
    const corr = hasCorrelationSynthOverrides();
    const any = col || corr;
    if (els.metadataColumnsSynthResetWrap) els.metadataColumnsSynthResetWrap.classList.toggle("hidden", !col);
    if (els.metadataCorrSynthResetWrap) els.metadataCorrSynthResetWrap.classList.toggle("hidden", !corr);
    if (els.metadataSynthResetAllWrap) els.metadataSynthResetAllWrap.classList.toggle("hidden", !any);
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
    if (!state.rows.length || !els.metadataHygieneList || !els.metadataDashboardStats || !els.metadataDashboardColumns || !els.metadataJson) return;

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
        return `<article class="meta-dash-card">
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
    renderChartsInto(colStats, els.metadataChartsRow, state.metaCharts, "mchart");
    updateSyntheticResetUiVisibility();
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

  function setStep(n) {
    if (n < 0 || n > 4) return;
    if (n >= 1 && !state.rows.length) return;
    state.step = n;
    renderStepper();
    els.viewUpload.classList.toggle("hidden", n !== 0);
    els.viewInspect.classList.toggle("hidden", n !== 1);
    if (els.viewMetadata) els.viewMetadata.classList.toggle("hidden", n !== 2);
    if (els.viewSynthetic) els.viewSynthetic.classList.toggle("hidden", n !== 3);
    if (els.viewReview) els.viewReview.classList.toggle("hidden", n !== 4);
    if (n === 1) renderInspect();
    if (n === 2) renderMetadata();
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

  function renderChartsInto(colStats, containerEl, chartList, idPrefix) {
    if (!containerEl) return;
    destroyChartList(chartList);
    containerEl.innerHTML = "";
    const Chart = window.Chart;
    if (!Chart) return;

    const numericCols = colStats.filter((c) => c.inferred === "numeric" && c.numericSample.length > 2).slice(0, 3);
    const catCols = colStats
      .filter((c) => c.inferred === "text" && c.nonNull > 0)
      .slice(0, 2);

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
      const labels = counts.map((_, i) => (min + i * step).toFixed(2));
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
      containerEl.innerHTML =
        '<p class="panel-lead" style="margin:0">Not enough typed columns to chart automatically. Upload a sample with numeric or categorical fields.</p>';
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

    if (els.btnProceedSynthetic) {
      els.btnProceedSynthetic.addEventListener("click", () => {
        if (!state.rows.length) return;
        setStep(3);
      });
    }

    if (els.btnBackMetadata) {
      els.btnBackMetadata.addEventListener("click", () => setStep(2));
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

    els.stepper.addEventListener("click", (e) => {
      const item = e.target.closest(".stepper-item");
      if (!item) return;
      const i = Number(item.dataset.step);
      if (i === 0) setStep(0);
      if (i === 1 && state.rows.length) setStep(1);
      if (i === 2 && state.rows.length) setStep(2);
      if (i === 3 && state.rows.length) setStep(3);
      if (i === 4 && state.rows.length) setStep(4);
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
