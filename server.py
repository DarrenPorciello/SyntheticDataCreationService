"""
Local API + static UI. Loads OPENAI_API_KEY from .env (never sent to the browser).
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from openai import AsyncOpenAI
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent

# Paths we attempted to read (for /api/health diagnostics).
_ENV_FILES_CHECKED: list[str] = []


def _normalize_env_line_chars(line: str) -> str:
    """Map common Unicode punctuation to ASCII so KEY=value survives odd editors."""
    return (
        line.replace("\uff1d", "=")  # FULLWIDTH EQUALS U+FF1D
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2018", "'")
        .replace("\u2019", "'")
    )


def _parse_env_line(line: str) -> tuple[str, str] | None:
    s = _normalize_env_line_chars(line).strip()
    if not s or s.startswith("#"):
        return None
    if "=" not in s:
        return None
    key, _, val = s.partition("=")
    key = key.strip().lstrip("\ufeff")
    if key.lower().startswith("export "):
        key = key[7:].strip()
    val = val.strip()
    if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
        val = val[1:-1].strip()
    return key, val


def _read_env_file_text(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        data = path.read_bytes()
    except OSError:
        return None
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return None


def _apply_env_file(path: Path) -> bool:
    """Read KEY=value lines and set os.environ (overrides existing for those keys)."""
    raw = _read_env_file_text(path)
    if raw is None:
        return False
    for line in raw.splitlines():
        parsed = _parse_env_line(line)
        if not parsed:
            continue
        k, v = parsed
        if not k:
            continue
        if k == "OPENAI_API_KEY" and not (v or "").strip():
            continue
        os.environ[k] = v
    return True


def _bootstrap_env() -> None:
    """Load env files: optional explicit path, then .env next to this server.py, then cwd."""
    global _ENV_FILES_CHECKED  # noqa: PLW0603

    _ENV_FILES_CHECKED = []
    explicit = (os.getenv("SOUTHLAKE_ENV_FILE") or os.getenv("DOTENV_PATH") or "").strip()
    # cwd/.env first, then .env next to server.py — so a stray empty OPENAI_API_KEY in the shell's
    # working directory (e.g. home folder) cannot overwrite the real project key.
    ordered: list[Path] = []
    ordered.append(Path.cwd() / ".env")
    ordered.append(BASE_DIR / ".env")
    if explicit:
        ordered.append(Path(explicit).expanduser())

    seen: set[str] = set()
    for p in ordered:
        rp = str(p.resolve())
        if rp in seen:
            continue
        seen.add(rp)
        _ENV_FILES_CHECKED.append(rp)
        if not p.is_file():
            continue
        load_dotenv(p, encoding="utf-8-sig", override=True)
        if not (os.getenv("OPENAI_API_KEY") or "").strip():
            os.environ.pop("OPENAI_API_KEY", None)
        _apply_env_file(p)

    if not (os.getenv("OPENAI_API_KEY") or "").strip():
        print(
            "[southlake] OPENAI_API_KEY is still empty after loading env files.",
            "server.py directory:",
            str(BASE_DIR),
            "cwd:",
            str(Path.cwd().resolve()),
            file=sys.stderr,
        )
        print("[southlake] Tried paths:", "; ".join(_ENV_FILES_CHECKED) or "(none)", file=sys.stderr)
        for label, p in (("next to server.py", BASE_DIR / ".env"), ("cwd", Path.cwd() / ".env")):
            ex = p.is_file()
            print(f"[southlake] {label}: {p.resolve()} exists={ex}", file=sys.stderr)


_bootstrap_env()


def _openai_api_key() -> str:
    raw = (os.getenv("OPENAI_API_KEY") or "").strip()
    if len(raw) >= 2 and ((raw[0] == raw[-1] == '"') or (raw[0] == raw[-1] == "'")):
        raw = raw[1:-1].strip()
    return raw


MAX_SAMPLE_ROWS = 25
MAX_CELL_LEN = 160
MAX_COLUMNS = 80


class ColumnStatIn(BaseModel):
    name: str
    inferred: str
    nonNull: int
    missing: int
    unique: int


class IssueIn(BaseModel):
    sev: str = "low"
    title: str
    detail: str


class QualityAnalyzeRequest(BaseModel):
    file_name: str = ""
    row_count: int = Field(ge=0)
    headers: list[str] = Field(default_factory=list)
    column_stats: list[ColumnStatIn] = Field(default_factory=list)
    sample_rows: list[dict[str, str]] = Field(default_factory=list)
    deterministic_findings: list[IssueIn] = Field(default_factory=list)


class IssueOut(BaseModel):
    sev: str
    title: str
    detail: str
    suggested_fix: str = ""


class QualityAnalyzeResponse(BaseModel):
    summary: str
    issues: list[IssueOut]


class MetadataSuggestionItemOut(BaseModel):
    title: str
    detail: str
    importance: str = "medium"
    related_columns: list[str] = Field(default_factory=list)
    suggested_action: str = ""


class MetadataSuggestRequest(BaseModel):
    file_name: str = ""
    row_count: int = Field(ge=0)
    headers: list[str] = Field(default_factory=list)
    column_stats: list[ColumnStatIn] = Field(default_factory=list)
    sample_rows: list[dict[str, str]] = Field(default_factory=list)
    hygiene_issues: list[IssueIn] = Field(default_factory=list)
    schema_context: dict = Field(default_factory=dict)


class MetadataSuggestResponse(BaseModel):
    summary: str
    suggestions: list[MetadataSuggestionItemOut]


class ReviewNumericDeltaIn(BaseModel):
    column: str = ""
    orig_mean: float | None = None
    synth_mean: float | None = None
    orig_std: float | None = None
    synth_std: float | None = None


class ReviewCorrDeltaIn(BaseModel):
    column_a: str = ""
    column_b: str = ""
    r_orig: float | None = None
    r_synth: float | None = None


class ReviewCatDeltaIn(BaseModel):
    column: str = ""
    orig_distinct: int = Field(ge=0, default=0)
    synth_distinct: int = Field(ge=0, default=0)
    orig_top_pct: float | None = None
    synth_top_pct: float | None = None


class ReviewSyntheticCheckRequest(BaseModel):
    file_name: str = Field(default="", max_length=240)
    synthetic_goal: str = Field(default="", max_length=4000)
    orig_row_count: int = Field(ge=0)
    synth_row_count: int = Field(ge=0)
    fidelity_score: int = Field(ge=0, le=100)
    fidelity_tier: str = Field(default="", max_length=48)
    fidelity_rationale: str = Field(default="", max_length=4500)
    headers_sample: list[str] = Field(default_factory=list, max_length=40)
    numeric_deltas: list[ReviewNumericDeltaIn] = Field(default_factory=list, max_length=36)
    correlation_deltas: list[ReviewCorrDeltaIn] = Field(default_factory=list, max_length=48)
    categorical_deltas: list[ReviewCatDeltaIn] = Field(default_factory=list, max_length=24)
    sample_orig_rows: list[dict[str, str]] = Field(default_factory=list, max_length=12)
    sample_synth_rows: list[dict[str, str]] = Field(default_factory=list, max_length=12)


class ReviewSyntheticPointOut(BaseModel):
    title: str
    detail: str
    sev: str = "medium"


class ReviewSyntheticCheckResponse(BaseModel):
    summary: str
    points: list[ReviewSyntheticPointOut]


SYNTH_AI_MAX_ROWS = int(os.getenv("SYNTH_AI_MAX_ROWS", "120"))
SYNTH_AI_MAX_OUTPUT_TOKENS = int(os.getenv("SYNTH_AI_MAX_OUTPUT_TOKENS", "32000"))


SYNTH_AI_MAX_INPUT_CHARS = int(os.getenv("SYNTH_AI_MAX_INPUT_CHARS", "180000"))


class SyntheticAiRequest(BaseModel):
    headers: list[str] = Field(default_factory=list, max_length=120)
    row_count: int = Field(ge=1, le=500)
    goal: str = Field(default="", max_length=4000)
    # Column marginals + optional correlation block (plain text CSV). Preferred over JSON for token limits.
    baseline_metadata_csv: str = Field(default="", max_length=650_000)
    # Optional legacy small JSON baseline; ignored when baseline_metadata_csv is sent.
    baseline_metadata: dict[str, Any] | None = None
    sample_rows: list[dict[str, str]] = Field(default_factory=list, max_length=12)


class SyntheticAiResponse(BaseModel):
    rows: list[dict[str, str]]
    model: str
    rows_in_response: int
    note: str = ""


def _normalize_ai_synth_row(row: object, headers: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    if not isinstance(row, dict):
        row = {}
    for h in headers:
        v = row.get(h, "")
        if v is None:
            out[h] = ""
        elif isinstance(v, bool):
            out[h] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[h] = str(v)
        else:
            s = str(v).replace("\r\n", "\n").replace("\r", "\n")
            if len(s) > 8000:
                s = s[:7999] + "…"
            out[h] = s
    return out


app = FastAPI(title="Synthetix API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _truncate_cell(v: object) -> str:
    s = "" if v is None else str(v)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    if len(s) > MAX_CELL_LEN:
        return s[: MAX_CELL_LEN - 1] + "…"
    return s


def _parse_json_object(content: str) -> dict:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


def _normalize_review_points(raw: list) -> list[ReviewSyntheticPointOut]:
    out: list[ReviewSyntheticPointOut] = []
    if not isinstance(raw, list):
        return out
    for item in raw[:4]:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip() or "Note"
        detail = str(item.get("detail", "")).strip() or "—"
        sev = str(item.get("sev", "medium")).lower()
        if sev not in ("high", "medium", "low"):
            sev = "medium"
        out.append(ReviewSyntheticPointOut(title=title[:200], detail=detail[:1200], sev=sev))
    return out


def _normalize_issues(raw: list) -> list[IssueOut]:
    out: list[IssueOut] = []
    if not isinstance(raw, list):
        return out
    for item in raw[:4]:
        if not isinstance(item, dict):
            continue
        sev = str(item.get("sev", "low")).lower()
        if sev not in ("high", "medium", "low"):
            sev = "low"
        title = str(item.get("title", "")).strip() or "Finding"
        detail = str(item.get("detail", "")).strip() or "—"
        sf = str(item.get("suggested_fix", "")).strip()
        out.append(IssueOut(sev=sev, title=title[:200], detail=detail[:1200], suggested_fix=sf[:900]))
    return out


@app.get("/api/health")
def health(diagnose: bool = False):
    key = _openai_api_key()
    primary = BASE_DIR / ".env"
    out: dict = {
        "ok": True,
        "openai_configured": bool(key),
        "openai_key_char_length": len(key),
        "server_dir": str(BASE_DIR),
        "process_cwd": str(Path.cwd().resolve()),
        "env_file_next_to_server_py": str(primary.resolve()),
        "env_file_exists": primary.is_file(),
    }
    if diagnose:
        candidates = [str((BASE_DIR / ".env").resolve()), str((Path.cwd() / ".env").resolve())]
        out["env_candidates"] = [{"path": p, "exists": Path(p).is_file()} for p in candidates]
        out["files_read_attempt"] = list(dict.fromkeys(_ENV_FILES_CHECKED))
    return out


@app.post("/api/quality-analyze", response_model=QualityAnalyzeResponse)
async def quality_analyze(body: QualityAnalyzeRequest):
    api_key = _openai_api_key()
    if not api_key:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY is not set in the server environment.")

    stats = body.column_stats[:MAX_COLUMNS]
    headers = body.headers[:MAX_COLUMNS]
    sample: list[dict[str, str]] = []
    for row in body.sample_rows[:MAX_SAMPLE_ROWS]:
        slim: dict[str, str] = {}
        for h in headers:
            if h in row:
                slim[h] = _truncate_cell(row[h])
        sample.append(slim)

    det = [i.model_dump() for i in body.deterministic_findings[:30]]

    payload = {
        "file_name": body.file_name,
        "row_count": body.row_count,
        "column_stats": [s.model_dump() for s in stats],
        "sample_rows": sample,
        "deterministic_findings": det,
    }

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    system = """You are a senior data steward helping prepare tabular data for synthetic data generation in healthcare and population-health contexts.

You receive: file metadata, per-column counts/types, a small sample of rows, and deterministic hygiene checks from software.

Your job:
1. Write a concise executive summary (2–4 sentences) for a non-technical stakeholder about data hygiene and synthetic-data readiness.
2. Produce a prioritized list of hygiene / quality issues (**at most 4**). The **issues** array MUST contain **at least 1** item—even if the data look clean (use a single low-severity synthetic-readiness or documentation nudge). You may rephrase, merge, or refine deterministic_findings, add important issues the code missed, and drop clear false positives. Use severity: high, medium, or low.
3. For EACH issue, add "suggested_fix": 1–3 short sentences stating plainly what the system does **for this dataset** when the user saves that fix into the session (e.g. "Adds this remediation to synthesis metadata so synthetic data and reports follow it.", "Documents that row-level cleanup should use Apply fixes (trim / dedupe) and carries that intent in the session package."). Write as direct facts—no "if you accept" or "when you click Accept" framing. Be accurate: saving persists guidance in metadata; it does not silently rewrite their CSV unless they use separate fix actions. Use an empty string only if no reasonable system-side outcome exists.

Respond with ONLY valid JSON matching this shape (no markdown fences):
{"summary": string, "issues": [{"sev": "high"|"medium"|"low", "title": string, "detail": string, "suggested_fix": string}]}

**issues** must be a non-empty array (minimum length 1, **maximum 4** — prioritize the most important items only).

If sample data might resemble real people, avoid repeating exact identifiers in titles; refer to column names instead where possible."""

    user = json.dumps(payload, ensure_ascii=False)

    try:
        client = AsyncOpenAI(api_key=api_key)
        completion = await client.chat.completions.create(
            model=model,
            temperature=0.3,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenAI request failed: {e!s}") from e

    content = completion.choices[0].message.content
    if not content:
        raise HTTPException(status_code=502, detail="Empty response from model.")

    try:
        data = _parse_json_object(content)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=502, detail=f"Model returned invalid JSON: {e}") from e

    summary = str(data.get("summary", "")).strip() or "Assessment complete."
    issues = _normalize_issues(data.get("issues", []))
    if not issues:
        issues = [
            IssueOut(
                sev="low",
                title="No structured issues returned",
                detail="The model did not return issue items; review the summary and raw data manually.",
                suggested_fix="Adds a placeholder hygiene note to this session’s synthesis metadata so the run is still documented when the model returns no items.",
            )
        ]
    _hygiene_fix_fallback = (
        "Adds this hygiene note to synthesis metadata for this dataset so synthetic generation and change reports stay aligned with the finding."
    )
    issues = [
        i.model_copy(
            update={"suggested_fix": ((i.suggested_fix or "").strip() or _hygiene_fix_fallback)[:900]}
        )
        for i in issues
    ]
    issues = issues[:4]

    return QualityAnalyzeResponse(summary=summary[:4000], issues=issues)


def _normalize_metadata_suggestions(raw: list) -> list[MetadataSuggestionItemOut]:
    out: list[MetadataSuggestionItemOut] = []
    if not isinstance(raw, list):
        return out
    for item in raw[:3]:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip() or "Suggestion"
        detail = str(item.get("detail", "")).strip() or "—"
        rc = item.get("related_columns", [])
        cols: list[str] = []
        if isinstance(rc, list):
            for x in rc[:3]:
                if isinstance(x, str) and x.strip():
                    cols.append(x.strip()[:120])
        imp = str(item.get("importance", "medium")).strip().lower()
        if imp not in ("high", "medium", "low"):
            imp = "medium"
        act = str(item.get("suggested_action", "")).strip()
        out.append(
            MetadataSuggestionItemOut(
                title=title[:120],
                detail=detail[:900],
                importance=imp,
                related_columns=cols,
                suggested_action=act[:280],
            )
        )
    return out


@app.post("/api/metadata-suggest", response_model=MetadataSuggestResponse)
async def metadata_suggest(body: MetadataSuggestRequest):
    api_key = _openai_api_key()
    if not api_key:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY is not set in the server environment.")

    stats = body.column_stats[:MAX_COLUMNS]
    headers = body.headers[:MAX_COLUMNS]
    sample: list[dict[str, str]] = []
    for row in body.sample_rows[:MAX_SAMPLE_ROWS]:
        slim: dict[str, str] = {}
        for h in headers:
            if h in row:
                slim[h] = _truncate_cell(row[h])
        sample.append(slim)

    hy = [i.model_dump() for i in body.hygiene_issues[:24]]
    ctx = body.schema_context if isinstance(body.schema_context, dict) else {}
    try:
        ctx_json = json.dumps(ctx, ensure_ascii=False)
    except (TypeError, ValueError):
        ctx_json = "{}"
    if len(ctx_json) > 24000:
        schema_ctx: dict = {
            "_truncated": True,
            "note": "schema_context exceeded size limit; omitting. Reduce columns or edit payload size.",
        }
    else:
        schema_ctx = json.loads(ctx_json)

    payload = {
        "file_name": body.file_name,
        "row_count": body.row_count,
        "column_stats": [s.model_dump() for s in stats],
        "sample_rows": sample,
        "hygiene_issues": hy,
        "schema_context": schema_ctx,
    }

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    system = """You are the **AI Metadata Agent** for Synthetix. You speak directly to customers and analysts who are shaping metadata so the system can generate **high-quality synthetic data** that reflects their real dataset.

You receive (as JSON): dataset shape, per-column profile stats, a small row sample, hygiene notes from an earlier quality pass, and **schema_context** (columns excluded from synthesis, user edits to labels/types/notes, synthetic distribution hints, and correlation targets).

Respond with ONLY valid JSON (no markdown, no code fences):
{"summary": string, "suggestions": array}

**summary** — One or two short sentences in plain, confident language: what stands out about their metadata and how ready it looks for synthetic generation. No jargon about systems or infrastructure.

**suggestions** — Between **0 and 3** objects. Return suggestions only when there is a clear metadata improvement to make. If metadata already looks appropriate, return an empty array.

Each suggestion object:
{"title": string, "detail": string, "importance": "high"|"medium"|"low", "related_columns": string[], "suggested_action": string}

- **title**: Benefit-focused, ≤8 words.
- **detail**: 1–2 short sentences, friendly and specific.
- **importance**: high, medium, or low priority for impact on synthetic quality.
- **related_columns**: Up to 3 names; every name MUST appear in the provided headers/column_stats. Use [] if none.
- **suggested_action**: One short imperative line the user can follow in the metadata UI (optional; use "" if redundant).

Rules:
- Never invent column names.
- Avoid repeating the same idea twice.
- Do **not** re-suggest generic hygiene fixes already covered in inspection (e.g., dedupe, trim whitespace, drop empty rows) unless you convert them into a specific metadata change with clear synthetic-quality impact.
- Keep every suggestion dataset-specific by citing concrete column context and why the metadata change will improve synthetic realism.
- Do not mention APIs, keys, models, servers, or "OpenAI".
- Write as the in-product agent."""

    user = json.dumps(payload, ensure_ascii=False)

    try:
        client = AsyncOpenAI(api_key=api_key)
        completion = await client.chat.completions.create(
            model=model,
            temperature=0.28,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenAI request failed: {e!s}") from e

    content = completion.choices[0].message.content
    if not content:
        raise HTTPException(status_code=502, detail="Empty response from model.")

    try:
        data = _parse_json_object(content)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=502, detail=f"Model returned invalid JSON: {e}") from e

    summary = str(data.get("summary", "")).strip() or "Here is a quick read on your metadata for synthetic data."
    suggestions = _normalize_metadata_suggestions(data.get("suggestions", []))[:3]

    return MetadataSuggestResponse(summary=summary[:1200], suggestions=suggestions)


@app.post("/api/review-synthetic-check", response_model=ReviewSyntheticCheckResponse)
async def review_synthetic_check(body: ReviewSyntheticCheckRequest):
    """OpenAI: short synthetic-vs-original review from compact client summaries and small samples."""
    api_key = _openai_api_key()
    if not api_key:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY is not set in the server environment.")

    hdrs = [str(h) for h in body.headers_sample if str(h).strip()][:40]
    sample_o: list[dict[str, str]] = []
    sample_s: list[dict[str, str]] = []
    for row in body.sample_orig_rows[:12]:
        if not isinstance(row, dict):
            continue
        slim: dict[str, str] = {}
        for h in hdrs:
            if h in row:
                slim[h] = _truncate_cell(row[h])
        sample_o.append(slim)
    for row in body.sample_synth_rows[:12]:
        if not isinstance(row, dict):
            continue
        slim: dict[str, str] = {}
        for h in hdrs:
            if h in row:
                slim[h] = _truncate_cell(row[h])
        sample_s.append(slim)

    payload = {
        "file_name": body.file_name,
        "synthetic_goal": (body.synthetic_goal or "").strip()[:2000],
        "orig_row_count": body.orig_row_count,
        "synth_row_count": body.synth_row_count,
        "fidelity_score_0_100": body.fidelity_score,
        "fidelity_tier_label": (body.fidelity_tier or "").strip(),
        "fidelity_rationale_bullets_plain_text": (body.fidelity_rationale or "").strip()[:4000],
        "numeric_deltas": [d.model_dump() for d in body.numeric_deltas[:36]],
        "correlation_deltas": [d.model_dump() for d in body.correlation_deltas[:48]],
        "categorical_deltas": [d.model_dump() for d in body.categorical_deltas[:24]],
        "sample_orig_rows": sample_o,
        "sample_synth_rows": sample_s,
    }

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    system = """You are a senior data steward reviewing **synthetic tabular data** against a **real working table** in the same project.

You receive JSON only (no raw database): file name, row counts, a short user goal for the synthetic run, a heuristic fidelity score and plain-text rationale produced by software, compact numeric mean/std deltas per column, Pearson correlation deltas for numeric pairs, categorical distinct counts and top-category share deltas, and two tiny row samples (original vs synthetic).

Your job:
1. Write a **summary** (3–5 sentences) tuned to **this** dataset: what looks aligned, what diverges, and what a human reviewer should double-check before trusting the synthetic file for their intended use (testing, demos, modeling, etc.). Speak plainly; avoid repeating the numeric tables verbatim.
2. Return **points**: **1 to 4** short review bullets (never more than 4). Each item: title (≤12 words), detail (1–2 sentences), sev high|medium|low based on practical risk to misuse (not statistical p-values). Cover correlation structure, tails/percentiles if implied by deltas, category cardinality shifts, and row-count mismatch when relevant. If the sample might resemble real people, do not quote exact values from cells; refer to columns instead.

Respond with ONLY valid JSON (no markdown fences):
{"summary": string, "points": [{"title": string, "detail": string, "sev": "high"|"medium"|"low"}]}"""

    user = json.dumps(payload, ensure_ascii=False)

    try:
        client = AsyncOpenAI(api_key=api_key)
        completion = await client.chat.completions.create(
            model=model,
            temperature=0.35,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenAI request failed: {e!s}") from e

    content = completion.choices[0].message.content
    if not content:
        raise HTTPException(status_code=502, detail="Empty response from model.")

    try:
        data = _parse_json_object(content)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=502, detail=f"Model returned invalid JSON: {e}") from e

    summary = str(data.get("summary", "")).strip() or "Review complete."
    points = _normalize_review_points(data.get("points", []))[:4]
    if not points:
        points = [
            ReviewSyntheticPointOut(
                sev="low",
                title="No structured points returned",
                detail="The model did not return point items; rely on the summary and the in-app tables.",
            )
        ]

    return ReviewSyntheticCheckResponse(summary=summary[:3500], points=points)


@app.post("/api/synthetic-generate-ai", response_model=SyntheticAiResponse)
async def synthetic_generate_ai(body: SyntheticAiRequest):
    """Generate synthetic rows via OpenAI using client-supplied *baseline* metadata (no user overrides)."""
    api_key = _openai_api_key()
    if not api_key:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY is not set in the server environment.")

    hdrs = [str(h) for h in body.headers if str(h).strip()]
    if not hdrs:
        raise HTTPException(status_code=400, detail="headers is required.")

    n_req = min(int(body.row_count), SYNTH_AI_MAX_ROWS)
    note_parts: list[str] = []
    if int(body.row_count) > SYNTH_AI_MAX_ROWS:
        note_parts.append(f"Row count capped to {SYNTH_AI_MAX_ROWS} for this API (requested {body.row_count}).")

    sample: list[dict[str, str]] = []
    for row in body.sample_rows[:10]:
        if not isinstance(row, dict):
            continue
        slim: dict[str, str] = {}
        for h in hdrs:
            if h in row:
                slim[h] = _truncate_cell(row[h])
        sample.append(slim)

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    csv_part = (body.baseline_metadata_csv or "").strip()
    legacy_meta = body.baseline_metadata if isinstance(body.baseline_metadata, dict) else {}
    use_legacy_json = len(csv_part) < 40 and bool(legacy_meta)

    if not use_legacy_json and len(csv_part) < 20:
        raise HTTPException(
            status_code=400,
            detail="Send baseline_metadata_csv (column marginals as CSV). It is required for normal requests.",
        )

    system = f"""You generate synthetic tabular data for privacy-safe testing.

You MUST respond with ONLY valid JSON (no markdown, no code fences) matching exactly this shape:
{{"rows": [ ... ]}}

Rules:
- The array "rows" MUST contain exactly {n_req} objects (no more, no fewer).
- Every object MUST have these keys exactly once, in this order: {json.dumps(hdrs, ensure_ascii=False)}
- Every value MUST be a JSON string (use digits and optional sign/dot for numbers). Use "" for intentionally missing values when missing_rate in the CSV suggests emptiness.
- The user message includes **column_marginals_csv**: a CSV file (header row + one row per column) with columns:
  name, included (1=yes 0=excluded from schema), dtype (numeric|text), missing_rate (0–1),
  n_min, n_max, n_mean, n_std (numeric summaries; empty if not numeric),
  hist_bin_p (pipe | separated bin proportions left-to-right for equal-width histogram on that column),
  cat_value_props (semicolon ; separated items value:proportion for categorical marginals; values may use middle-dot instead of colon inside text).
  There may be a second section starting with a line "=== correlation_pearson_observed" — Pearson r matrix (-1..1); honor approximate linear structure between numeric columns when sampling if reasonable.
- Respect dtype and included: excluded columns (included=0) must be "" in output.
- Do NOT copy any row from sample_rows_json verbatim; use it only for formatting cues.
- Invent plausible values; never claim to be real individuals or use real identifiers from the sample."""

    sample_json = json.dumps(sample, ensure_ascii=False)
    if use_legacy_json:
        legacy_dump = json.dumps(
            {"baseline_metadata": legacy_meta, "sample_rows": sample, "row_count": n_req, "goal": (body.goal or "").strip()[:4000]},
            ensure_ascii=False,
        )
        user = f"headers_json: {json.dumps(hdrs, ensure_ascii=False)}\nlegacy_json_payload:\n{legacy_dump}"
        if len(user) > SYNTH_AI_MAX_INPUT_CHARS:
            raise HTTPException(
                status_code=400,
                detail="Legacy baseline_metadata JSON is too large; use baseline_metadata_csv from the app instead.",
            )
    else:
        user = (
            f"task: generate_synthetic_rows\n"
            f"row_count: {n_req}\n"
            f"headers_json: {json.dumps(hdrs, ensure_ascii=False)}\n"
            f"goal: {(body.goal or '').strip()[:4000]}\n\n"
            f"column_marginals_csv:\n{csv_part}\n\n"
            f"sample_rows_json:\n{sample_json}\n"
        )
        if len(user) > SYNTH_AI_MAX_INPUT_CHARS:
            keep = max(5000, SYNTH_AI_MAX_INPUT_CHARS - len(sample_json) - 2000)
            csv_part = csv_part[:keep] + "\n# …truncated…\n"
            user = (
                f"task: generate_synthetic_rows\n"
                f"row_count: {n_req}\n"
                f"headers_json: {json.dumps(hdrs, ensure_ascii=False)}\n"
                f"goal: {(body.goal or '').strip()[:4000]}\n\n"
                f"column_marginals_csv:\n{csv_part}\n\n"
                f"sample_rows_json:\n{sample_json}\n"
            )
            note_parts.append("column_marginals_csv was truncated to fit the model input size limit.")

    try:
        client = AsyncOpenAI(api_key=api_key)
        completion = await client.chat.completions.create(
            model=model,
            temperature=0.35,
            max_tokens=SYNTH_AI_MAX_OUTPUT_TOKENS,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenAI request failed: {e!s}") from e

    content = completion.choices[0].message.content
    if not content:
        raise HTTPException(status_code=502, detail="Empty response from model.")

    try:
        data = _parse_json_object(content)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=502, detail=f"Model returned invalid JSON: {e}") from e

    raw_rows = data.get("rows")
    if not isinstance(raw_rows, list):
        raise HTTPException(status_code=502, detail='Model JSON must contain a "rows" array.')

    fixed: list[dict[str, str]] = []
    for i, row in enumerate(raw_rows):
        if i >= n_req:
            break
        fixed.append(_normalize_ai_synth_row(row, hdrs))

    if len(fixed) < n_req:
        note_parts.append(f"Model returned {len(fixed)} of {n_req} rows.")
    if len(fixed) == 0:
        raise HTTPException(status_code=502, detail="Model returned no rows.")

    return SyntheticAiResponse(
        rows=fixed,
        model=model,
        rows_in_response=len(fixed),
        note=" ".join(note_parts).strip(),
    )


@app.get("/")
def index():
    return FileResponse(BASE_DIR / "index.html")


app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


if __name__ == "__main__":
    import threading
    import time
    import webbrowser

    import uvicorn

    # Bind loopback only; avoids some Windows "socket forbidden" cases vs 0.0.0.0.
    # Override: SERVER_PORT=12700  or  UVICORN_RELOAD=0  (reload can be finicky on Windows)
    _port = int(os.getenv("SERVER_PORT", "8765"))
    _reload = os.getenv("UVICORN_RELOAD", "1").strip() not in ("0", "false", "no")
    _url = f"http://127.0.0.1:{_port}"

    if os.getenv("OPEN_BROWSER", "1").strip() not in ("0", "false", "no"):

        def _open_browser() -> None:
            time.sleep(1.0)
            webbrowser.open(_url)

        threading.Thread(target=_open_browser, daemon=True).start()

    uvicorn.run("server:app", host="127.0.0.1", port=_port, reload=_reload)
