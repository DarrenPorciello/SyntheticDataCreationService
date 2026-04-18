"""
Local API + static UI. Loads OPENAI_API_KEY from .env (never sent to the browser).
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

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


app = FastAPI(title="Southlake Synthetic Data Studio API")

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


def _normalize_issues(raw: list) -> list[IssueOut]:
    out: list[IssueOut] = []
    if not isinstance(raw, list):
        return out
    for item in raw[:20]:
        if not isinstance(item, dict):
            continue
        sev = str(item.get("sev", "low")).lower()
        if sev not in ("high", "medium", "low"):
            sev = "low"
        title = str(item.get("title", "")).strip() or "Finding"
        detail = str(item.get("detail", "")).strip() or "—"
        out.append(IssueOut(sev=sev, title=title[:200], detail=detail[:1200]))
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
2. Produce a prioritized list of hygiene / quality issues. You may rephrase, merge, or refine deterministic_findings, add important issues the code missed, and drop clear false positives. Use severity: high, medium, or low.

Respond with ONLY valid JSON matching this shape (no markdown fences):
{"summary": string, "issues": [{"sev": "high"|"medium"|"low", "title": string, "detail": string}]}

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
            )
        ]

    return QualityAnalyzeResponse(summary=summary[:4000], issues=issues)


def _normalize_metadata_suggestions(raw: list) -> list[MetadataSuggestionItemOut]:
    out: list[MetadataSuggestionItemOut] = []
    if not isinstance(raw, list):
        return out
    for item in raw[:4]:
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

    system = """You are the **AI Metadata Agent** for Synthetic Data Studio. You speak directly to customers and analysts who are shaping metadata so the system can generate **high-quality synthetic data** that reflects their real dataset.

You receive (as JSON): dataset shape, per-column profile stats, a small row sample, hygiene notes from an earlier quality pass, and **schema_context** (columns excluded from synthesis, user edits to labels/types/notes, synthetic distribution hints, and correlation targets).

Respond with ONLY valid JSON (no markdown, no code fences):
{"summary": string, "suggestions": array}

**summary** — One or two short sentences in plain, confident language: what stands out about their metadata and how ready it looks for synthetic generation. No jargon about systems or infrastructure.

**suggestions** — Between **0 and 4** objects. Only include changes that would **meaningfully improve synthetic output** given this data (e.g. include/exclude decisions, clearer labels or type intent, synthesis notes, realistic numeric/category targets, correlation targets, handling identifiers or skewed fields). Skip low-impact or speculative tips. If metadata already fits the data well, return **[]**.

Each suggestion object:
{"title": string, "detail": string, "importance": "high"|"medium"|"low", "related_columns": string[], "suggested_action": string}

- **title**: Benefit-focused, ≤8 words.
- **detail**: 1–2 short sentences, friendly and specific.
- **importance**: high, medium, or low priority for impact on synthetic quality.
- **related_columns**: Up to 3 names; every name MUST appear in the provided headers/column_stats. Use [] if none.
- **suggested_action**: One short imperative line the user can follow in the metadata UI (optional; use "" if redundant).

Rules: Never invent column names. Avoid repeating the same idea twice. Do not mention APIs, keys, models, servers, or "OpenAI". Write as the in-product agent."""

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
    suggestions = _normalize_metadata_suggestions(data.get("suggestions", []))

    return MetadataSuggestResponse(summary=summary[:1200], suggestions=suggestions)


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
