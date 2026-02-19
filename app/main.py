# main.py
from __future__ import annotations

import io
import json
import re
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.duckdb_utils import get_connection

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/")
def serve_index():
    return FileResponse("app/static/index.html")


# Global connection (correctness-first; reuse one connection)
con = get_connection()


# ----------------------------
# Robust parsing helpers
# ----------------------------
def _parse_channels(channels: Optional[str]) -> List[str]:
    """
    The frontend may send channels in a few annoying forms:
      - "" (empty)
      - "a,b,c"
      - "[]" or '["a","b"]' (JSON)
      - "null" / "undefined" / "None"
    We treat all non-sensical / empty forms as "no channel filter".
    """
    if channels is None:
        return []

    s = str(channels).strip()
    if not s:
        return []

    low = s.lower()
    if low in {"[]", "null", "none", "undefined"}:
        return []

    # If it looks like JSON, try to parse it
    if (s.startswith("[") and s.endswith("]")) or (s.startswith('"[') and s.endswith(']"')):
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                out: List[str] = []
                for x in parsed:
                    if x is None:
                        continue
                    xs = str(x).strip()
                    if xs and xs.lower() not in {"null", "none", "undefined"}:
                        out.append(xs)
                return out
        except Exception:
            # fall through to comma-split
            pass

    # Comma-separated
    parts = [p.strip() for p in s.split(",")]
    parts = [p for p in parts if p and p.lower() not in {"null", "none", "undefined"}]
    return parts

def _extract_phrases_and_terms(q: str) -> tuple[list[str], list[str]]:
    """
    Split query into:
      - phrases inside double quotes:  "can can"
      - remaining unquoted terms: can, can, etc.

    Returns (phrases, terms).
    """
    q = (q or "").strip()
    if not q:
        return [], []

    # Extract "..." phrases (no escaping support; simple and robust)
    phrases = re.findall(r'"([^"]+)"', q)

    # Remove quoted parts from the query, leaving unquoted terms
    q_wo_phrases = re.sub(r'"[^"]+"', " ", q)
    terms = [t for t in re.split(r"\s+", q_wo_phrases.strip()) if t]

    return [p.strip() for p in phrases if p.strip()], terms


def _whole_token_pattern(token: str) -> str:
    token = token.strip()
    return r"(?i)(^|[^a-z0-9])" + re.escape(token) + r"([^a-z0-9]|$)"


def _phrase_pattern(phrase: str) -> str:
    """
    Build a case-insensitive regex that matches a sequence of whole tokens
    in order, allowing non-alphanumeric separators between them.

    Example: "can can" matches "can can", "can, can", "can  can", etc.
    """
    toks = [t for t in re.split(r"\s+", phrase.strip()) if t]
    if not toks:
        return ""

    # Start boundary + tok1 + separators + tok2 + ... + end boundary
    pat = r"(?i)(^|[^a-z0-9])" + re.escape(toks[0])
    for t in toks[1:]:
        pat += r"[^a-z0-9]+" + re.escape(t)
    pat += r"([^a-z0-9]|$)"
    return pat


def _parse_csv_list(s: Optional[str]) -> List[str]:
    """
    Parse a list-ish value:
      - "" -> []
      - "a,b" -> ["a","b"]
      - '["a","b"]' -> ["a","b"]
      - null/none/undefined -> []
    """
    if s is None:
        return []
    s2 = str(s).strip()
    if not s2:
        return []
    low = s2.lower()
    if low in {"[]", "null", "none", "undefined"}:
        return []
    if s2.startswith("[") and s2.endswith("]"):
        try:
            v = json.loads(s2)
            if isinstance(v, list):
                return [str(x).strip() for x in v if x is not None and str(x).strip()]
        except Exception:
            pass
    parts = [p.strip() for p in s2.split(",")]
    return [p for p in parts if p and p.lower() not in {"null", "none", "undefined"}]


# ----------------------------
# Search helpers (global text box)
# ----------------------------
def _token_patterns(q: str) -> List[str]:
    """
    Build whole-token, case-insensitive regex patterns for each token in the query.
    We split on whitespace.

    Example token 'can' matches:
      - "can"
      - "can," etc.
    and avoids matching inside longer strings like "candy".
    """
    q = (q or "").strip()
    if not q:
        return []

    tokens = [t for t in re.split(r"\s+", q) if t]
    patterns: List[str] = []
    for t in tokens:
        patterns.append(r"(?i)(^|[^a-z0-9])" + re.escape(t) + r"([^a-z0-9]|$)")
    return patterns


# ----------------------------
# Column filters (header filters)
# ----------------------------
# Public column keys expected from the frontend -> actual DB columns
ALLOWED_FILTER_COLS: Dict[str, str] = {
    "id": "segment_id",
    "channel": "channel",
    "video_id": "video_id",
    "speaker": "speaker",
    "start_time": "start_time",
    "end_time": "end_time",
    # Usually handled by the global search box; enable if you want:
    # "text": "text",
    # "pos_tags": "pos_tags",
}


def _parse_filters(filters_json: Optional[str]) -> Dict[str, List[str]]:
    """
    Expects URL-encoded JSON like:
      {"id":["JR0fi48vngx"], "speaker":["SPEAKER01","SPEAKER02"]}
    Values may also be comma-separated strings.
    Unknown columns are ignored.
    """
    if not filters_json:
        return {}

    s = str(filters_json).strip()
    if not s or s.lower() in {"null", "none", "undefined", "{}"}:
        return {}

    try:
        obj = json.loads(s)
        if not isinstance(obj, dict):
            return {}

        out: Dict[str, List[str]] = {}
        for k, v in obj.items():
            if k not in ALLOWED_FILTER_COLS:
                continue

            if v is None:
                continue

            if isinstance(v, list):
                vals = [str(x).strip() for x in v if x is not None and str(x).strip()]
            else:
                vals = _parse_csv_list(str(v))

            if vals:
                out[k] = vals

        return out
    except Exception:
        return {}


def _build_where_and_params(text: str, channels: str, filters_json: str) -> Tuple[str, List[str]]:
    """
    Correctness-first filtering:
      - global whole-token regex matching on text OR pos_tags
      - multi-word queries require ALL tokens to match (AND)
      - channel filter robust to junk input
      - per-column filters are exact-match IN filters, persistent across sort/search

    Returns:
      where_sql like " WHERE 1=1 AND ... "
      params list for DuckDB parameter binding
    """
    selected_channels = _parse_channels(channels)
    filters = _parse_filters(filters_json)

    where_clauses = ["1=1"]
    params: List[str] = []

    q = (text or "").strip()
    if q:
        phrases, terms = _extract_phrases_and_terms(q)
    
        # 1) Phrase constraints (ALL phrases must match somewhere)
        phrase_clauses = []
        for ph in phrases:
            pat = _phrase_pattern(ph)
            if pat:
                phrase_clauses.append("(regexp_matches(text, ?) OR regexp_matches(pos_tags, ?))")
                params.extend([pat, pat])
    
        # 2) Term constraints (ALL UNIQUE tokens must match somewhere)
        # Dedup fixes: "can can" (unquoted) won't degrade to "can" via A AND A,
        # it will explicitly become just one constraint on "can".
        seen = set()
        term_clauses = []
        for t in terms:
            tlow = t.lower()
            if tlow in seen:
                continue
            seen.add(tlow)
    
            pat = _whole_token_pattern(t)
            term_clauses.append("(regexp_matches(text, ?) OR regexp_matches(pos_tags, ?))")
            params.extend([pat, pat])
    
        # Combine: require ALL phrase matches AND ALL term matches
        all_clauses = phrase_clauses + term_clauses
        if all_clauses:
            where_clauses.append("(" + " AND ".join(all_clauses) + ")")


    if selected_channels:
        where_clauses.append("channel IN (" + ",".join(["?"] * len(selected_channels)) + ")")
        params.extend(selected_channels)

    # Exact-match IN filters for header-selected values
    for public_col, values in filters.items():
        db_col = ALLOWED_FILTER_COLS[public_col]
        where_clauses.append(f"{db_col} IN (" + ",".join(["?"] * len(values)) + ")")
        params.extend(values)

    where_sql = " WHERE " + " AND ".join(where_clauses)
    return where_sql, params


# ----------------------------
# Routes
# ----------------------------
@app.get("/channels")
def get_channels():
    try:
        rows = con.execute(
            "SELECT DISTINCT channel FROM segments WHERE channel IS NOT NULL ORDER BY channel"
        ).fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/suggest")
def suggest_values(
    col: str = Query(..., description="Public column name, e.g. 'id'"),
    prefix: str = Query("", description="Prefix typed by user, e.g. 'JR'"),
    text: str = Query(""),
    channels: str = Query(""),
    filters: str = Query("", description="JSON dict of active column filters"),
    limit: int = Query(20, ge=1, le=100),
):
    """
    Autocomplete values for a column, under current global filters.
    """
    try:
        if col not in ALLOWED_FILTER_COLS:
            return JSONResponse(status_code=400, content={"error": "Invalid column"})

        p = (prefix or "").strip()
        if not p:
            return []  # keep it cheap; no prefix => no suggestions

        db_col = ALLOWED_FILTER_COLS[col]

        where_sql, params = _build_where_and_params(text, channels, filters)

        # Prefix match: LIKE 'prefix%'
        # NOTE: If you want case-insensitive, use ILIKE (DuckDB supports ILIKE).
        sql = f"""
            SELECT DISTINCT {db_col} AS v
            FROM segments
            {where_sql}
              AND {db_col} LIKE ?
            ORDER BY v
            LIMIT ?;
        """
        rows = con.execute(sql, params + [p + "%", limit]).fetchall()
        return [r[0] for r in rows if r and r[0] is not None]

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/data")
def get_paginated_data(
    text: str = Query(""),
    page: int = Query(1, ge=1),
    size: int = Query(100, ge=1, le=500),
    sort: str = Query("id"),
    direction: str = Query("asc"),
    channels: str = Query(""),
    filters: str = Query("", description="JSON dict of active column filters"),
):
    try:
        offset = (page - 1) * size

        sort_map = {
            "id": "segment_id",
            "channel": "channel",
            "video_id": "video_id",
            "speaker": "speaker",
            "start_time": "start_time",
            "end_time": "end_time",
            "text": "text",
            "pos_tags": "pos_tags",
        }
        sort_col = sort_map.get(sort, "segment_id")
        dir_sql = "DESC" if direction.lower() == "desc" else "ASC"

        where_sql, params = _build_where_and_params(text, channels, filters)

        count_sql = f"SELECT count(*) FROM segments{where_sql};"
        data_sql = f"""
            SELECT
                segment_id AS id,
                channel,
                video_id,
                speaker,
                start_time,
                end_time,
                text,
                pos_tags,
                audio_url
            FROM segments
            {where_sql}
            ORDER BY {sort_col} {dir_sql}
            LIMIT ? OFFSET ?;
        """

        total = int(con.execute(count_sql, params).fetchone()[0])
        df = con.execute(data_sql, params + [size, offset]).df()

        return {"total": total, "data": df.to_dict(orient="records")}

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/download/csv")
def download_csv(
    text: str = Query(""),
    page: int = Query(1, ge=1),
    size: int = Query(100, ge=1, le=500),
    channels: str = Query(""),
    filters: str = Query("", description="JSON dict of active column filters"),
):
    """
    Download CSV for the current page (same filters as /data).
    """
    try:
        offset = (page - 1) * size
        where_sql, params = _build_where_and_params(text, channels, filters)

        query = f"""
            SELECT
                segment_id AS id,
                channel,
                video_id,
                speaker,
                start_time,
                end_time,
                text,
                pos_tags,
                audio_url
            FROM segments
            {where_sql}
            ORDER BY segment_id ASC
            LIMIT ? OFFSET ?;
        """

        df = con.execute(query, params + [size, offset]).df()

        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)

        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=ycsep_page.csv"},
        )

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/audio/{id}")
def get_audio(id: str):
    # Legacy endpoint: frontend uses audio_url from /data.
    return JSONResponse(
        status_code=400,
        content={"detail": "Use audio_url from data response"},
    )

