from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.duckdb_utils import get_connection
import io
import re
import json

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


# Global connection
con = get_connection()


def _parse_channels(channels: str) -> list[str]:
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
                out = []
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



def _token_patterns(q: str) -> list[str]:
    """
    Build whole-token, case-insensitive regex patterns for each token in the query.
    We split on whitespace.
    """
    q = (q or "").strip()
    if not q:
        return []

    tokens = [t for t in re.split(r"\s+", q) if t]
    patterns = []
    for t in tokens:
        # Whole-token match: letters/digits define tokens.
        # This matches 'whatever' in "whatever," and avoids matching inside "malaysia".
        patterns.append(r"(?i)(^|[^a-z0-9])" + re.escape(t) + r"([^a-z0-9]|$)")
    return patterns


def _build_where_and_params(text: str, channels: str):
    """
    Correctness-first search:
      - whole-token regex matching on text OR pos_tags
      - supports multi-word queries by requiring ALL tokens to match (AND)
    Channel filter is robust to junk input.
    """
    selected_channels = _parse_channels(channels)

    where_clauses = ["1=1"]
    params: list[str] = []

    q = (text or "").strip()
    if q:
        patterns = _token_patterns(q)

        # Require all tokens to appear somewhere in text/pos_tags
        # (If you want OR semantics for multi-word queries, change AND -> OR below.)
        token_clauses = []
        for pat in patterns:
            token_clauses.append("(regexp_matches(text, ?) OR regexp_matches(pos_tags, ?))")
            params.extend([pat, pat])

        if token_clauses:
            where_clauses.append("(" + " AND ".join(token_clauses) + ")")

    if selected_channels:
        where_clauses.append("channel IN (" + ",".join(["?"] * len(selected_channels)) + ")")
        params.extend(selected_channels)

    where_sql = " WHERE " + " AND ".join(where_clauses)
    return where_sql, params

from typing import Optional, Dict, List, Tuple

ALLOWED_FILTER_COLS = {
    "id": "segment_id",
    "channel": "channel",
    "video_id": "video_id",
    "speaker": "speaker",
    "start_time": "start_time",
    "end_time": "end_time",
    # You *can* allow text/pos_tags too, but those are usually handled by the global text box.
    # "text": "text",
    # "pos_tags": "pos_tags",
}

def _parse_csv_list(s: Optional[str]) -> List[str]:
    if s is None:
        return []
    s = str(s).strip()
    if not s:
        return []
    low = s.lower()
    if low in {"[]", "null", "none", "undefined"}:
        return []
    # allow JSON list too
    if s.startswith("[") and s.endswith("]"):
        try:
            v = json.loads(s)
            if isinstance(v, list):
                return [str(x).strip() for x in v if x is not None and str(x).strip()]
        except Exception:
            pass
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p and p.lower() not in {"null", "none", "undefined"}]

def _parse_filters(filters_json: Optional[str]) -> Dict[str, List[str]]:
    """
    Expects URL-encoded JSON like:
      {"id":["JR0fi48vngx"], "speaker":["SPEAKER01","SPEAKER02"]}
    Values can also be a comma string.
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

def _build_where_and_params_v2(text: str, channels: str, filters_json: str):
    """
    Global text + channels + per-column "selected values" filters.
    """
    selected_channels = _parse_channels(channels)
    filters = _parse_filters(filters_json)

    where_clauses = ["1=1"]
    params: list[str] = []

    q = (text or "").strip()
    if q:
        patterns = _token_patterns(q)
        token_clauses = []
        for pat in patterns:
            token_clauses.append("(regexp_matches(text, ?) OR regexp_matches(pos_tags, ?))")
            params.extend([pat, pat])
        if token_clauses:
            where_clauses.append("(" + " AND ".join(token_clauses) + ")")

    if selected_channels:
        where_clauses.append("channel IN (" + ",".join(["?"] * len(selected_channels)) + ")")
        params.extend(selected_channels)

    # Column filters are exact-match IN filters (persistent across sort/search)
    for public_col, values in filters.items():
        db_col = ALLOWED_FILTER_COLS[public_col]
        where_clauses.append(f"{db_col} IN (" + ",".join(["?"] * len(values)) + ")")
        params.extend(values)

    where_sql = " WHERE " + " AND ".join(where_clauses)
    return where_sql, params


@app.get("/channels")
def get_channels():
    try:
        rows = con.execute(
            "SELECT DISTINCT channel FROM segments WHERE channel IS NOT NULL ORDER BY channel"
        ).fetchall()
        return [r[0] for r in rows]
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

        where_sql, params = _build_where_and_params(text, channels)

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
):
    """
    Download CSV for the current page (same filters as /data).
    """
    try:
        offset = (page - 1) * size
        where_sql, params = _build_where_and_params(text, channels)

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
    return JSONResponse(status_code=400, content={"detail": "Use audio_url from data response"})

