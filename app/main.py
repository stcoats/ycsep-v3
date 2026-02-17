from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.duckdb_utils import get_connection
import io
import re

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

# Try to make FTS available on THIS connection. If it doesn't work, we fall back safely.
try:
    con.execute("INSTALL fts;")
except Exception:
    pass
try:
    con.execute("LOAD fts;")
except Exception:
    pass


# Cache whether FTS is usable; computing this once avoids doing a probe on every request.
_FTS_USABLE = None


def _fts_is_usable() -> bool:
    """
    Returns True if the fts_main_segments macros created by create_fts_index(...)
    exist AND can be called in this connection.
    """
    global _FTS_USABLE
    if _FTS_USABLE is not None:
        return _FTS_USABLE

    try:
        # Probe: if the macro exists and can be executed, this succeeds.
        con.execute(
            "SELECT fts_main_segments.match_bm25(segment_id, ?, fields:='text') "
            "FROM segments LIMIT 1;",
            ["probe"],
        ).fetchone()
        _FTS_USABLE = True
    except Exception:
        _FTS_USABLE = False

    return _FTS_USABLE


def _build_where_and_params_fts(text: str, channels: str):
    """
    FTS-based WHERE + params.
    Note: some common words can behave like stopwords depending on how the index was built,
    causing 0 hits even when the word exists. We handle that by falling back to regex if needed.
    """
    selected_channels = [c for c in channels.split(",") if c]
    where_clauses = ["1=1"]
    params = []

    q = (text or "").strip()
    if q:
        where_clauses.append(
            "fts_main_segments.match_bm25(segment_id, ?, fields:='text') IS NOT NULL"
        )
        params.append(q)

    if selected_channels:
        where_clauses.append("channel IN (" + ",".join(["?"] * len(selected_channels)) + ")")
        params.extend(selected_channels)

    where_sql = " WHERE " + " AND ".join(where_clauses)
    return where_sql, params


def _build_where_and_params_regex(text: str, channels: str):
    """
    Regex-based whole-token WHERE + params.
    This is the correctness fallback for cases where FTS returns 0 for stopword-like queries.
    """
    selected_channels = [c for c in channels.split(",") if c]
    where_clauses = ["1=1"]
    params = []

    q = (text or "").strip()
    if q:
        # Whole-token, case-insensitive:
        # matches "sia" as a token, not inside "malaysia"
        pattern = r"(?i)(^|[^a-z0-9])" + re.escape(q) + r"([^a-z0-9]|$)"
        where_clauses.append("(regexp_matches(text, ?) OR regexp_matches(pos_tags, ?))")
        params.extend([pattern, pattern])

    if selected_channels:
        where_clauses.append("channel IN (" + ",".join(["?"] * len(selected_channels)) + ")")
        params.extend(selected_channels)

    where_sql = " WHERE " + " AND ".join(where_clauses)
    return where_sql, params


def _count_total(where_sql: str, params: list) -> int:
    return int(con.execute(f"SELECT count(*) FROM segments{where_sql};", params).fetchone()[0])


def _run_paged_query(where_sql: str, params: list, sort_col: str, dir_sql: str, size: int, offset: int):
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
    df = con.execute(data_sql, params + [size, offset]).df()
    return df


def _run_paged_query_with_fallback(text: str, channels: str, sort_col: str, dir_sql: str, size: int, offset: int):
    """
    Strategy:
    - If FTS is usable and there's a query, try FTS first.
    - If FTS returns 0 total, immediately retry with regex whole-token matching.
      (This fixes "already" / other stopword-like cases without requiring DB rebuild.)
    - If no query, just filter by channels (no need for fallback).
    """
    q = (text or "").strip()

    if q and _fts_is_usable():
        where_sql, params = _build_where_and_params_fts(q, channels)
        total = _count_total(where_sql, params)
        if total > 0:
            df = _run_paged_query(where_sql, params, sort_col, dir_sql, size, offset)
            return total, df

        # FTS produced 0 hits: fallback to regex whole-token matching
        where_sql, params = _build_where_and_params_regex(q, channels)
        total = _count_total(where_sql, params)
        df = _run_paged_query(where_sql, params, sort_col, dir_sql, size, offset)
        return total, df

    # No query or FTS not usable: regex builder (which also handles empty q fine)
    where_sql, params = _build_where_and_params_regex(q, channels)
    total = _count_total(where_sql, params)
    df = _run_paged_query(where_sql, params, sort_col, dir_sql, size, offset)
    return total, df


def _run_csv_query_with_fallback(text: str, channels: str, size: int, offset: int):
    q = (text or "").strip()

    def run(where_sql, params):
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
        return con.execute(query, params + [size, offset]).df()

    if q and _fts_is_usable():
        where_sql, params = _build_where_and_params_fts(q, channels)
        total = _count_total(where_sql, params)
        if total > 0:
            return run(where_sql, params)

        where_sql, params = _build_where_and_params_regex(q, channels)
        return run(where_sql, params)

    where_sql, params = _build_where_and_params_regex(q, channels)
    return run(where_sql, params)


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

        total, df = _run_paged_query_with_fallback(
            text=text,
            channels=channels,
            sort_col=sort_col,
            dir_sql=dir_sql,
            size=size,
            offset=offset,
        )

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

        df = _run_csv_query_with_fallback(
            text=text,
            channels=channels,
            size=size,
            offset=offset,
        )

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
