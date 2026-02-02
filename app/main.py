from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.duckdb_utils import get_connection
import io

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


@app.get("/channels")
def get_channels():
    try:
        rows = con.execute(
            "SELECT DISTINCT channel FROM segments WHERE channel IS NOT NULL ORDER BY channel"
        ).fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


def _build_where_and_params(text: str, channels: str):
    """
    Build WHERE clause + params for both /data and /download/csv.
    Uses FTS if available; falls back to LIKE.
    """
    selected_channels = [c for c in channels.split(",") if c]
    where_clauses = ["1=1"]
    params = []

    use_fts = False
    if text.strip():
        try:
            con.execute("SELECT 1 FROM fts_main_segments LIMIT 1;").fetchone()
            use_fts = True
        except Exception:
            use_fts = False

        if use_fts:
            where_clauses.append("fts_main_segments.match_bm25(segment_id, ?) IS NOT NULL")
            params.append(text)
        else:
            where_clauses.append(
                "(lower(text) LIKE '%' || lower(?) || '%' OR lower(pos_tags) LIKE '%' || lower(?) || '%')"
            )
            params.extend([text, text])

    if selected_channels:
        where_clauses.append("channel IN (" + ",".join(["?"] * len(selected_channels)) + ")")
        params.extend(selected_channels)

    where_sql = " WHERE " + " AND ".join(where_clauses)
    return where_sql, params


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
