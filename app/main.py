from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
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


# Global connection
con = get_connection()


@app.get("/channels")
def get_channels():
    """
    Return distinct channels for checkbox filters.
    Uses the segments DB (table: segments).
    """
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
    """
    Paginated segment-level results.

    Expected table schema (your ycsep_v3_segments.duckdb):
      segments(segment_id, channel, speaker, start_time, end_time, text, pos_tags, video_id, audio_url, ...)

    Returns JSON:
      {"total": <int>, "data": [ {id, channel, video_id, speaker, start_time, end_time, text, pos_tags, audio_url}, ... ]}
    """
    try:
        offset = (page - 1) * size
        selected_channels = [c for c in channels.split(",") if c]

        # Prevent ORDER BY injection
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

        where_clauses = ["1=1"]
        params = []

        # Search: prefer FTS if available; fallback to LIKE if not.
        use_fts = False
        if text.strip():
            try:
                # If the FTS index exists, this view will exist
                con.execute("SELECT 1 FROM fts_main_segments LIMIT 1;").fetchone()
                use_fts = True
            except Exception:
                use_fts = False

            if use_fts:
                where_clauses.append("fts_main_segments.match_bm25(segment_id, ?) IS NOT NULL")
                params.append(text)
            else:
                # Fallback: substring match on text and POS tags
                where_clauses.append(
                    "(lower(text) LIKE '%' || lower(?) || '%' OR lower(pos_tags) LIKE '%' || lower(?) || '%')"
                )
                params.extend([text, text])

        if selected_channels:
            where_clauses.append("channel IN (" + ",".join(["?"] * len(selected_channels)) + ")")
            params.extend(selected_channels)

        where_sql = " WHERE " + " AND ".join(where_clauses)

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


@app.get("/audio/{id}")
def get_audio(id: str):
    # Legacy endpoint: the frontend should use audio_url from /data.
    return JSONResponse(status_code=400, content={"detail": "Use audio_url from data response"})
