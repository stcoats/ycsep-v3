from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.duckdb_utils import get_connection
import os

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

# Initialize connection and download DB if needed
con = get_connection()
ALLAS_AUDIO_BASE = "https://a3s.fi/swift/v1/YCSEP_v2/"

@app.get("/channels")
def get_channels():
    try:
        rows = con.execute("SELECT DISTINCT channel FROM data WHERE channel IS NOT NULL ORDER BY channel").fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get channels: {e}")

@app.get("/data")
def get_paginated_data(text: str = Query("")):
    if not text:
        return {"total": 0, "data": []}

    # Groups individual word rows into phrases and aggregates the POS tags
    query = """
        SELECT 
            min(id) as id,
            listagg(text, ' ') WITHIN GROUP (ORDER BY id) as text,
            listagg(pos_tag, ' ') WITHIN GROUP (ORDER BY id) as pos_tags,
            channel,
            speaker,
            file as video_id,
            min(start_time) as start_time,
            max(end_time) as end_time
        FROM (
            SELECT *, (id - row_number() OVER (ORDER BY id)) as grp
            FROM data
            WHERE id IN (SELECT id FROM data WHERE fts_main_data.match_bm25(id, ?) IS NOT NULL)
        )
        GROUP BY channel, speaker, video_id, grp
        ORDER BY start_time ASC
        LIMIT 100
    """
    try:
        df = con.execute(query, [text]).df()
        
        # Format the URL to point directly to the audio file on Allas with timestamp
        df['audio_url'] = df.apply(lambda r: f"{ALLAS_AUDIO_BASE}{r['video_id']}#t={r['start_time']:.2f}", axis=1)
        
        results = df.to_dict(orient="records")
        return {"total": len(results), "data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {e}")

@app.get("/audio/{id}")
def get_audio(id: str):
    raise HTTPException(status_code=400, detail="Use the audio_url provided in the data response.")