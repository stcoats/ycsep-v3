from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse
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

# Global connection with Rahti-specific memory limits
con = get_connection()
ALLAS_AUDIO_BASE = "https://a3s.fi/swift/v1/YCSEP_v2/"

@app.get("/channels")
def get_channels():
    try:
        rows = con.execute("SELECT DISTINCT channel FROM data WHERE channel IS NOT NULL ORDER BY channel").fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/data")
def get_paginated_data(text: str = Query("")):
    if not text:
        return {"total": 0, "data": []}

    # Reconstructs phrases by finding all words in the same segment as the match
    query = """
        SELECT 
            min(id) as id,
            listagg(text, ' ' ORDER BY id) as text,
            listagg(COALESCE(pos_tag, 'UNK'), ' ' ORDER BY id) as pos_tags,
            channel,
            speaker,
            video_id,
            file,
            min(start_time) as start_time,
            max(end_time) as end_time
        FROM data
        WHERE (video_id, start_time) IN (
            SELECT DISTINCT video_id, start_time 
            FROM data 
            WHERE id IN (SELECT id FROM data WHERE fts_main_data.match_bm25(id, ?) IS NOT NULL)
        )
        GROUP BY channel, speaker, video_id, file, start_time
        ORDER BY start_time ASC
        LIMIT 100
    """
    try:
        df = con.execute(query, [text]).df()
        
        if df.empty:
            return {"total": 0, "data": []}
            
        # Generates audio URLs using the 'file' column from your V2 schema
        df['audio_url'] = df.apply(
            lambda r: f"{ALLAS_AUDIO_BASE}{r['file']}#t={float(r['start_time']):.2f}", 
            axis=1
        )
        
        return {"total": len(df), "data": df.to_dict(orient="records")}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/audio/{id}")
def get_audio(id: str):
    # Direct links are now used; this endpoint is a fallback/legacy
    return JSONResponse(status_code=400, content={"detail": "Use audio_url from data response"})