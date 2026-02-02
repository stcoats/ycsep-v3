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
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/data")
def get_paginated_data(text: str = Query("")):
    if not text:
        return {"total": 0, "data": []}

    # Added COALESCE to pos_tag to handle any unmapped words safely
    query = """
        SELECT 
            min(id) as id,
            listagg(text, ' ' ORDER BY id) as text,
            listagg(COALESCE(pos_tag, 'UNK'), ' ' ORDER BY id) as pos_tags,
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
        if con is None:
            return JSONResponse(status_code=500, content={"error": "Database connection is None"})

        df = con.execute(query, [text]).df()
        
        if df.empty:
            return {"total": 0, "data": []}
            
        df['audio_url'] = df.apply(lambda r: f"{ALLAS_AUDIO_BASE}{r['video_id']}#t={r['start_time']:.2f}", axis=1)
        
        results = df.to_dict(orient="records")
        return {"total": len(results), "data": results}
        
    except Exception as e:
        return JSONResponse(
            status_code=500, 
            content={
                "error": str(e),
                "query_attempted": query
            }
        )

@app.get("/audio/{id}")
def get_audio(id: str):
    raise HTTPException(status_code=400, detail="Use the audio_url provided in the data response.")