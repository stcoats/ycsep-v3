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

    # 1. 'all_groups' pre-calculates the grp for every row
    # 2. 'target_groups' identifies which groups contain the searched text
    # 3. Final join pulls all words belonging to those groups
    query = """
        WITH all_groups AS (
            SELECT *, 
                   (id - row_number() OVER (PARTITION BY video_id ORDER BY id)) as grp
            FROM data
        ),
        target_groups AS (
            SELECT DISTINCT video_id, grp
            FROM all_groups
            WHERE id IN (SELECT id FROM data WHERE fts_main_data.match_bm25(id, ?) IS NOT NULL)
        )
        SELECT 
            min(a.id) as id,
            listagg(a.text, ' ' ORDER BY a.id) as text,
            listagg(COALESCE(a.pos_tag, 'UNK'), ' ' ORDER BY a.id) as pos_tags,
            a.channel,
            a.speaker,
            a.video_id,
            a.file,
            min(a.start_time) as start_time,
            max(a.end_time) as end_time
        FROM all_groups a
        JOIN target_groups t ON a.video_id = t.video_id AND a.grp = t.grp
        GROUP BY a.channel, a.speaker, a.video_id, a.file, a.grp
        ORDER BY start_time ASC
        LIMIT 100
    """
    try:
        df = con.execute(query, [text]).df()
        if df.empty:
            return {"total": 0, "data": []}
            
        audio_base = "https://a3s.fi/swift/v1/YCSEP_v2/"
        df['audio_url'] = df.apply(
            lambda r: f"{audio_base}{r['file']}#t={float(r['start_time']):.2f}", 
            axis=1
        )
        
        return {"total": len(df), "data": df.to_dict(orient="records")}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/audio/{id}")
def get_audio(id: str):
    raise HTTPException(status_code=400, detail="Use the audio_url provided in the data response.")