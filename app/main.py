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

    # 1. Finds the group 'grp' for every matching word
    # 2. Selects ALL words that share those groups to rebuild the full phrases
    query = """
        WITH matches AS (
            SELECT DISTINCT video_id, (id - row_number() OVER (PARTITION BY video_id ORDER BY id)) as grp
            FROM data
            WHERE id IN (SELECT id FROM data WHERE fts_main_data.match_bm25(id, ?) IS NOT NULL)
        )
        SELECT 
            min(d.id) as id,
            listagg(d.text, ' ' ORDER BY d.id) as text,
            listagg(COALESCE(d.pos_tag, 'UNK'), ' ' ORDER BY d.id) as pos_tags,
            d.channel,
            d.speaker,
            d.video_id,
            d.file,
            min(d.start_time) as start_time,
            max(d.end_time) as end_time
        FROM data d
        JOIN matches m ON d.video_id = m.video_id 
        AND (d.id - row_number() OVER (PARTITION BY d.video_id ORDER BY d.id)) = m.grp
        GROUP BY d.channel, d.speaker, d.video_id, d.file, m.grp
        ORDER BY start_time ASC
        LIMIT 100
    """
    try:
        df = con.execute(query, [text]).df()
        if df.empty:
            return {"total": 0, "data": []}
            
        # Correctly formatted audio URL using the 'file' column from your schema
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