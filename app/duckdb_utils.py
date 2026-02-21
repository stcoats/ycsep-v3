import duckdb
import os
import urllib.request


def get_connection():
    # Segments DB (served from Allas)
    db_url = "https://a3s.fi/swift/v1/YCSEP_v2/ycsep_v4_segments.duckdb"
    db_path = "/tmp/ycsep_v5_segments.duckdb"  # make this match the URL

    # Writable directory for DuckDB extensions in Rahti
    ext_path = "/tmp/duckdb_extensions"
    os.makedirs(ext_path, exist_ok=True)

    # Download the DB if it's not already in /tmp
    if not os.path.exists(db_path):
        print(f"Downloading DB from {db_url}...")
        urllib.request.urlretrieve(db_url, db_path)

    con = duckdb.connect(db_path, read_only=True)

    # Rahti-safe settings (avoid permission + memory issues)
    con.execute(f"SET extension_directory='{ext_path}';")
    con.execute("SET memory_limit='512MB';")
    con.execute("SET threads=1;")

    # Make FTS available in this connection if possible.
    # INSTALL may fail in locked-down environments; LOAD may still succeed.
    try:
        con.execute("INSTALL fts;")
    except Exception:
        pass

    try:
        con.execute("LOAD fts;")
    except Exception:
        pass

    return con

