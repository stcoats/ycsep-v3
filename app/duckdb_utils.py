import duckdb
import os
import urllib.request
import tempfile

#updated to merged segments, ~15 sec. per segment#
def get_connection():
    db_url = os.getenv(
        "YCSEP_DB_URL",
        "https://a3s.fi/swift/v1/YCSEP_v2/ycsep_v5_segments_merged.duckdb",
    )
    db_path = os.getenv("YCSEP_DB_PATH", "/tmp/ycsep_v5_segments_merged.duckdb")

    ext_path = "/tmp/duckdb_extensions"
    os.makedirs(ext_path, exist_ok=True)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    if not os.path.exists(db_path):
        print(f"Downloading DB from {db_url} ...")
        fd, tmp_path = tempfile.mkstemp(suffix=".duckdb", dir=os.path.dirname(db_path))
        os.close(fd)
        try:
            urllib.request.urlretrieve(db_url, tmp_path)
            os.replace(tmp_path, db_path)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    con = duckdb.connect(db_path, read_only=True)

    con.execute(f"SET extension_directory='{ext_path}';")
    con.execute("SET memory_limit='512MB';")
    con.execute("SET threads=1;")

    try:
        con.execute("INSTALL fts;")
    except Exception:
        pass

    try:
        con.execute("LOAD fts;")
    except Exception:
        pass

    return con

