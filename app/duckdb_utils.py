import duckdb
import os
import urllib.request

#updated to merged segments, ~15 sec. per segment#
def get_connection():
    db_url = "https://a3s.fi/swift/v1/YCSEP_v2/ycsep_v5_segments_merged.duckdb"
    db_path = "/tmp/ycsep_v5_segments_merged.duckdb"

    ext_path = "/tmp/duckdb_extensions"
    os.makedirs(ext_path, exist_ok=True)

    # ALWAYS redownload
    if os.path.exists(db_path):
        os.remove(db_path)

    print(f"Downloading DB from {db_url} ...")
    urllib.request.urlretrieve(db_url, db_path)

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

