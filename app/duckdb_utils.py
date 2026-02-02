import duckdb
import os
import urllib.request

def get_connection():
    db_path = "/tmp/v2_data_final.db"
    db_url = "https://a3s.fi/swift/v1/YCSEP_v2/v2_data_final.db"
    ext_path = "/tmp/duckdb_extensions"

    if not os.path.exists(ext_path):
        os.makedirs(ext_path)

    if not os.path.exists(db_path):
        urllib.request.urlretrieve(db_url, db_path)

    con = duckdb.connect(db_path, read_only=True)
    # Redirects extension installation to the writable /tmp folder inside Rahti
    con.execute(f"SET extension_directory='{ext_path}';")
    return con