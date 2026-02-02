import duckdb
import os
import urllib.request

def get_connection():
    db_path = "/tmp/v2_data_final.db"
    db_url = "https://a3s.fi/swift/v1/YCSEP_v2/v2_data_final.db"
    ext_path = "/tmp/duckdb_extensions"

    # Create the writeable directory for extensions
    if not os.path.exists(ext_path):
        os.makedirs(ext_path)

    # Download the DB if it's not already in /tmp
    if not os.path.exists(db_path):
        print(f"Downloading DB from {db_url}...")
        urllib.request.urlretrieve(db_url, db_path)

    con = duckdb.connect(db_path, read_only=True)
    
    # CRITICAL: These settings stop the 'Out of Memory' and 'Permission Denied' errors
    con.execute(f"SET extension_directory='{ext_path}';")
    con.execute("SET memory_limit='512MB';")
    con.execute("SET threads=1;")
    
    return con