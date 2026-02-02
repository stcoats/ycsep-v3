import duckdb
import os
import urllib.request

def get_connection():
    db_path = "/tmp/v2_data_final.db"
    db_url = "https://a3s.fi/swift/v1/YCSEP_v2/v2_data_final.db"
    ext_path = "/tmp/duckdb_extensions"

    # Create the extension directory if it doesn't exist
    if not os.path.exists(ext_path):
        os.makedirs(ext_path)

    # Download the database
    if not os.path.exists(db_path):
        print(f"Downloading DB from {db_url}...")
        urllib.request.urlretrieve(db_url, db_path)

    # Connect and immediately configure the extension path
    con = duckdb.connect(db_path, read_only=True)
    
    # This tells DuckDB to use /tmp for installing the 'fts' extension
    con.execute(f"SET extension_directory='{ext_path}';")
    con.execute("INSTALL fts; LOAD fts;")
    
    return con