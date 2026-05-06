import os
import glob
import snowflake.connector

conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "mariogalvisg"
)
cur = conn.cursor()

STAGE = "@MGG_PAGOS.PUBLIC.MGG_PAGOS_APP_STAGE"
LOCAL = "/Users/mgalvis/mgg_pagos_app"

cur.execute("USE DATABASE MGG_PAGOS")
cur.execute("USE SCHEMA PUBLIC")

files_uploaded = 0

for name, subdir in [
    ("streamlit_app.py", ""),
    ("use_case_layout.py", ""),
    ("pyproject.toml", ""),
    (".streamlit/config.toml", ".streamlit"),
]:
    local_path = os.path.join(LOCAL, name)
    stage_path = f"'{STAGE}/{subdir}/'" if subdir else f"'{STAGE}/'"
    cur.execute(f"PUT 'file://{local_path}' {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    files_uploaded += 1
    print(f"  {name}")

for f in sorted(glob.glob(f"{LOCAL}/app_pages/*.py")):
    fname = os.path.basename(f)
    cur.execute(f"PUT 'file://{f}' '{STAGE}/app_pages/' AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    files_uploaded += 1
    print(f"  app_pages/{fname}")

for f in sorted(glob.glob(f"{LOCAL}/pages/*.py")):
    fname = os.path.basename(f)
    cur.execute(f"PUT 'file://{f}' '{STAGE}/pages/' AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    files_uploaded += 1
    print(f"  pages/{fname}")

for f in sorted(glob.glob(f"{LOCAL}/static/*")):
    fname = os.path.basename(f)
    cur.execute(f"PUT 'file://{f}' '{STAGE}/static/' AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    files_uploaded += 1
    print(f"  static/{fname}")

print(f"\nUploaded {files_uploaded} files to {STAGE}")

cur.execute(f"ALTER STREAMLIT MGG_PAGOS.PUBLIC.MGG_PAGOS SET ROOT_LOCATION = '{STAGE}'")
print("Updated ROOT_LOCATION")

cur.close()
conn.close()
print("Deploy complete!")
