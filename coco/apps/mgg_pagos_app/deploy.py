import os
import snowflake.connector
import glob

conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "mariogalvisg"
)
cur = conn.cursor()

cur.execute("USE DATABASE MGG_PAGOS")
cur.execute("USE SCHEMA PUBLIC")

stage = "@MGG_PAGOS.PUBLIC.MGG_PAGOS_APP_STAGE"

base = "/Users/mgalvis/mgg_pagos_app"

files_to_upload = [
    ("streamlit_app.py", ""),
    ("use_case_layout.py", ""),
    ("pyproject.toml", ""),
    (".streamlit/config.toml", ".streamlit"),
]

for f in os.listdir(os.path.join(base, "pages")):
    if f.endswith(".py"):
        files_to_upload.append((f"pages/{f}", "pages"))

for f in os.listdir(os.path.join(base, "static")):
    if f.endswith(".ttf"):
        files_to_upload.append((f"static/{f}", "static"))

for filepath, subdir in files_to_upload:
    local_path = os.path.join(base, filepath)
    stage_path = f"{stage}/{subdir}" if subdir else stage
    print(f"Uploading {filepath}...")
    cur.execute(f"PUT 'file://{local_path}' '{stage_path}/' AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    result = cur.fetchall()
    print(f"  -> {result}")

print("\nAll files uploaded. Creating Streamlit app...")

cur.execute("DROP STREAMLIT IF EXISTS MGG_PAGOS.PUBLIC.MGG_PAGOS_APP")

create_sql = """
CREATE STREAMLIT MGG_PAGOS.PUBLIC.MGG_PAGOS_APP
    ROOT_LOCATION = '@MGG_PAGOS.PUBLIC.MGG_PAGOS_APP_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'VW_GENAI'
"""

cur.execute(create_sql)
print("Streamlit app created successfully!")

cur.execute("SHOW STREAMLITS LIKE 'MGG_PAGOS_APP' IN SCHEMA MGG_PAGOS.PUBLIC")
rows = cur.fetchall()
for r in rows:
    print(f"  Name: {r[1]}, URL: {r[6] if len(r) > 6 else 'N/A'}")

cur.close()
conn.close()
print("Done!")
