import os
import streamlit as st
import pandas as pd


def is_sis():
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session()
        return True
    except Exception:
        return False


def _is_spcs():
    return os.path.isfile("/snowflake/session/token")


@st.cache_resource
def get_connector():
    import snowflake.connector
    if _is_spcs():
        return snowflake.connector.connect(
            host=os.getenv("SNOWFLAKE_HOST"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            authenticator="oauth",
            token=open("/snowflake/session/token").read(),
            database="MGG_FINTECH",
            schema="PUBLIC",
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        )
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "mariogalvisg"
    )


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    if is_sis():
        from snowflake.snowpark.context import get_active_session
        df = get_active_session().sql(sql).to_pandas()
    else:
        conn = get_connector()
        df = pd.read_sql(sql, conn)
    df.columns = [str(c).upper() for c in df.columns]
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass
    return df
