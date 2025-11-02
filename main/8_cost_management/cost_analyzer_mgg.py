"""
SNOWFLAKE COST ANALYZER - Análisis Completo de Créditos
App completa para Snowsight - Solo SELECT a vistas del sistema
Sin creación de objetos, todo en memoria
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
from typing import Dict, List, Optional, Tuple
from snowflake.snowpark.context import get_active_session

# Intentar importar plotly para gráficos avanzados
try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except:
    PLOTLY_AVAILABLE = False
    st.warning("Plotly no disponible, usando gráficos básicos de Streamlit")

# ============================================================================
# CONFIGURACIÓN DE SKUs - BASADO EN CreditConsumptionTable.pdf
# ============================================================================

SKU_MAPPING = {
    "Compute - Virtual Warehouse": {
        "sku_id": "WAREHOUSE_COMPUTE",
        "description": "Créditos de Virtual Warehouses (XS, S, M, L, XL, etc.)",
        "measurement": "SERVICE_TYPE = 'WAREHOUSE_METERING' en METERING_HISTORY",
        "credits_column": "CREDITS_USED",
        "date_column": "START_TIME",
        "extra_info": "Tasa variable por tamaño de warehouse y tiempo activo"
    },
    "Cloud Services": {
        "sku_id": "CLOUD_SERVICES",
        "description": "Servicios de nube (metadatos, seguridad, optimización)",
        "measurement": "SERVICE_TYPE = 'CLOUD_SERVICES' en METERING_HISTORY",
        "credits_column": "CREDITS_USED",
        "date_column": "START_TIME",
        "extra_info": "4.4 créditos/hora, con ajuste del 10% diario"
    },
    "Storage - On-Premise": {
        "sku_id": "STORAGE_ONPREM",
        "description": "Almacenamiento on-premise",
        "measurement": "DATABASE_NAME contiene '_ONPREM' en DATABASE_STORAGE_USAGE_HISTORY",
        "credits_column": "AVERAGE_DATABASE_BYTES",
        "date_column": "USAGE_DATE",
        "extra_info": "Billing por TB promedio/mes"
    },
    "Storage - Iceberg": {
        "sku_id": "STORAGE_ICEBERG",
        "description": "Almacenamiento de tablas Iceberg",
        "measurement": "TABLE_CATALOG = 'ICEBERG' en DATABASE_STORAGE_USAGE_HISTORY",
        "credits_column": "AVERAGE_DATABASE_BYTES",
        "date_column": "USAGE_DATE",
        "extra_info": "Almacenamiento externo con pricing diferenciado"
    },
    "Data Transfer - Ingress": {
        "sku_id": "DATA_TRANSFER_IN",
        "description": "Transferencia de datos entrantes (ingress)",
        "measurement": "TRANSFER_TYPE = 'INGRESS' en DATA_TRANSFER_HISTORY",
        "credits_column": "BYTES_TRANSFERRED",
        "date_column": "START_TIME",
        "extra_info": "Gratis en mayoría de casos, pero tracked"
    },
    "Data Transfer - Egress": {
        "sku_id": "DATA_TRANSFER_OUT",
        "description": "Transferencia de datos salientes (egress)",
        "measurement": "TRANSFER_TYPE = 'EGRESS' en DATA_TRANSFER_HISTORY",
        "credits_column": "BYTES_TRANSFERRED",
        "date_column": "START_TIME",
        "extra_info": "Cobro variable según región y cloud provider"
    },
    "Cortex Functions - Complete": {
        "sku_id": "CORTEX_COMPLETE",
        "description": "Uso de CORTEX.COMPLETE (text generation)",
        "measurement": "FUNCTION_NAME = 'COMPLETE' en CORTEX_FUNCTIONS_USAGE_HISTORY",
        "credits_column": "TOKEN_CREDITS",
        "date_column": "START_TIME",
        "extra_info": "Billing por tokens (input + output)"
    },
    "Cortex Functions - Embed": {
        "sku_id": "CORTEX_EMBED",
        "description": "Uso de CORTEX.EMBED (embeddings)",
        "measurement": "FUNCTION_NAME LIKE 'EMBED%' en CORTEX_FUNCTIONS_USAGE_HISTORY",
        "credits_column": "TOKEN_CREDITS",
        "date_column": "START_TIME",
        "extra_info": "Diferentes modelos tienen tasas distintas"
    },
    "Cortex Analyst": {
        "sku_id": "CORTEX_ANALYST",
        "description": "Cortex Analyst (REST API para análisis)",
        "measurement": "Tabla CORTEX_ANALYST_USAGE_HISTORY",
        "credits_column": "CREDITS",
        "date_column": "START_TIME",
        "extra_info": "67 créditos por 1,000 mensajes"
    },
    "Document AI": {
        "sku_id": "DOCUMENT_AI",
        "description": "Procesamiento de documentos con AI",
        "measurement": "Tabla DOCUMENT_AI_USAGE_HISTORY o CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY",
        "credits_column": "CREDITS_USED",
        "date_column": "START_TIME",
        "extra_info": "8 créditos/hora de compute + per-page charges"
    },
    "Search Serving": {
        "sku_id": "SEARCH_SERVING",
        "description": "Cortex Search (búsqueda vectorial)",
        "measurement": "Tabla CORTEX_SEARCH_SERVING_USAGE_HISTORY",
        "credits_column": "CREDITS",
        "date_column": "START_TIME",
        "extra_info": "6.3 créditos por GB/mes de datos indexados"
    },
    "Materialized Views": {
        "sku_id": "MATERIALIZED_VIEWS",
        "description": "Refrescos automáticos de Materialized Views",
        "measurement": "MATERIALIZED_VIEW_REFRESH_HISTORY o MATVW_AGGR_REFRESH",
        "credits_column": "ESTIMATED_CREDITS",
        "date_column": "START_TIME",
        "extra_info": "Serverless compute para refrescos"
    },
    "Search Optimization": {
        "sku_id": "SEARCH_OPTIMIZATION",
        "description": "Servicio de optimización de búsqueda",
        "measurement": "Tabla SEARCH_OPTIMIZATION_HISTORY",
        "credits_column": "CREDITS_USED",
        "date_column": "START_TIME",
        "extra_info": "Compute serverless para mantenimiento de índices"
    },
    "Data Quality": {
        "sku_id": "DATA_QUALITY",
        "description": "Snowflake Data Quality Score",
        "measurement": "Tabla DATA_QUALITY_HISTORY",
        "credits_column": "CREDITS_USED",
        "date_column": "START_TIME",
        "extra_info": "Compute serverless para scoring"
    },
    "External Functions": {
        "sku_id": "EXTERNAL_FUNCTIONS",
        "description": "Funciones externas (llamadas a APIs)",
        "measurement": "EXTERNAL_FUNCTION_HISTORY",
        "credits_column": "CREDITS_USED",
        "date_column": "START_TIME",
        "extra_info": "Compute + data transfer"
    },
    "Replication": {
        "sku_id": "REPLICATION",
        "description": "Replicación entre regiones/accounts",
        "measurement": "REPLICATION_USAGE_HISTORY",
        "credits_column": "BYTES_TRANSFERRED",
        "date_column": "START_TIME",
        "extra_info": "Storage + transfer costs"
    },
    "SPCS Compute": {
        "sku_id": "SPCS_COMPUTE",
        "description": "Snowpark Container Services compute",
        "measurement": "SERVICE_TYPE LIKE '%SPCS%' en METERING_HISTORY",
        "credits_column": "CREDITS_USED",
        "date_column": "START_TIME",
        "extra_info": "Billing por segundos de compute pools activos"
    },
    "Fine-Tuning": {
        "sku_id": "FINE_TUNING",
        "description": "Fine-tuning de modelos Cortex",
        "measurement": "Tabla CORTEX_FINE_TUNING_USAGE_HISTORY",
        "credits_column": "TOKEN_CREDITS",
        "date_column": "START_TIME",
        "extra_info": "Pricing por modelo y tokens procesados"
    }
}

# ============================================================================
# CONFIGURACIÓN Y SESIÓN
# ============================================================================

st.set_page_config(
    page_title="Snowflake Cost Analyzer",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def init_session():
    """Inicializa sesión de Snowflake desde Snowsight"""
    try:
        return get_active_session()
    except Exception as e:
        st.error(f"❌ Error conectando a Snowflake: {e}")
        st.info("💡 Esta app debe ejecutarse desde Snowsight Streamlit Apps")
        st.stop()

session = init_session()

# ============================================================================
# UTILIDADES DE DESCUBRIMIENTO DE ESQUEMA
# ============================================================================

@st.cache_data(ttl=3600)
def check_view_exists(view_name: str, schema: str = "ACCOUNT_USAGE") -> bool:
    """Verifica si una vista existe en el schema"""
    try:
        query = f"""
        SELECT TABLE_NAME 
        FROM SNOWFLAKE.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
            AND TABLE_NAME = '{view_name}'
            AND TABLE_TYPE = 'VIEW'
        LIMIT 1
        """
        result = session.sql(query).collect()
        return len(result) > 0
    except:
        return False

@st.cache_data(ttl=3600)
def get_columns_info(view_name: str, schema: str = "ACCOUNT_USAGE") -> pd.DataFrame:
    """Obtiene información de columnas de una vista"""
    try:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM SNOWFLAKE.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{schema}' 
            AND TABLE_NAME = '{view_name}'
        ORDER BY ORDINAL_POSITION
        """
        return session.sql(query).to_pandas()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def detect_date_column(view_name: str, schema: str = "ACCOUNT_USAGE") -> Optional[str]:
    """Detecta qué columna de fecha usar"""
    cols_df = get_columns_info(view_name, schema)
    if cols_df.empty:
        return None
    
    # Prioridad: START_TIME > USAGE_DATE > END_TIME > DATE > CREATED_ON
    priority_cols = ['START_TIME', 'USAGE_DATE', 'END_TIME', 'DATE', 'CREATED_ON']
    for col in priority_cols:
        if col in cols_df['COLUMN_NAME'].values:
            return col
    return None

@st.cache_data(ttl=3600)
def detect_credits_column(view_name: str, schema: str = "ACCOUNT_USAGE") -> Optional[str]:
    """Detecta qué columna de créditos usar"""
    cols_df = get_columns_info(view_name, schema)
    if cols_df.empty:
        return None
    
    # Prioridad: CREDITS_USED > CREDITS > TOKEN_CREDITS > ESTIMATED_CREDITS
    priority_cols = ['CREDITS_USED', 'CREDITS', 'TOKEN_CREDITS', 'ESTIMATED_CREDITS']
    for col in priority_cols:
        if col in cols_df['COLUMN_NAME'].values:
            return col
    return None

# Cache global de metadatos
@st.cache_data(ttl=3600)
def get_account_info() -> Dict:
    """Obtiene información de la cuenta"""
    try:
        queries = [
            ("account", "SELECT CURRENT_ACCOUNT() as VALUE"),
            ("region", "SELECT CURRENT_REGION() as VALUE"),
            ("role", "SELECT CURRENT_ROLE() as VALUE"),
            ("warehouse", "SELECT CURRENT_WAREHOUSE() as VALUE"),
            ("database", "SELECT CURRENT_DATABASE() as VALUE"),
            ("schema", "SELECT CURRENT_SCHEMA() as VALUE")
        ]
        info = {}
        for key, query in queries:
            try:
                result = session.sql(query).collect()
                if result:
                    info[key] = result[0]['VALUE']
            except:
                info[key] = "N/A"
        return info
    except:
        return {}

# ============================================================================
# FUNCIONES DE CONSULTA
# ============================================================================

def safe_query(query: str, description: str = "") -> Tuple[pd.DataFrame, str]:
    """Ejecuta query de forma segura con manejo de errores
    Retorna: (DataFrame, query_sql)"""
    try:
        df = session.sql(query).to_pandas()
        return df, query
    except Exception as e:
        st.warning(f"⚠️ {description}: {str(e)[:200]}")
        return pd.DataFrame(), query

def build_filter_conditions(warehouses=None, roles=None, users=None) -> str:
    """Construye condiciones WHERE adicionales basadas en filtros"""
    conditions = []
    
    if warehouses:
        wh_list = ','.join([f"'{wh}'" for wh in warehouses])
        conditions.append(f"WAREHOUSE_NAME IN ({wh_list})")
    
    if roles:
        role_list = ','.join([f"'{role}'" for role in roles])
        conditions.append(f"ROLE_NAME IN ({role_list})")
    
    if users:
        user_list = ','.join([f"'{user}'" for user in users])
        conditions.append(f"USER_NAME IN ({user_list})")
    
    if conditions:
        return " AND " + " AND ".join(conditions)
    return ""

@st.cache_data(ttl=300, show_spinner="Cargando métricas...")
def get_overview_metrics(start_date, end_date, warehouses=None, roles=None, users=None) -> Tuple[Dict, List[str]]:
    """Obtiene métricas de overview
    Retorna: (metrics_dict, queries_list)"""
    metrics = {}
    queries = []
    
    # Total de créditos
    if check_view_exists("METERING_HISTORY"):
        date_col = detect_date_column("METERING_HISTORY")
        credits_col = detect_credits_column("METERING_HISTORY")
        if date_col and credits_col:
            query = f"""
            SELECT SUM({credits_col}) as TOTAL
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
            """
            result, query_str = safe_query(query, "Total créditos")
            queries.append(("Total créditos", query_str))
            metrics['total_credits'] = result['TOTAL'].sum() if not result.empty else 0
    else:
        metrics['total_credits'] = 0
    
    # Desglose por servicio
    if check_view_exists("METERING_HISTORY"):
        date_col = detect_date_column("METERING_HISTORY")
        credits_col = detect_credits_column("METERING_HISTORY")
        if date_col and credits_col:
            query = f"""
            SELECT 
                SERVICE_TYPE,
                SUM({credits_col}) as TOTAL_CREDITS,
                COUNT(*) as RECORD_COUNT
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
            GROUP BY SERVICE_TYPE
            ORDER BY TOTAL_CREDITS DESC
            """
            result, query_str = safe_query(query, "Desglose por servicio")
            queries.append(("Desglose por servicio", query_str))
            metrics['service_breakdown'] = result
    else:
        metrics['service_breakdown'] = pd.DataFrame()
    
    return metrics, queries

@st.cache_data(ttl=300)
def get_daily_trends(start_date, end_date) -> Tuple[pd.DataFrame, str]:
    """Obtiene tendencia diaria de créditos
    Retorna: (DataFrame, query_sql)"""
    if not check_view_exists("METERING_HISTORY"):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("METERING_HISTORY")
    credits_col = detect_credits_column("METERING_HISTORY")
    if not date_col or not credits_col:
        return pd.DataFrame(), ""
    
    query = f"""
    SELECT 
        DATE({date_col}) as DATE,
        SUM({credits_col}) as DAILY_CREDITS,
        COUNT(DISTINCT SERVICE_TYPE) as NUM_SERVICES
    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
    GROUP BY DATE({date_col})
    ORDER BY DATE
    """
    return safe_query(query, "Tendencias diarias")

@st.cache_data(ttl=300)
def get_warehouse_stats(start_date, end_date, warehouses=None, roles=None, users=None) -> Tuple[pd.DataFrame, str]:
    """Estadísticas de warehouses
    Retorna: (DataFrame, query_sql)"""
    if not check_view_exists("WAREHOUSE_METERING_HISTORY"):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("WAREHOUSE_METERING_HISTORY")
    credits_col = detect_credits_column("WAREHOUSE_METERING_HISTORY")
    if not date_col or not credits_col:
        return pd.DataFrame(), ""
    
    filter_cond = build_filter_conditions(warehouses, roles=None, users=None)
    
    # Query básica sin columnas problemáticas
    query = f"""
    SELECT 
        WAREHOUSE_NAME,
        SUM({credits_col}) as TOTAL_CREDITS,
        COUNT(*) as RECORD_COUNT
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'{filter_cond}
    GROUP BY WAREHOUSE_NAME
    ORDER BY TOTAL_CREDITS DESC
    """
    return safe_query(query, "Stats de warehouses")

@st.cache_data(ttl=300)
def get_top_queries(start_date, end_date, limit: int = 20, warehouses=None, roles=None, users=None) -> Tuple[pd.DataFrame, str]:
    """Top queries por créditos
    Retorna: (DataFrame, query_sql)"""
    if not check_view_exists("QUERY_HISTORY"):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("QUERY_HISTORY")
    if not date_col:
        return pd.DataFrame(), ""
    
    filter_cond = build_filter_conditions(warehouses, roles, users)
    
    query = f"""
    SELECT 
        QUERY_ID,
        WAREHOUSE_NAME,
        DATABASE_NAME,
        SCHEMA_NAME,
        USER_NAME,
        ROLE_NAME,
        QUERY_TEXT,
        TOTAL_ELAPSED_TIME,
        BYTES_SCANNED,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        BYTES_SPILLED_TO_REMOTE_STORAGE,
        CREDITS_USED_CLOUD_SERVICES,
        START_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
        AND QUERY_TYPE = 'SELECT'
        AND EXECUTION_STATUS = 'SUCCESS'{filter_cond}
    ORDER BY TOTAL_ELAPSED_TIME DESC
    LIMIT {limit}
    """
    return safe_query(query, "Top queries")

@st.cache_data(ttl=300)
def get_storage_stats(start_date, end_date) -> Tuple[pd.DataFrame, str]:
    """Estadísticas de almacenamiento
    Retorna: (DataFrame, query_sql)"""
    if not check_view_exists("DATABASE_STORAGE_USAGE_HISTORY"):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("DATABASE_STORAGE_USAGE_HISTORY")
    if not date_col:
        return pd.DataFrame(), ""
    
    # Query básica con columnas estándar
    query = f"""
    SELECT 
        {date_col} as USAGE_DATE,
        DATABASE_NAME,
        AVERAGE_DATABASE_BYTES
    FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
    ORDER BY {date_col} DESC, AVERAGE_DATABASE_BYTES DESC
    """
    return safe_query(query, "Stats de storage")

@st.cache_data(ttl=300)
def get_cortex_stats(start_date, end_date) -> Tuple[pd.DataFrame, List[str]]:
    """Estadísticas de Cortex AI
    Retorna: (DataFrame, queries_list)"""
    cortex_data = []
    queries = []
    
    # CORTEX_FUNCTIONS_USAGE_HISTORY
    if check_view_exists("CORTEX_FUNCTIONS_USAGE_HISTORY"):
        date_col = detect_date_column("CORTEX_FUNCTIONS_USAGE_HISTORY")
        credits_col = detect_credits_column("CORTEX_FUNCTIONS_USAGE_HISTORY")
        if date_col and credits_col:
            query = f"""
            SELECT 
                FUNCTION_NAME,
                MODEL_NAME,
                SUM({credits_col}) as TOTAL_CREDITS,
                SUM(TOKENS) as TOTAL_TOKENS,
                COUNT(*) as INVOCATIONS
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
            WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
            GROUP BY FUNCTION_NAME, MODEL_NAME
            ORDER BY TOTAL_CREDITS DESC
            """
            result, query_str = safe_query(query, "Cortex functions")
            queries.append(("Cortex Functions", query_str))
            if not result.empty:
                result['SERVICE_TYPE'] = 'CORTEX_FUNCTIONS'
                cortex_data.append(result)
    
    # CORTEX_ANALYST_USAGE_HISTORY
    if check_view_exists("CORTEX_ANALYST_USAGE_HISTORY"):
        date_col = detect_date_column("CORTEX_ANALYST_USAGE_HISTORY")
        credits_col = detect_credits_column("CORTEX_ANALYST_USAGE_HISTORY")
        if date_col and credits_col:
            query = f"""
            SELECT 
                'CORTEX_ANALYST' as FUNCTION_NAME,
                NULL as MODEL_NAME,
                SUM({credits_col}) as TOTAL_CREDITS,
                SUM(REQUEST_COUNT) as TOTAL_TOKENS,
                COUNT(*) as INVOCATIONS
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
            WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
            GROUP BY FUNCTION_NAME, MODEL_NAME
            """
            result, query_str = safe_query(query, "Cortex Analyst")
            queries.append(("Cortex Analyst", query_str))
            if not result.empty:
                result['SERVICE_TYPE'] = 'CORTEX_ANALYST'
                cortex_data.append(result)
    
    if cortex_data:
        return pd.concat(cortex_data, ignore_index=True), queries
    return pd.DataFrame(), queries

@st.cache_data(ttl=300)
def get_data_transfer_stats(start_date, end_date) -> Tuple[pd.DataFrame, str]:
    """Estadísticas de transferencia de datos
    Retorna: (DataFrame, query_sql)"""
    if not check_view_exists("DATA_TRANSFER_HISTORY"):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("DATA_TRANSFER_HISTORY")
    if not date_col:
        return pd.DataFrame(), ""
    
    # Query básica sin columnas que pueden no existir
    query = f"""
    SELECT 
        TRANSFER_TYPE,
        SOURCE_REGION,
        TARGET_REGION,
        BYTES_TRANSFERRED
    FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
    ORDER BY BYTES_TRANSFERRED DESC
    """
    return safe_query(query, "Data transfer")

@st.cache_data(ttl=300)
def get_user_stats(start_date, end_date, warehouses=None, roles=None, users=None) -> Tuple[pd.DataFrame, str]:
    """Estadísticas de uso por usuario
    Retorna: (DataFrame, query_sql)"""
    if not check_view_exists("QUERY_HISTORY"):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("QUERY_HISTORY")
    if not date_col:
        return pd.DataFrame(), ""
    
    filter_cond = build_filter_conditions(warehouses, roles, users)
    
    query = f"""
    SELECT 
        USER_NAME,
        SUM(CREDITS_USED_CLOUD_SERVICES) as TOTAL_CREDITS,
        COUNT(*) as QUERY_COUNT,
        SUM(TOTAL_ELAPSED_TIME) as TOTAL_TIME_MS
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
        AND EXECUTION_STATUS = 'SUCCESS'{filter_cond}
    GROUP BY USER_NAME
    ORDER BY TOTAL_CREDITS DESC
    """
    return safe_query(query, "User stats")

@st.cache_data(ttl=300)
def get_role_stats(start_date, end_date, warehouses=None, roles=None, users=None) -> Tuple[pd.DataFrame, str]:
    """Estadísticas de uso por rol
    Retorna: (DataFrame, query_sql)"""
    if not check_view_exists("QUERY_HISTORY"):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("QUERY_HISTORY")
    if not date_col:
        return pd.DataFrame(), ""
    
    filter_cond = build_filter_conditions(warehouses, roles, users)
    
    query = f"""
    SELECT 
        ROLE_NAME,
        SUM(CREDITS_USED_CLOUD_SERVICES) as TOTAL_CREDITS,
        COUNT(*) as QUERY_COUNT,
        SUM(TOTAL_ELAPSED_TIME) as TOTAL_TIME_MS
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
        AND EXECUTION_STATUS = 'SUCCESS'{filter_cond}
    GROUP BY ROLE_NAME
    ORDER BY TOTAL_CREDITS DESC
    """
    return safe_query(query, "Role stats")

@st.cache_data(ttl=300)
def get_user_role_combined(start_date, end_date, warehouses=None, roles=None, users=None) -> Tuple[pd.DataFrame, str]:
    """Estadísticas combinadas usuario-rol
    Retorna: (DataFrame, query_sql)"""
    if not check_view_exists("QUERY_HISTORY"):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("QUERY_HISTORY")
    if not date_col:
        return pd.DataFrame(), ""
    
    filter_cond = build_filter_conditions(warehouses, roles, users)
    
    query = f"""
    SELECT 
        USER_NAME,
        ROLE_NAME,
        SUM(CREDITS_USED_CLOUD_SERVICES) as TOTAL_CREDITS,
        COUNT(*) as QUERY_COUNT,
        SUM(TOTAL_ELAPSED_TIME) as TOTAL_TIME_MS
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
        AND EXECUTION_STATUS = 'SUCCESS'{filter_cond}
    GROUP BY USER_NAME, ROLE_NAME
    ORDER BY TOTAL_CREDITS DESC
    """
    return safe_query(query, "User-Role combined")

@st.cache_data(ttl=300)
def get_cost_by_object_tag(start_date, end_date) -> Tuple[pd.DataFrame, str]:
    """Atribución de costos usando etiquetas de objetos (Object Tagging)
    Retorna: (DataFrame, query_sql)"""
    if not (check_view_exists("WAREHOUSE_METERING_HISTORY") and check_view_exists("TAG_REFERENCES")):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("WAREHOUSE_METERING_HISTORY")
    credits_col = detect_credits_column("WAREHOUSE_METERING_HISTORY")
    if not date_col or not credits_col:
        return pd.DataFrame(), ""
    
    query = f"""
    SELECT 
        COALESCE(tag_references.TAG_VALUE, 'untagged') AS TAG_VALUE,
        SUM(warehouse_metering_history.{credits_col}) AS TOTAL_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY warehouse_metering_history
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tag_references
        ON warehouse_metering_history.WAREHOUSE_ID = tag_references.OBJECT_ID
    WHERE warehouse_metering_history.{date_col} >= '{start_date}' 
        AND warehouse_metering_history.{date_col} < '{end_date}'
    GROUP BY COALESCE(tag_references.TAG_VALUE, 'untagged')
    ORDER BY TOTAL_CREDITS DESC
    """
    return safe_query(query, "Cost by object tag")

@st.cache_data(ttl=300)
def get_cost_by_naming_convention(start_date, end_date) -> Tuple[pd.DataFrame, str]:
    """Atribución de costos usando convenciones de nombres en warehouses
    Retorna: (DataFrame, query_sql)"""
    if not check_view_exists("WAREHOUSE_METERING_HISTORY"):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("WAREHOUSE_METERING_HISTORY")
    credits_col = detect_credits_column("WAREHOUSE_METERING_HISTORY")
    if not date_col or not credits_col:
        return pd.DataFrame(), ""
    
    query = f"""
    SELECT 
        CASE 
            WHEN POSITION('_' IN WAREHOUSE_NAME) > 0 
                THEN SPLIT_PART(WAREHOUSE_NAME, '_', 1)
            ELSE 'others'
        END AS TEAM_NAME,
        SUM({credits_col}) AS TOTAL_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
    GROUP BY TEAM_NAME
    ORDER BY TOTAL_CREDITS DESC
    """
    return safe_query(query, "Cost by naming convention")

@st.cache_data(ttl=300)
def get_cost_attribution_by_user(start_date, end_date) -> Tuple[pd.DataFrame, str]:
    """Atribución de costos por usuario usando query_attribution_history
    Retorna: (DataFrame, query_sql)"""
    if not (check_view_exists("WAREHOUSE_METERING_HISTORY") and check_view_exists("QUERY_ATTRIBUTION_HISTORY")):
        return pd.DataFrame(), ""
    
    wh_date_col = detect_date_column("WAREHOUSE_METERING_HISTORY")
    qa_date_col = detect_date_column("QUERY_ATTRIBUTION_HISTORY")
    wh_credits_col = detect_credits_column("WAREHOUSE_METERING_HISTORY")
    
    if not wh_date_col or not qa_date_col or not wh_credits_col:
        return pd.DataFrame(), ""
    
    query = f"""
    WITH wh_bill AS (
        SELECT SUM({wh_credits_col}) AS compute_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE {wh_date_col} >= '{start_date}' AND {wh_date_col} < '{end_date}'
    ),
    user_credits AS (
        SELECT user_name, SUM(CREDITS_ATTRIBUTED_COMPUTE) AS credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
        WHERE {qa_date_col} >= '{start_date}' AND {qa_date_col} < '{end_date}'
        GROUP BY user_name
    ),
    total_credit AS (
        SELECT SUM(credits) AS sum_all_credits
        FROM user_credits
    )
    SELECT 
        u.user_name,
        u.credits / t.sum_all_credits * w.compute_credits AS attributed_credits
    FROM user_credits u, total_credit t, wh_bill w
    ORDER BY attributed_credits DESC
    """
    return safe_query(query, "Cost attribution by user")

@st.cache_data(ttl=300)
def get_cost_by_query_tag(start_date, end_date) -> Tuple[pd.DataFrame, str]:
    """Atribución de costos usando etiquetas de consultas (Query Tags)
    Retorna: (DataFrame, query_sql)"""
    if not (check_view_exists("WAREHOUSE_METERING_HISTORY") and check_view_exists("QUERY_ATTRIBUTION_HISTORY")):
        return pd.DataFrame(), ""
    
    wh_date_col = detect_date_column("WAREHOUSE_METERING_HISTORY")
    qa_date_col = detect_date_column("QUERY_ATTRIBUTION_HISTORY")
    wh_credits_col = detect_credits_column("WAREHOUSE_METERING_HISTORY")
    
    if not wh_date_col or not qa_date_col or not wh_credits_col:
        return pd.DataFrame(), ""
    
    query = f"""
    WITH wh_bill AS (
        SELECT SUM({wh_credits_col}) AS compute_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE {wh_date_col} >= '{start_date}' AND {wh_date_col} < '{end_date}'
    ),
    tag_credits AS (
        SELECT COALESCE(NULLIF(query_tag, ''), 'untagged') AS tag, SUM(CREDITS_ATTRIBUTED_COMPUTE) AS credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
        WHERE {qa_date_col} >= '{start_date}' AND {qa_date_col} < '{end_date}'
        GROUP BY tag
    ),
    total_credit AS (
        SELECT SUM(credits) AS sum_all_credits
        FROM tag_credits
    )
    SELECT 
        tc.tag,
        tc.credits / t.sum_all_credits * w.compute_credits AS attributed_credits
    FROM tag_credits tc, total_credit t, wh_bill w
    ORDER BY attributed_credits DESC
    """
    return safe_query(query, "Cost by query tag")

@st.cache_data(ttl=300)
def get_cost_by_query_hash(start_date, end_date, limit: int = 20) -> Tuple[pd.DataFrame, str]:
    """Atribución de costos para consultas recurrentes (Query Hash)
    Retorna: (DataFrame, query_sql)"""
    if not check_view_exists("QUERY_ATTRIBUTION_HISTORY"):
        return pd.DataFrame(), ""
    
    date_col = detect_date_column("QUERY_ATTRIBUTION_HISTORY")
    if not date_col:
        return pd.DataFrame(), ""
    
    query = f"""
    SELECT 
        QUERY_PARAMETERIZED_HASH,
        COUNT(*) AS QUERY_COUNT,
        SUM(CREDITS_ATTRIBUTED_COMPUTE) AS TOTAL_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
    GROUP BY QUERY_PARAMETERIZED_HASH
    ORDER BY TOTAL_CREDITS DESC
    LIMIT {limit}
    """
    return safe_query(query, "Cost by query hash")

@st.cache_data(ttl=600)
def get_available_warehouses(start_date, end_date) -> list:
    """Obtener lista de warehouses disponibles en el período"""
    if not check_view_exists("WAREHOUSE_METERING_HISTORY"):
        return []
    
    date_col = detect_date_column("WAREHOUSE_METERING_HISTORY")
    if not date_col:
        return []
    
    query = f"""
    SELECT DISTINCT WAREHOUSE_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
    ORDER BY WAREHOUSE_NAME
    """
    result, _ = safe_query(query, "Available warehouses")
    return result['WAREHOUSE_NAME'].tolist() if not result.empty else []

@st.cache_data(ttl=600)
def get_available_roles(start_date, end_date) -> list:
    """Obtener lista de roles disponibles en el período"""
    if not check_view_exists("QUERY_HISTORY"):
        return []
    
    date_col = detect_date_column("QUERY_HISTORY")
    if not date_col:
        return []
    
    query = f"""
    SELECT DISTINCT ROLE_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
    ORDER BY ROLE_NAME
    """
    result, _ = safe_query(query, "Available roles")
    return result['ROLE_NAME'].tolist() if not result.empty else []

@st.cache_data(ttl=600)
def get_available_users(start_date, end_date) -> list:
    """Obtener lista de usuarios disponibles en el período"""
    if not check_view_exists("QUERY_HISTORY"):
        return []
    
    date_col = detect_date_column("QUERY_HISTORY")
    if not date_col:
        return []
    
    query = f"""
    SELECT DISTINCT USER_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'
    ORDER BY USER_NAME
    """
    result, _ = safe_query(query, "Available users")
    return result['USER_NAME'].tolist() if not result.empty else []

# ============================================================================
# FUNCIÓN AUXILIAR PARA CREAR PIE CHARTS
# ============================================================================

def create_pie_chart(df, label_col, value_col, cost_col=None, title="Distribución", show_percentage=True):
    """Crea un gráfico de pastel con valores, porcentajes y costos USD si están disponibles"""
    if df.empty:
        st.info("No hay datos para mostrar")
        return
    
    # Preparar datos
    chart_df = df.copy()
    total = chart_df[value_col].sum()
    chart_df['PERCENTAGE'] = (chart_df[value_col] / total * 100).round(2)
    
    if PLOTLY_AVAILABLE:
        # Preparar hover text
        if cost_col and cost_col in chart_df.columns:
            # Formatear valores según el tipo (créditos, GB, TB, etc.)
            if 'CREDITS' in value_col.upper() or 'CREDIT' in value_col.upper():
                value_label = "Créditos"
                value_format = lambda v: f"{v:.4f}"
            elif 'TB' in value_col.upper():
                value_label = "TB"
                value_format = lambda v: f"{v:.2f}"
            elif 'GB' in value_col.upper():
                value_label = "GB"
                value_format = lambda v: f"{v:.2f}"
            else:
                value_label = "Valor"
                value_format = lambda v: f"{v:.2f}"
            
            hover_text = [
                f"<b>{row[label_col]}</b><br>" +
                f"{value_label}: {value_format(row[value_col])}<br>" +
                f"Porcentaje: {row['PERCENTAGE']:.2f}%<br>" +
                f"Costo: ${row[cost_col]:.2f} USD"
                for _, row in chart_df.iterrows()
            ]
        else:
            hover_text = [
                f"<b>{row[label_col]}</b><br>" +
                f"Valor: {row[value_col]:.4f}<br>" +
                f"Porcentaje: {row['PERCENTAGE']:.2f}%"
                for _, row in chart_df.iterrows()
            ]
        
        # Crear el pie chart con hover personalizado
        fig = go.Figure(data=[go.Pie(
            labels=chart_df[label_col],
            values=chart_df[value_col],
            hole=0.4,
            texttemplate='%{label}<br>%{percent}' if show_percentage else '%{label}<br>%{value:.2f}',
            textposition='outside',
            customdata=hover_text,
            hovertemplate='%{customdata}<extra></extra>'
        )])
        fig.update_layout(
            height=450,
            showlegend=True,
            legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.1),
            title=title,
            margin=dict(l=20, r=20, t=60, b=40),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        # Fallback a bar chart si plotly no está disponible
        st.bar_chart(chart_df.set_index(label_col)[value_col])

# ============================================================================
# FUNCIÓN AUXILIAR PARA MOSTRAR QUERIES SQL
# ============================================================================

def display_sql_queries(queries, title="🔍 Query SQL Ejecutado"):
    """Muestra una sección con los queries SQL ejecutados"""
    st.markdown("---")
    st.subheader(title)
    st.caption("Esta sección muestra el query SQL utilizado para generar los datos mostrados arriba. Úsalo como referencia o validación.")
    
    if isinstance(queries, str):
        # Un solo query como string
        if queries:
            with st.expander("Ver Query SQL", expanded=False):
                st.code(queries, language="sql")
    elif isinstance(queries, list):
        # Lista de queries (puede ser lista de strings o lista de tuples)
        if queries:
            if len(queries) == 1:
                # Un solo query
                if isinstance(queries[0], tuple):
                    query_name, query_str = queries[0]
                    with st.expander(f"Ver Query SQL: {query_name}", expanded=False):
                        st.code(query_str, language="sql")
                else:
                    with st.expander("Ver Query SQL", expanded=False):
                        st.code(queries[0], language="sql")
            else:
                # Múltiples queries
                for i, query_item in enumerate(queries):
                    if isinstance(query_item, tuple):
                        query_name, query_str = query_item
                        if query_str:
                            with st.expander(f"Query {i+1}: {query_name}", expanded=False):
                                st.code(query_str, language="sql")
                    else:
                        if query_item:
                            with st.expander(f"Query {i+1}", expanded=False):
                                st.code(query_item, language="sql")
    else:
        st.info("No hay queries SQL disponibles para mostrar")

# ============================================================================
# UI PRINCIPAL
# ============================================================================

# Estilos CSS personalizados para mejor diseño visual
st.markdown("""
    <style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 0.5rem;
        border-radius: 0.5rem;
    }
    h1, h2, h3 {
        margin-top: 1.5rem !important;
        margin-bottom: 1rem !important;
    }
    </style>
""", unsafe_allow_html=True)

# Header con info de cuenta
st.title("💰 Snowflake Cost Analyzer")
account_info = get_account_info()

# Mostrar información de cuenta (primera fila)
if account_info:
    col_h1, col_h2, col_h3, col_h4 = st.columns(4)
    with col_h1:
        st.caption(f"🏢 **Account:** {account_info.get('account', 'N/A')}")
    with col_h2:
        st.caption(f"🌍 **Region:** {account_info.get('region', 'N/A')}")
    with col_h3:
        st.caption(f"👤 **Role:** {account_info.get('role', 'N/A')}")
    with col_h4:
        st.caption(f"⏱️ **Updated:** {datetime.now().strftime('%H:%M:%S')}")

st.markdown("---")

# Espacio reservado para mostrar información de fechas después de que se definan en el sidebar
date_info_placeholder = st.empty()

# Sidebar con filtros
with st.sidebar:
    st.header("📅 Filtros Globales")
    
    # Filtros de fecha
    st.subheader("Período de Análisis")
    date_range = st.radio(
        "Rango rápido",
        ["7 días", "30 días", "90 días", "Personalizado"],
        index=1
    )
    
    if date_range == "7 días":
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()
    elif date_range == "30 días":
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
    elif date_range == "90 días":
        start_date = datetime.now() - timedelta(days=90)
        end_date = datetime.now()
    else:
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            start_date = st.date_input("Inicio", value=datetime.now() - timedelta(days=30))
        with col_d2:
            end_date = st.date_input("Fin", value=datetime.now())
    
    # Configuración
    # Inicializar filtros como listas vacías (filtros adicionales removidos)
    warehouses = []
    roles = []
    users = []
    
    st.subheader("⚙️ Configuración")
    credit_price_usd = st.number_input(
        "Precio de crédito (USD)",
        min_value=0.0,
        value=3.0,
        step=0.01,
        help="Precio promedio de un crédito en USD (default: 3.0)"
    )
    

# Calcular días del período para usar en todas las tabs
days = (end_date - start_date).days if isinstance(start_date, datetime) and isinstance(end_date, datetime) else (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days

# Mostrar información de fechas en el header (segunda fila)
with date_info_placeholder.container():
    # Formatear fechas para mostrar
    if isinstance(start_date, datetime):
        start_date_str = start_date.strftime('%Y-%m-%d')
    elif isinstance(start_date, pd.Timestamp):
        start_date_str = start_date.strftime('%Y-%m-%d')
    else:
        start_date_str = str(start_date)
    
    if isinstance(end_date, datetime):
        end_date_str = end_date.strftime('%Y-%m-%d')
    elif isinstance(end_date, pd.Timestamp):
        end_date_str = end_date.strftime('%Y-%m-%d')
    else:
        end_date_str = str(end_date)
    
    col_d1, col_d2, col_d3 = st.columns(3)
    with col_d1:
        st.caption(f"📅 **Fecha Inicio:** {start_date_str}")
    with col_d2:
        st.caption(f"📅 **Fecha Fin:** {end_date_str}")
    with col_d3:
        st.caption(f"📊 **Días Evaluados:** {days}")

st.markdown("---")

# Tabs principales
tab_overview, tab_compute, tab_storage, tab_transfer, tab_cortex, tab_usuarios, tab_roles, tab_tags, tab_queries, tab_skus, tab_settings = st.tabs([
    "📊 Overview",
    "🖥️ Compute",
    "💾 Storage",
    "🔄 Data Transfer",
    "🤖 Cortex & AI",
    "👤 Usuarios",
    "🔐 Roles",
    "🏷️ Tags",
    "📝 Queries",
    "📋 SKUs",
    "⚙️ Settings"
])

# ============================================================================
# TAB: OVERVIEW
# ============================================================================

with tab_overview:
    st.header("📊 Overview - Resumen General")
    
    with st.spinner("Cargando métricas..."):
        metrics, overview_queries = get_overview_metrics(start_date, end_date, warehouses, roles, users)
    
    # === MÉTRICAS PRINCIPALES ===
    total_credits_gastados = metrics.get('total_credits', 0)
    avg_daily = total_credits_gastados / days if days > 0 else 0
    cost_usd_gastado = total_credits_gastados * credit_price_usd
    
    # Filas de métricas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Créditos Gastados",
            f"{total_credits_gastados:.4f}",
            help="Suma de créditos consumidos en el período"
        )
    
    with col2:
        st.metric(
            "Costo USD Gastado",
            f"${cost_usd_gastado:.2f}",
            help=f"Basado en {credit_price_usd} USD por crédito"
        )
    
    with col3:
        st.metric(
            "Promedio Diario",
            f"{avg_daily:.4f}",
            help="Créditos promedio por día"
        )
    
    with col4:
        num_services = len(metrics.get('service_breakdown', pd.DataFrame()))
        st.metric(
            "Servicios Activos",
            num_services,
            help="Número de servicios distintos en uso"
        )
    
    st.markdown("---")
    
    # === GRÁFICO DE PIE - DISTRIBUCIÓN DE COSTOS ===
    service_df = metrics.get('service_breakdown', pd.DataFrame())
    if not service_df.empty:
        # Calcular porcentajes
        total = service_df['TOTAL_CREDITS'].sum()
        service_df['PERCENTAGE'] = (service_df['TOTAL_CREDITS'] / total * 100).round(2)
        service_df['COST_USD'] = service_df['TOTAL_CREDITS'] * credit_price_usd
        
        st.subheader("🥧 Distribución de Costos por Categoría (100%)")
        
        # Gráfico de pie
        col_chart, col_info = st.columns([2, 1])
        
        with col_chart:
            if PLOTLY_AVAILABLE:
                fig = go.Figure(data=[go.Pie(
                    labels=service_df['SERVICE_TYPE'],
                    values=service_df['TOTAL_CREDITS'],
                    hole=0.4,
                    texttemplate='%{label}<br>%{percent}',
                    textposition='outside',
                    hovertemplate='<b>%{label}</b><br>' +
                                'Créditos: %{value:.4f}<br>' +
                                'Porcentaje: %{percent}<br>' +
                                'USD: $%{customdata:.2f}<extra></extra>',
                    customdata=service_df['COST_USD']
                )])
                fig.update_layout(
                    height=450,
                    showlegend=True,
                    legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.1),
                    margin=dict(l=20, r=20, t=40, b=40),
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.bar_chart(service_df.set_index('SERVICE_TYPE')['PERCENTAGE'])
        
        with col_info:
            st.markdown("### Detalle Rápido")
            for idx, row in service_df.head(5).iterrows():
                with st.container():
                    st.markdown(f"**{row['SERVICE_TYPE']}**")
                    st.caption(f"{row['PERCENTAGE']:.1f}% • {row['TOTAL_CREDITS']:.4f} créditos")
        
        st.markdown("---")
        
        # === EXPANDER DINÁMICO PARA DETALLES ===
        st.subheader("📋 Detalles por Categoría")
        # Crear lista de todas las categorías generadoras de costo (del breakdown + todas las SKU)
        all_categories = service_df['SERVICE_TYPE'].tolist() if not service_df.empty else []
        # Agregar todas las categorías de SKU_MAPPING
        sku_categories = list(SKU_MAPPING.keys())
        # Combinar y eliminar duplicados manteniendo orden
        combined_categories = all_categories.copy()
        for sku_cat in sku_categories:
            if sku_cat not in combined_categories:
                combined_categories.append(sku_cat)
        
        selected_service = st.selectbox(
            "Selecciona una categoría para ver detalles:",
            options=["-- Todas --"] + combined_categories,
            key="overview_service_select"
        )
        
        if selected_service and selected_service != "-- Todas --":
            # Verificar si la categoría está en service_df
            if selected_service in service_df['SERVICE_TYPE'].values:
                detail_data = service_df[service_df['SERVICE_TYPE'] == selected_service].iloc[0]
                
                col_d1, col_d2, col_d3 = st.columns(3)
                with col_d1:
                    st.metric("Créditos", f"{detail_data['TOTAL_CREDITS']:.4f}")
                with col_d2:
                    st.metric("Porcentaje", f"{detail_data['PERCENTAGE']:.2f}%")
                with col_d3:
                    st.metric("Costo USD", f"${detail_data['COST_USD']:.2f}")
                
                # Aquí podrías añadir más detalles específicos por servicio
            else:
                # Es una categoría de SKU que no tiene datos en el período
                if selected_service in SKU_MAPPING:
                    sku_info = SKU_MAPPING[selected_service]
                    st.info(f"ℹ️ La categoría '{selected_service}' no tiene datos en el período seleccionado.")
                    st.markdown(f"**Descripción:** {sku_info['description']}")
                    st.markdown(f"**Medición:** {sku_info['measurement']}")
                    st.markdown(f"**Columnas:** `{sku_info['credits_column']}`, `{sku_info['date_column']}`")
                    st.markdown(f"**Info adicional:** {sku_info['extra_info']}")
        else:
            # Mostrar todas las categorías
            st.dataframe(service_df[['SERVICE_TYPE', 'TOTAL_CREDITS', 'PERCENTAGE', 'RECORD_COUNT']].style.format({
                'TOTAL_CREDITS': '{:.6f}',
                'PERCENTAGE': '{:.2f}%',
                'RECORD_COUNT': '{:,.0f}'
            }), use_container_width=True)
        
        st.markdown("---")
        
        # Exportar
        with st.expander("📥 Exportar datos"):
            csv = service_df.to_csv(index=False)
            st.download_button("Descargar CSV", csv, "service_breakdown.csv", "text/csv")
    
    else:
        st.info("No hay datos de desglose por servicio")
    
    st.markdown("---")
    
    # === TENDENCIA DIARIA ===
    st.subheader("📈 Tendencia Diaria de Consumo")
    trends, trends_query = get_daily_trends(start_date, end_date)
    if not trends.empty:
        trends['DATE'] = pd.to_datetime(trends['DATE'])
        st.line_chart(trends.set_index('DATE')['DAILY_CREDITS'])
        
        with st.expander("Ver datos detallados"):
            st.dataframe(trends)
            
            # Exportar
            csv_trends = trends.to_csv(index=False)
            st.download_button(
                "📥 Exportar CSV",
                csv_trends,
                "daily_trends.csv",
                "text/csv"
            )
    else:
        st.info("No hay datos de tendencias disponibles")
    
    # === MOSTRAR QUERIES SQL ===
    all_overview_queries = list(overview_queries)
    if trends_query:
        all_overview_queries.append(("Tendencias diarias", trends_query))
    if all_overview_queries:
        display_sql_queries(all_overview_queries, "🔍 Queries SQL Ejecutados - Overview")

# ============================================================================
# TAB: COMPUTE
# ============================================================================

with tab_compute:
    st.header("🖥️ Compute - Warehouses y Procesamiento")
    
    warehouse_stats, warehouse_query = get_warehouse_stats(start_date, end_date, warehouses, roles, users)
    if not warehouse_stats.empty:
        # === MÉTRICAS RESUMEN ===
        total_credits_compute = warehouse_stats['TOTAL_CREDITS'].sum()
        cost_usd_compute = total_credits_compute * credit_price_usd
        avg_daily_compute = total_credits_compute / days if days > 0 else 0
        
        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        with col_m1:
            st.metric("Total Créditos", f"{total_credits_compute:.4f}")
        with col_m2:
            st.metric("Costo USD", f"${cost_usd_compute:.2f}")
        with col_m3:
            st.metric("Promedio Diario", f"{avg_daily_compute:.4f}")
        with col_m4:
            st.metric("WHs Únicos", warehouse_stats['WAREHOUSE_NAME'].nunique())
        
        st.markdown("---")
        
        st.subheader("Warehouses por Consumo")
        # Agregar columna de precio en USD
        warehouse_stats_display = warehouse_stats.copy()
        warehouse_stats_display['COST_USD'] = warehouse_stats_display['TOTAL_CREDITS'] * credit_price_usd
        
        # Gráfica de pastel para warehouses (primero)
        create_pie_chart(
            warehouse_stats_display,
            'WAREHOUSE_NAME',
            'TOTAL_CREDITS',
            'COST_USD',
            title="Distribución de Créditos por Warehouse"
        )
        
        st.markdown("")  # Espacio antes de la tabla
        
        # Reordenar columnas para mostrar COST_USD después de TOTAL_CREDITS
        cols_order = ['WAREHOUSE_NAME', 'TOTAL_CREDITS', 'COST_USD', 'RECORD_COUNT']
        warehouse_stats_display = warehouse_stats_display[cols_order]
        st.dataframe(
            warehouse_stats_display.style.format({
                'TOTAL_CREDITS': '{:.4f}',
                'COST_USD': '${:.2f}'
            }),
            use_container_width=True
        )
        
        # Estadísticas adicionales
        col_c1, col_c2 = st.columns(2)
        with col_c1:
            st.metric("Queries Totales", f"{warehouse_stats['RECORD_COUNT'].sum():,}")
        with col_c2:
            st.metric("Registros Totales", f"{warehouse_stats['RECORD_COUNT'].sum():,}")
        
        with st.expander("📥 Exportar"):
            csv = warehouse_stats.to_csv(index=False)
            st.download_button("CSV", csv, "warehouse_stats.csv", "text/csv")
    else:
        st.info("No hay datos de warehouses disponibles")
    
    # === MOSTRAR QUERY SQL ===
    if warehouse_query:
        display_sql_queries(warehouse_query, "🔍 Query SQL Ejecutado - Compute")

# ============================================================================
# TAB: STORAGE
# ============================================================================

with tab_storage:
    st.header("💾 Storage - Almacenamiento de Datos")
    
    storage_stats, storage_query = get_storage_stats(start_date, end_date)
    if not storage_stats.empty:
        # Convertir bytes a TB
        storage_stats['TB'] = storage_stats['AVERAGE_DATABASE_BYTES'] / (1024**4)
        
        # === MÉTRICAS RESUMEN ===
        total_tb = storage_stats['TB'].sum()
        unique_dbs = storage_stats['DATABASE_NAME'].nunique()
        
        col_s1, col_s2, col_s3, col_s4 = st.columns(4)
        with col_s1:
            st.metric("Total TB", f"{total_tb:.2f}")
        with col_s2:
            st.metric("DBs Únicas", unique_dbs)
        with col_s3:
            avg_tb_daily = total_tb / days if days > 0 else 0
            st.metric("Promedio TB/día", f"{avg_tb_daily:.2f}")
        with col_s4:
            st.info("💡 Storage billing basado en promedio mensual")
        
        st.markdown("---")
        
        st.subheader("Almacenamiento por Base de Datos")
        recent_storage = storage_stats.drop_duplicates('DATABASE_NAME', keep='first')
        # Storage se cobra mensualmente por TB, usando precio estándar aproximado
        # Snowflake cobra aproximadamente $40 USD por TB/mes (varía por región)
        storage_price_per_tb_month = 40.0  # Precio aproximado USD por TB por mes
        recent_storage_display = recent_storage.copy()
        # Calcular costo mensual estimado
        recent_storage_display['COST_USD_MONTHLY'] = recent_storage_display['TB'] * storage_price_per_tb_month
        # Calcular costo proporcional por días evaluados
        recent_storage_display['COST_USD'] = recent_storage_display['COST_USD_MONTHLY'] * (days / 30.0)
        
        # Ordenar por costo USD de mayor a menor
        recent_storage_display = recent_storage_display.sort_values('COST_USD', ascending=False)
        
        # Gráfica de barras para almacenamiento (primero)
        st.subheader("📊 Consumo de Almacenamiento por Base de Datos")
        if PLOTLY_AVAILABLE:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=recent_storage_display['DATABASE_NAME'],
                y=recent_storage_display['COST_USD'],
                text=[f"${val:.2f}" for val in recent_storage_display['COST_USD']],
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>' +
                            'Costo USD: $%{y:.2f}<br>' +
                            'TB: %{customdata:.2f}<extra></extra>',
                customdata=recent_storage_display['TB'],
                marker_color='steelblue'
            ))
            fig.update_layout(
                xaxis_title="Base de Datos",
                yaxis_title="Costo USD",
                height=450,
                title="Distribución de Almacenamiento por Base de Datos (Ordenado por Costo)",
                xaxis={'categoryorder': 'total descending'},
                margin=dict(l=20, r=20, t=60, b=80),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Fallback a bar chart básico
            st.bar_chart(recent_storage_display.set_index('DATABASE_NAME')['COST_USD'])
        
        cols_order = ['DATABASE_NAME', 'TB', 'COST_USD', 'USAGE_DATE']
        st.dataframe(recent_storage_display[cols_order], use_container_width=True)
        st.caption("💡 Nota: Storage se cobra mensualmente (~$40 USD/TB/mes). El costo mostrado es proporcional al período evaluado.")
        
        # Tendencia temporal
        storage_trend = storage_stats.groupby('USAGE_DATE')['TB'].sum().reset_index()
        storage_trend['USAGE_DATE'] = pd.to_datetime(storage_trend['USAGE_DATE'])
        st.line_chart(storage_trend.set_index('USAGE_DATE'))
        
        with st.expander("📥 Exportar"):
            csv = storage_stats.to_csv(index=False)
            st.download_button("CSV", csv, "storage_stats.csv", "text/csv")
    else:
        st.info("No hay datos de storage disponibles")
    
    # === MOSTRAR QUERY SQL ===
    if storage_query:
        display_sql_queries(storage_query, "🔍 Query SQL Ejecutado - Storage")

# ============================================================================
# TAB: DATA TRANSFER
# ============================================================================

with tab_transfer:
    st.header("🔄 Data Transfer - Transferencia de Datos")
    
    transfer_stats, transfer_query = get_data_transfer_stats(start_date, end_date)
    if not transfer_stats.empty:
        # Convertir a GB
        transfer_stats['GB'] = transfer_stats['BYTES_TRANSFERRED'] / (1024**3)
        transfer_stats['TB'] = transfer_stats['GB'] / 1024
        
        # === MÉTRICAS RESUMEN ===
        total_gb = transfer_stats['GB'].sum()
        total_tb = transfer_stats['TB'].sum()
        unique_types = transfer_stats['TRANSFER_TYPE'].nunique()
        
        col_t1, col_t2, col_t3, col_t4 = st.columns(4)
        with col_t1:
            st.metric("Total GB", f"{total_gb:.2f}")
        with col_t2:
            st.metric("Total TB", f"{total_tb:.3f}")
        with col_t3:
            st.metric("Tipos Únicos", unique_types)
        with col_t4:
            avg_gb_daily = total_gb / days if days > 0 else 0
            st.metric("Promedio GB/día", f"{avg_gb_daily:.2f}")
        
        st.markdown("---")
        
        st.subheader("Transferencias por Tipo")
        type_summary = transfer_stats.groupby('TRANSFER_TYPE')['GB'].sum().reset_index()
        # Data Transfer se cobra diferente según tipo: Egress tiene costo, Ingress es generalmente gratis
        # Precio aproximado: $0.09 USD por GB para egress (varía por región y cloud provider)
        transfer_price_per_gb = 0.09  # Precio aproximado USD por GB para egress
        type_summary_display = type_summary.copy()
        # Calcular costo estimado (principalmente para EGRESS)
        type_summary_display['COST_USD'] = type_summary_display.apply(
            lambda row: row['GB'] * transfer_price_per_gb if row['TRANSFER_TYPE'] == 'EGRESS' else 0.0,
            axis=1
        )
        # Gráfica de pastel para transferencias por tipo (primero)
        create_pie_chart(
            type_summary_display,
            'TRANSFER_TYPE',
            'GB',
            'COST_USD',
            title="Distribución de Transferencias por Tipo"
        )
        
        cols_order = ['TRANSFER_TYPE', 'GB', 'COST_USD']
        st.dataframe(type_summary_display[cols_order], use_container_width=True)
        st.caption("💡 Nota: EGRESS tiene costo (~$0.09 USD/GB), INGRESS generalmente es gratis. Precios varían por región.")
        
        st.subheader("Transferencias por Región")
        region_summary = transfer_stats.groupby(['SOURCE_REGION', 'TARGET_REGION'])['GB'].sum().reset_index()
        st.dataframe(region_summary, use_container_width=True)
        
        with st.expander("📥 Exportar"):
            csv = transfer_stats.to_csv(index=False)
            st.download_button("CSV", csv, "transfer_stats.csv", "text/csv")
    else:
        st.info("No hay datos de transferencia disponibles")
    
    # === MOSTRAR QUERY SQL ===
    if transfer_query:
        display_sql_queries(transfer_query, "🔍 Query SQL Ejecutado - Data Transfer")

# ============================================================================
# TAB: CORTEX & AI
# ============================================================================

with tab_cortex:
    st.header("🤖 Cortex & AI - Uso de Servicios AI")
    
    cortex_stats, cortex_queries = get_cortex_stats(start_date, end_date)
    if not cortex_stats.empty:
        # === MÉTRICAS RESUMEN ===
        total_credits_cortex = cortex_stats['TOTAL_CREDITS'].sum()
        cost_usd_cortex = total_credits_cortex * credit_price_usd
        avg_daily_cortex = total_credits_cortex / days if days > 0 else 0
        
        col_cx1, col_cx2, col_cx3, col_cx4 = st.columns(4)
        with col_cx1:
            st.metric("Total Créditos", f"{total_credits_cortex:.4f}")
        with col_cx2:
            st.metric("Costo USD", f"${cost_usd_cortex:.2f}")
        with col_cx3:
            st.metric("Promedio Diario", f"{avg_daily_cortex:.4f}")
        with col_cx4:
            st.metric("Invocaciones", f"{cortex_stats['INVOCATIONS'].sum():,}")
        
        st.markdown("---")
        
        st.subheader("Uso por Función y Modelo")
        # Agregar columna de precio en USD
        cortex_stats_display = cortex_stats.copy()
        cortex_stats_display['COST_USD'] = cortex_stats_display['TOTAL_CREDITS'] * credit_price_usd
        # Reordenar columnas para mostrar COST_USD después de TOTAL_CREDITS
        cols_order = [col for col in cortex_stats_display.columns if col != 'COST_USD']
        # Insertar COST_USD después de TOTAL_CREDITS si existe
        if 'TOTAL_CREDITS' in cols_order:
            idx = cols_order.index('TOTAL_CREDITS')
            cols_order.insert(idx + 1, 'COST_USD')
        else:
            cols_order.append('COST_USD')
        cortex_stats_display = cortex_stats_display[cols_order]
        
        # Gráfica de pastel para Cortex & AI (primero)
        # Agrupar por función si existe, sino usar SERVICE_TYPE
        if 'FUNCTION_NAME' in cortex_stats_display.columns:
            cortex_pie_data = cortex_stats_display.groupby('FUNCTION_NAME').agg({
                'TOTAL_CREDITS': 'sum',
                'COST_USD': 'sum'
            }).reset_index()
            create_pie_chart(
                cortex_pie_data,
                'FUNCTION_NAME',
                'TOTAL_CREDITS',
                'COST_USD',
                title="Distribución de Uso Cortex & AI por Función"
            )
        elif 'SERVICE_TYPE' in cortex_stats_display.columns:
            cortex_pie_data = cortex_stats_display.groupby('SERVICE_TYPE').agg({
                'TOTAL_CREDITS': 'sum',
                'COST_USD': 'sum'
            }).reset_index()
            create_pie_chart(
                cortex_pie_data,
                'SERVICE_TYPE',
                'TOTAL_CREDITS',
                'COST_USD',
                title="Distribución de Uso Cortex & AI por Servicio"
            )
        else:
            # Usar todos los datos disponibles
            create_pie_chart(
                cortex_stats_display,
                cortex_stats_display.columns[0],  # Primera columna como label
                'TOTAL_CREDITS',
                'COST_USD',
                title="Distribución de Uso Cortex & AI"
            )
        
        st.dataframe(cortex_stats_display, use_container_width=True)
        
        # Métricas adicionales
        col_cx1, col_cx2 = st.columns(2)
        with col_cx1:
            st.metric("Total Tokens", f"{cortex_stats.get('TOTAL_TOKENS', 0).sum():,}")
        with col_cx2:
            unique_functions = cortex_stats['FUNCTION_NAME'].nunique() if 'FUNCTION_NAME' in cortex_stats.columns else 0
            st.metric("Funciones Únicas", unique_functions)
        
        with st.expander("📥 Exportar"):
            csv = cortex_stats.to_csv(index=False)
            st.download_button("CSV", csv, "cortex_stats.csv", "text/csv")
    else:
        st.info("No hay datos de Cortex AI disponibles")
    
    # === MOSTRAR QUERIES SQL ===
    if cortex_queries:
        display_sql_queries(cortex_queries, "🔍 Queries SQL Ejecutados - Cortex & AI")

# ============================================================================
# TAB: QUERIES
# ============================================================================

with tab_queries:
    st.header("📝 Queries - Análisis de Consultas")
    
    limit_queries = st.slider("Número de queries a mostrar", 10, 100, 20)
    
    queries, queries_sql = get_top_queries(start_date, end_date, limit_queries, warehouses, roles, users)
    if not queries.empty:
        st.subheader(f"Top {limit_queries} Queries por Tiempo")
        
        # Preparar datos para mostrar
        display_df = queries.copy()
        display_df['TOTAL_ELAPSED_TIME_SEC'] = display_df['TOTAL_ELAPSED_TIME'] / 1000  # a segundos
        display_df['BYTES_SCANNED_MB'] = display_df['BYTES_SCANNED'] / (1024**2)
        
        # Calcular créditos y costo USD
        # Usar CREDITS_USED_CLOUD_SERVICES si está disponible, sino calcular estimado
        if 'CREDITS_USED_CLOUD_SERVICES' in display_df.columns:
            display_df['CREDITS_CONSUMED'] = display_df['CREDITS_USED_CLOUD_SERVICES'].fillna(0)
        else:
            # Estimación basada en tiempo (aproximación muy básica)
            display_df['CREDITS_CONSUMED'] = (display_df['TOTAL_ELAPSED_TIME'] / 3600000).fillna(0)  # Créditos estimados
        
        display_df['COST_USD'] = display_df['CREDITS_CONSUMED'] * credit_price_usd
        
        # Ordenar por créditos consumidos (descendente)
        display_df = display_df.sort_values('CREDITS_CONSUMED', ascending=False)
        
        # Seleccionar columnas para mostrar
        display_cols = ['QUERY_ID', 'WAREHOUSE_NAME', 'USER_NAME', 'TOTAL_ELAPSED_TIME_SEC', 
                       'BYTES_SCANNED_MB', 'CREDITS_CONSUMED', 'COST_USD', 'START_TIME']
        # Filtrar solo las columnas que existen
        display_cols = [col for col in display_cols if col in display_df.columns]
        
        # Mostrar tabla con formato numérico
        st.dataframe(
            display_df[display_cols].style.format({
                'CREDITS_CONSUMED': '{:.6f}',
                'COST_USD': '${:.2f}',
                'BYTES_SCANNED_MB': '{:.2f}',
                'TOTAL_ELAPSED_TIME_SEC': '{:.2f}'
            }),
            use_container_width=True
        )
        
        # Detalles de query seleccionada
        query_ids = queries['QUERY_ID'].tolist()
        selected_query = st.selectbox("Ver detalles de query:", query_ids)
        if selected_query:
            query_detail = queries[queries['QUERY_ID'] == selected_query].iloc[0]
            
            with st.expander("Detalles de query"):
                st.code(query_detail['QUERY_TEXT'] if pd.notna(query_detail['QUERY_TEXT']) else "N/A", language="sql")
                
                col_q1, col_q2, col_q3 = st.columns(3)
                with col_q1:
                    st.metric("Tiempo (ms)", f"{query_detail['TOTAL_ELAPSED_TIME']:,}")
                with col_q2:
                    st.metric("Bytes Escaneados", f"{query_detail['BYTES_SCANNED']:,}")
                with col_q3:
                    st.metric("Creds Cloud Svcs", f"{query_detail.get('CREDITS_USED_CLOUD_SERVICES', 0):.4f}")
        
        with st.expander("📥 Exportar"):
            csv = queries.to_csv(index=False)
            st.download_button("CSV", csv, "top_queries.csv", "text/csv")
    else:
        st.info("No hay datos de queries disponibles")
    
    # === MOSTRAR QUERY SQL ===
    if queries_sql:
        display_sql_queries(queries_sql, "🔍 Query SQL Ejecutado - Queries")

# ============================================================================
# TAB: USUARIOS
# ============================================================================

with tab_usuarios:
    st.header("👤 Usuarios - Análisis de Consumo por Usuario")
    
    user_stats, user_query = get_user_stats(start_date, end_date, warehouses, roles, users)
    
    # === MÉTRICAS RESUMEN ===
    total_users = 0
    total_credits_users = 0
    
    if not user_stats.empty:
        total_users = len(user_stats)
        total_credits_users = user_stats['TOTAL_CREDITS'].sum()
    
    cost_usd_users = total_credits_users * credit_price_usd
    avg_daily_users = total_credits_users / days if days > 0 else 0
    
    col_u1, col_u2, col_u3, col_u4 = st.columns(4)
    with col_u1:
        st.metric("Usuarios Activos", total_users, help="Usuarios con actividad en el período")
    with col_u2:
        st.metric("Total Créditos", f"{total_credits_users:.4f}")
    with col_u3:
        st.metric("Costo USD", f"${cost_usd_users:.2f}")
    with col_u4:
        st.metric("Promedio Diario", f"{avg_daily_users:.4f}")
    
    st.markdown("---")
    
    # === USUARIOS ===
    if not user_stats.empty:
        st.markdown("")  # Espacio adicional para mejor separación visual
        # Calcular porcentajes
        user_stats['PERCENTAGE'] = (user_stats['TOTAL_CREDITS'] / total_credits_users * 100).round(2)
        user_stats['COST_USD'] = user_stats['TOTAL_CREDITS'] * credit_price_usd
        user_stats['AVG_TIME_SEC'] = (user_stats['TOTAL_TIME_MS'] / user_stats['QUERY_COUNT'] / 1000).round(2)
        
        # Ordenar por créditos de mayor a menor y mostrar top 20
        top_users = user_stats.sort_values('TOTAL_CREDITS', ascending=False).head(20)
        
        # Gráfica de barras para usuarios (primero)
        st.subheader("📊 Consumo por Usuario (Top 20)")
        if PLOTLY_AVAILABLE:
            # Preparar datos para el hover
            hover_data = [
                [f"${row['COST_USD']:.2f}", f"{row['PERCENTAGE']:.2f}%"]
                for _, row in top_users.iterrows()
            ]
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=top_users['USER_NAME'],
                y=top_users['TOTAL_CREDITS'],
                text=[f"{row['TOTAL_CREDITS']:.4f} créditos<br>${row['COST_USD']:.2f} USD" 
                      for _, row in top_users.iterrows()],
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>' +
                            'Créditos: %{y:.4f}<br>' +
                            'Costo USD: %{customdata[0]}<br>' +
                            'Porcentaje: %{customdata[1]}<extra></extra>',
                customdata=hover_data,
                marker_color='steelblue'
            ))
            fig.update_layout(
                xaxis_title="Usuario",
                yaxis_title="Créditos Consumidos",
                height=450,
                title="Distribución de Consumo por Usuario (Ordenado por Créditos)",
                xaxis={'categoryorder': 'total descending'},
                margin=dict(l=20, r=20, t=60, b=80),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Fallback a bar chart básico
            st.bar_chart(top_users.set_index('USER_NAME')['TOTAL_CREDITS'])
        
        st.markdown("")  # Espacio antes de la tabla
        
        # Tabla de usuarios
        st.dataframe(
            top_users[['USER_NAME', 'TOTAL_CREDITS', 'COST_USD', 'PERCENTAGE', 'QUERY_COUNT']].style.format({
                'TOTAL_CREDITS': '{:.4f}',
                'COST_USD': '${:.2f}',
                'PERCENTAGE': '{:.2f}%'
            }),
            use_container_width=True
        )
        
        with st.expander("📥 Exportar usuarios"):
            csv = user_stats.to_csv(index=False)
            st.download_button("CSV", csv, "user_stats.csv", "text/csv")
    else:
        st.info("No hay datos de usuarios disponibles")
    
    # === MOSTRAR QUERY SQL ===
    if user_query:
        display_sql_queries(user_query, "🔍 Query SQL Ejecutado - Usuarios")

# ============================================================================
# TAB: ROLES
# ============================================================================

with tab_roles:
    st.header("🔐 Roles - Análisis de Consumo por Rol")
    
    role_stats, role_query = get_role_stats(start_date, end_date, warehouses, roles, users)
    
    # === MÉTRICAS RESUMEN ===
    total_roles = 0
    total_credits_roles = 0
    
    if not role_stats.empty:
        total_roles = len(role_stats)
        total_credits_roles = role_stats['TOTAL_CREDITS'].sum()
    
    cost_usd_roles = total_credits_roles * credit_price_usd
    avg_daily_roles = total_credits_roles / days if days > 0 else 0
    
    col_r1, col_r2, col_r3, col_r4 = st.columns(4)
    with col_r1:
        st.metric("Roles Activos", total_roles, help="Roles con actividad en el período")
    with col_r2:
        st.metric("Total Créditos", f"{total_credits_roles:.4f}")
    with col_r3:
        st.metric("Costo USD", f"${cost_usd_roles:.2f}")
    with col_r4:
        st.metric("Promedio Diario", f"{avg_daily_roles:.4f}")
    
    st.markdown("---")
    
    # === ROLES ===
    if not role_stats.empty:
        st.markdown("")  # Espacio adicional para mejor separación visual
        # Calcular porcentajes
        role_stats['PERCENTAGE'] = (role_stats['TOTAL_CREDITS'] / total_credits_roles * 100).round(2)
        role_stats['COST_USD'] = role_stats['TOTAL_CREDITS'] * credit_price_usd
        role_stats['AVG_TIME_SEC'] = (role_stats['TOTAL_TIME_MS'] / role_stats['QUERY_COUNT'] / 1000).round(2)
        
        # Ordenar por créditos de mayor a menor
        role_stats_sorted = role_stats.sort_values('TOTAL_CREDITS', ascending=False)
        top_roles = role_stats_sorted.head(20)
        
        # Gráfica de pie para roles - CONSUMO TOTAL (primero, en la parte superior)
        st.subheader("📊 Consumo Total por Rol")
        
        # Usar todos los roles ordenados para la gráfica
        roles_for_chart = role_stats_sorted.copy()
        
        # Gráfica de pastel (pie chart) primero
        create_pie_chart(
            roles_for_chart,
            'ROLE_NAME',
            'TOTAL_CREDITS',
            'COST_USD',
            title="Distribución de Consumo por Rol"
        )
        
        st.markdown("---")
        
        # Preparar datos para el hover y texto en barras
        hover_data = [
            [f"${row['COST_USD']:.2f}", f"{row['PERCENTAGE']:.2f}%"]
            for _, row in roles_for_chart.iterrows()
        ]
        
        # Texto simplificado para mostrar en cada barra (más legible)
        bar_text = [
            f"{row['TOTAL_CREDITS']:.4f}<br>${row['COST_USD']:.2f}" 
            for _, row in roles_for_chart.iterrows()
        ]
        
        if PLOTLY_AVAILABLE:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=roles_for_chart['ROLE_NAME'],
                y=roles_for_chart['TOTAL_CREDITS'],
                text=bar_text,
                textposition='outside',
                textfont=dict(size=10),
                hovertemplate='<b>%{x}</b><br>' +
                            'Créditos: %{y:.4f}<br>' +
                            'Costo USD: %{customdata[0]}<br>' +
                            'Porcentaje: %{customdata[1]}<extra></extra>',
                customdata=hover_data,
                marker_color='steelblue',
                name='Créditos Consumidos'
            ))
            fig.update_layout(
                xaxis_title="Rol",
                yaxis_title="Créditos Consumidos",
                height=450,
                title="Consumo Total por Rol (Ordenado de Mayor a Menor)",
                xaxis={'categoryorder': 'total descending'},
                showlegend=False,
                margin=dict(l=20, r=20, t=60, b=80),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Fallback a bar chart básico
            st.bar_chart(roles_for_chart.set_index('ROLE_NAME')['TOTAL_CREDITS'])
        
        st.markdown("---")
        
        st.markdown("")  # Espacio antes de la tabla
        
        # Detalle en tabla (Top 20)
        st.subheader("📋 Detalle - Top 20 Roles")
        st.dataframe(
            top_roles[['ROLE_NAME', 'TOTAL_CREDITS', 'COST_USD', 'PERCENTAGE', 'QUERY_COUNT']].style.format({
                'TOTAL_CREDITS': '{:.4f}',
                'COST_USD': '${:.2f}',
                'PERCENTAGE': '{:.2f}%'
            }),
            use_container_width=True
        )
        
        with st.expander("📥 Exportar roles"):
            csv = role_stats.to_csv(index=False)
            st.download_button("CSV", csv, "role_stats.csv", "text/csv")
    else:
        st.info("No hay datos de roles disponibles")
    
    # === MOSTRAR QUERY SQL ===
    if role_query:
        display_sql_queries(role_query, "🔍 Query SQL Ejecutado - Roles")

# ============================================================================
# TAB: TAGS
# ============================================================================

with tab_tags:
    st.header("🏷️ Tags - Análisis de Costos por Etiquetas")
    
    # Selector de método de tags
    tag_methods = {
        "Object Tags": get_cost_by_object_tag,
        "Query Tags": get_cost_by_query_tag
    }
    
    selected_tag_method = st.selectbox("Seleccionar tipo de etiqueta:", list(tag_methods.keys()))
    
    if selected_tag_method:
        # Cargar datos según método seleccionado
        tag_data, tag_query = tag_methods[selected_tag_method](start_date, end_date)
        
        if not tag_data.empty:
            # Calcular totales y porcentajes
            # La columna de créditos puede tener diferentes nombres según el método
            credits_col = None
            for col in tag_data.columns:
                if 'CREDIT' in col.upper() or 'TOTAL' in col.upper() or col.upper() in ['TAG_VALUE', 'TAG']:
                    # Verificar si es la columna de valores (segunda columna generalmente)
                    if col != tag_data.columns[0]:
                        credits_col = col
                        break
            
            if credits_col is None and len(tag_data.columns) >= 2:
                credits_col = tag_data.columns[1]
            
            if credits_col:
                total_tag_credits = tag_data[credits_col].sum()
                
                # === MÉTRICAS RESUMEN ===
                cost_usd_tags = total_tag_credits * credit_price_usd
                avg_daily_tags = total_tag_credits / days if days > 0 else 0
                
                col_t1, col_t2, col_t3, col_t4 = st.columns(4)
                with col_t1:
                    st.metric("Tags Únicos", len(tag_data))
                with col_t2:
                    st.metric("Total Créditos", f"{total_tag_credits:.4f}")
                with col_t3:
                    st.metric("Costo USD", f"${cost_usd_tags:.2f}")
                with col_t4:
                    st.metric("Promedio Diario", f"{avg_daily_tags:.4f}")
                
                st.markdown("---")
                
                # Calcular porcentajes y costo USD
                tag_data['PERCENTAGE'] = (tag_data[credits_col] / total_tag_credits * 100).round(2)
                tag_data['COST_USD'] = tag_data[credits_col] * credit_price_usd
                
                # Preparar datos para gráfica
                tag_display = tag_data.copy()
                # Renombrar columna de créditos para consistencia
                if credits_col != 'TOTAL_CREDITS':
                    tag_display['TOTAL_CREDITS'] = tag_display[credits_col]
                
                # Gráfica de pastel para tags (primero)
                create_pie_chart(
                    tag_display.head(20),  # Top 20 tags
                    tag_display.columns[0],  # Primera columna es el tag
                    'TOTAL_CREDITS',
                    'COST_USD',
                    title=f"Distribución de Costos por {selected_tag_method}"
                )
                
                # Tabla de tags
                # Mostrar columnas relevantes
                display_cols = [tag_data.columns[0], credits_col, 'COST_USD', 'PERCENTAGE']
                st.dataframe(tag_data[display_cols], use_container_width=True)
                
                # Exportar
                with st.expander("📥 Exportar"):
                    csv = tag_data.to_csv(index=False)
                    filename = f"tag_stats_{selected_tag_method.lower().replace(' ', '_')}.csv"
                    st.download_button("CSV", csv, filename, "text/csv")
                
                # Mostrar query SQL
                if tag_query:
                    display_sql_queries(tag_query, f"🔍 Query SQL Ejecutado - {selected_tag_method}")
            else:
                st.warning("No se pudo identificar la columna de créditos en los datos.")
        else:
            st.warning(f"No hay datos disponibles para '{selected_tag_method}'. Verifica que las vistas necesarias estén disponibles.")

# ============================================================================
# TAB: SKUs
# ============================================================================

with tab_skus:
    st.header("📋 SKUs - Mapeo de Servicios")
    st.markdown("**Diccionario de SKUs y cómo se miden** (basado en CreditConsumptionTable.pdf)")
    
    for sku_name, sku_info in SKU_MAPPING.items():
        with st.expander(f"📦 {sku_name}"):
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(f"**SKU ID:** `{sku_info['sku_id']}`")
                st.markdown(f"**Descripción:** {sku_info['description']}")
            with col_b:
                st.markdown(f"**Medición:** {sku_info['measurement']}")
                st.markdown(f"**Columnas:** `{sku_info['credits_column']}`, `{sku_info['date_column']}`")
            st.info(f"💡 {sku_info['extra_info']}")
    
    # Tabla resumen
    st.subheader("📋 Resumen Tabular")
    sku_df = pd.DataFrame(SKU_MAPPING).T
    st.dataframe(sku_df[['sku_id', 'description', 'measurement']], use_container_width=True)

# ============================================================================
# TAB: SETTINGS
# ============================================================================

with tab_settings:
    st.header("⚙️ Settings - Configuración")
    
    col_s1, col_s2 = st.columns(2)
    
    with col_s1:
        st.subheader("Pricing")
        credit_price_input = st.number_input(
            "Precio por crédito (USD)",
            min_value=0.01,
            value=3.0,
            step=0.01
        )
        
        st.subheader("Límites")
        max_queries = st.slider("Max queries a mostrar", 10, 200, 50)
        cache_ttl = st.slider("Cache TTL (segundos)", 60, 3600, 300)
    
    with col_s2:
        st.subheader("Filtros Defaults")
        default_days = st.selectbox("Días por defecto", [7, 30, 60, 90])
        
        st.subheader("Info del Sistema")
        st.json(account_info)

# Footer
st.markdown("---")
st.caption(f"💰 Snowflake Cost Analyzer | Generado en {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Solo lectura a ACCOUNT_USAGE")

