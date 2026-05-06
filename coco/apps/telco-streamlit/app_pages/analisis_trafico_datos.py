from app_pages.page_template import render_page

config = {
    "key": "atd",
    "table": "MGG_TELCO.ANALITICA_DE_RED_Y_CLIENTE.ANALISIS_TRAFICO_DATOS",
    "icon": ":material/data_usage:",
    "title": "Analisis Trafico Datos",
    "subtitle": "Analisis de trafico por aplicacion, franja horaria y tecnologia para optimizacion de red y monetizacion.",
    "cards": [
        "Entiende que aplicaciones consumen la red, cuando y donde para optimizar QoS, planificar capacidad y habilitar monetizacion.",
        "Mide trafico DL/UL por app, share, crecimiento, throughput, latencia, QoE, tasa de abandono y congestion por franja horaria.",
        "Habilita decisiones de zero-rating, priorizacion de trafico, expansion de capacidad y pricing diferenciado por servicio.",
    ],
    "date_col": "FECHA",
    "filter_cols": ["CATEGORIA_APP", "TECNOLOGIA", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(SUM(TRAFICO_DL_GB)/1e3, 1) AS DL_TB,
        ROUND(SUM(TRAFICO_UL_GB)/1e3, 1) AS UL_TB,
        ROUND(AVG(THROUGHPUT_AVG_MBPS), 1) AS THROUGHPUT,
        ROUND(AVG(LATENCIA_AVG_MS), 1) AS LATENCIA,
        ROUND(AVG(QOE_SCORE), 1) AS QOE,
        ROUND(AVG(CRECIMIENTO_MOM_PCT), 1) AS CREC_MOM,
        ROUND(AVG(CONGESTION_INDEX)*100, 1) AS CONGESTION,
        COUNT(*) AS REGISTROS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Download (TB)", "Upload (TB)", "Throughput (Mbps)", "Latencia (ms)", "QoE", "Crecimiento MoM", "Congestion", "Registros"],
    "kpi_formats": ["{:.1f}", "{:.1f}", "{:.1f}", "{:.1f}", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA) AS MES,
        ROUND(SUM(TRAFICO_DL_GB)/1e3, 1) AS DL_TB,
        ROUND(AVG(QOE_SCORE), 1) AS QOE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Download (TB)", "QoE"],
    "treemap_query": """SELECT CATEGORIA_APP, ROUND(SUM(TRAFICO_DL_GB)/1e3, 1) AS TB
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Trafico DL por categoria (TB)"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(CONGESTION_INDEX)*100, 1) AS CONGESTION
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Congestion por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "CONGESTION", "caption": "Tamano: registros | Color: indice congestion"},
    "diagnostics": None,
    "simulator": None,
}

render_page(config)
