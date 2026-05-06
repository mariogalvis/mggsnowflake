from app_pages.page_template import render_page

config = {
    "key": "par",
    "table": "MGG_TELCO.FRAUDE_Y_SEGURIDAD.PREVENCION_ABUSO_RED",
    "icon": ":material/block:",
    "title": "Prevencion Abuso Red",
    "subtitle": "Deteccion de uso abusivo de red (tethering excesivo, SMS masivo, bypass fair use) con acciones automaticas.",
    "cards": [
        "Detecta y mitiga abusos de red que degradan la experiencia de otros usuarios y generan costos no monetizados.",
        "Monitorea consumo vs plan, tethering, SMS masivo, ratio de consumo y aplica politicas de fair use automaticamente.",
        "Protege la experiencia de la mayoria de usuarios. Recupera costos de red y reduce congestion en celdas afectadas.",
    ],
    "date_col": "FECHA_DETECCION",
    "filter_cols": ["TIPO_ABUSO", "ESTADO", "ACCION_TOMADA"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_ABUSO)*100, 1) AS SCORE_PROM,
        ROUND(SUM(CASE WHEN ABUSO_CONFIRMADO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS CONF_PCT,
        ROUND(SUM(CASE WHEN FALSO_POSITIVO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS FP_PCT,
        ROUND(AVG(RATIO_CONSUMO_VS_PLAN), 1) AS RATIO_CONSUMO,
        ROUND(SUM(IMPACTO_RED_COP)/1e6, 1) AS IMPACTO_M,
        ROUND(AVG(IMPACTO_OTROS_USUARIOS)*100, 1) AS IMPACT_OTROS_PCT,
        ROUND(SUM(CASE WHEN REINCIDENCIA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS REINCID_PCT,
        COUNT(*) AS INCIDENTES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score abuso", "Confirmados", "Falsos positivos", "Ratio consumo", "Impacto red (M)", "Impacto otros", "Reincidencia", "Incidentes"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}x", "${:.1f}M", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_DETECCION) AS MES,
        COUNT(*) AS INCIDENTES,
        ROUND(AVG(SCORE_ABUSO)*100, 1) AS SCORE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Incidentes", "Score (%)"],
    "treemap_query": """SELECT TIPO_ABUSO, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Incidentes por tipo de abuso"},
    "geo_query": None,
    "geo_config": None,
    "diagnostics": None,
    "simulator": None,
}

render_page(config)
