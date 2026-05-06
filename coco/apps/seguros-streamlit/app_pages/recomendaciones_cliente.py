from app_pages.page_template import render_page

config = {
    "key": "recl",
    "table": "MGG_SEGUROS.RETENCION_Y_EXPERIENCIA.RECOMENDACIONES_CLIENTE",
    "icon": ":material/recommend:",
    "title": "Recomendaciones Cliente",
    "subtitle": "Acciones proactivas personalizadas para cada cliente: prevencion, servicio, producto y engagement.",
    "cards": [
        "Next-best-action para cada cliente en el momento correcto. Reduce fatigue de comunicacion y maximiza utilidad percibida de cada interaccion.",
        "Motor que combina triggers (renovacion proxima, evento vida, siniestro reciente) con propension y score de fatigue para recomendar accion, canal y timing optimo.",
        "Recomendaciones relevantes aumentan engagement 40-60%. Clientes engaged tienen 2x mayor retencion y 3x mayor propension a cross-sell.",
    ],
    "date_col": "FECHA_RECOMENDACION",
    "filter_cols": ["TIPO_RECOMENDACION", "CANAL_ENTREGA", "RAMO"],
    "kpi_query": """SELECT
        ROUND(AVG(RELEVANCIA_SCORE)*100, 1) AS RELEVANCIA,
        ROUND(COUNT(CASE WHEN ACCION_TOMADA THEN 1 END)*100.0/COUNT(*), 1) AS ACCION_PCT,
        ROUND(COUNT(CASE WHEN RECOMENDACION_VISTA THEN 1 END)*100.0/COUNT(*), 1) AS VISTA_PCT,
        ROUND(SUM(VALOR_POTENCIAL_COP)/1e9, 1) AS VALOR_B,
        ROUND(AVG(ENGAGEMENT_CLIENTE)*100, 1) AS ENGAGEMENT,
        ROUND(AVG(FATIGUE_SCORE)*100, 1) AS FATIGUE,
        ROUND(COUNT(CASE WHEN FEEDBACK_POSITIVO THEN 1 END)*100.0/NULLIF(COUNT(CASE WHEN ACCION_TOMADA THEN 1 END),0), 1) AS FEEDBACK_POS,
        COUNT(*) AS RECOMENDACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Relevancia", "Accion tomada", "Recomendacion vista", "Valor potencial", "Engagement", "Score fatigue", "Feedback positivo", "Recomendaciones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_RECOMENDACION) AS MES,
        ROUND(COUNT(CASE WHEN ACCION_TOMADA THEN 1 END)*100.0/COUNT(*), 1) AS ACCION,
        ROUND(AVG(RELEVANCIA_SCORE)*100, 1) AS RELEVANCIA
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Accion tomada (%)", "Relevancia (%)"],
    "treemap_query": """SELECT TIPO_RECOMENDACION, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Tipos de recomendacion"},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
