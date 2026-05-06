from app_pages.page_template import render_page

config = {
    "key": "gaf",
    "table": "MGG_SEGUROS.FRAUDE_EN_SEGUROS.GESTION_ALERTAS_FRAUDE",
    "icon": ":material/notifications_active:",
    "title": "Gestion Alertas Fraude",
    "subtitle": "Priorizacion y gestion del ciclo de vida de alertas de fraude: generacion, investigacion, resolucion.",
    "cards": [
        "Gestiona el volumen de alertas sin perder casos criticos. Prioriza por impacto y probabilidad para optimizar el tiempo de investigadores expertos.",
        "Cola inteligente que prioriza alertas por score de fraude, monto involucrado, impacto potencial y carga de trabajo del investigador. Tracking de SLA y efectividad por regla.",
        "Reduce falsos positivos (fatigue de investigadores). Maximiza deteccion verdadera con recursos limitados. Cada alerta resuelta dentro de SLA protege la reputacion.",
    ],
    "date_col": "FECHA_GENERACION",
    "filter_cols": ["TIPO_ALERTA", "SEVERIDAD", "ESTADO", "ORIGEN"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE_AVG,
        ROUND(COUNT(CASE WHEN FRAUDE_CONFIRMADO THEN 1 END)*100.0/COUNT(*), 1) AS CONFIRMACION,
        ROUND(COUNT(CASE WHEN FALSO_POSITIVO THEN 1 END)*100.0/COUNT(*), 1) AS FP_PCT,
        ROUND(SUM(AHORRO_POR_DETECCION_COP)/1e9, 1) AS AHORRO_B,
        ROUND(AVG(DIAS_RESOLUCION), 1) AS DIAS_RES,
        ROUND(COUNT(CASE WHEN DENTRO_SLA THEN 1 END)*100.0/COUNT(*), 1) AS SLA_PCT,
        ROUND(AVG(EFECTIVIDAD_REGLA)*100, 1) AS EFECTIVIDAD,
        COUNT(*) AS ALERTAS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score fraude", "Confirmacion", "Falsos positivos", "Ahorro deteccion", "Dias resolucion", "Dentro SLA", "Efectividad regla", "Alertas"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_GENERACION) AS MES,
        COUNT(*) AS ALERTAS,
        ROUND(COUNT(CASE WHEN FRAUDE_CONFIRMADO THEN 1 END)*100.0/COUNT(*), 1) AS CONFIRMACION
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Alertas generadas", "Confirmacion (%)"],
    "treemap_query": """SELECT TIPO_ALERTA, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Tipos de alerta"},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
