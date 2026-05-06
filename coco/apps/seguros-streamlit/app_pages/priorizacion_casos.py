from app_pages.page_template import render_page

config = {
    "key": "pc",
    "table": "MGG_SEGUROS.SINIESTROS.PRIORIZACION_CASOS",
    "icon": ":material/sort:",
    "title": "Priorizacion de Casos",
    "subtitle": "Asignacion inteligente de prioridad segun monto, complejidad, score fraude y perfil del cliente.",
    "cards": [
        "Garantiza que los siniestros mas criticos se atienden primero. Reduce horas sin asignar y mejora SLA en casos de alto impacto.",
        "Score de prioridad compuesto por monto reclamado, score de fraude, complejidad, presencia de victimas, impacto mediatico y valor del cliente (primas/antiguedad).",
        "Clientes VIP atendidos con prioridad mejoran retencion. Deteccion temprana de fraude reduce pagos indebidos. Cumplimiento de SLA por prioridad optimiza recursos.",
    ],
    "date_col": "FECHA_REPORTE",
    "filter_cols": ["TIPO_SINIESTRO", "PRIORIDAD", "ESTADO", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_PRIORIDAD), 0) AS SCORE_PRIO,
        ROUND(COUNT(CASE WHEN DENTRO_SLA THEN 1 END)*100.0/COUNT(*), 1) AS SLA_PCT,
        ROUND(AVG(HORAS_SIN_ASIGNAR), 1) AS HORAS_SIN_ASIG,
        ROUND(SUM(MONTO_RECLAMADO_COP)/1e9, 1) AS MONTO_B,
        ROUND(AVG(SCORE_FRAUDE)*100, 1) AS FRAUDE_PCT,
        ROUND(AVG(COMPLEJIDAD), 1) AS COMPLEX,
        SUM(CASE WHEN CLIENTE_VIP THEN 1 ELSE 0 END) AS VIP_CASES,
        COUNT(*) AS CASOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score prioridad", "Cumplimiento SLA", "Horas sin asignar", "Monto reclamado", "Score fraude", "Complejidad", "Casos VIP", "Total casos"],
    "kpi_formats": ["{:.0f}", "{:.1f}%", "{:.1f}h", "${:.1f}B", "{:.1f}%", "{:.1f}", "{:,.0f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_REPORTE) AS MES,
        ROUND(AVG(HORAS_SIN_ASIGNAR), 1) AS HORAS,
        ROUND(COUNT(CASE WHEN DENTRO_SLA THEN 1 END)*100.0/COUNT(*), 1) AS SLA
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Horas sin asignar", "SLA (%)"],
    "treemap_query": """SELECT PRIORIDAD, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por prioridad", "colorscale": [[0, "#36B37E"], [0.5, "#FFAB00"], [1, "#DE350B"]]},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_PRIORIDAD),0) AS SCORE FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "SCORE", "title": "Prioridad por ciudad"},
    "diagnostics": None, "simulator": None,
}

render_page(config)
