from app_pages.page_template import render_page

config = {
    "key": "gi",
    "table": "MGG_SEGUROS.DISTRIBUCION_Y_VENTAS.GESTION_INTERMEDIARIOS",
    "icon": ":material/handshake:",
    "title": "Gestion Intermediarios",
    "subtitle": "Monitoreo del desempeno de agentes, brokers y corredores. Produccion, calidad de cartera y riesgo de fuga.",
    "cards": [
        "Identifica intermediarios de alto valor para proteger y de bajo rendimiento para desarrollar o desvincular. Detecta riesgo de fuga de corredores productivos.",
        "Dashboard 360 del intermediario: produccion, renovacion, siniestralidad de cartera, conversion de cotizaciones, NPS, quejas y probabilidad de churn.",
        "80% de la produccion viene del 20% de intermediarios. Retener top performers y desarrollar el siguiente tier tiene impacto directo en crecimiento de primas.",
    ],
    "date_col": "FECHA_CORTE",
    "filter_cols": ["TIPO_INTERMEDIARIO", "ESTADO", "RAMO_PRINCIPAL", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(TASA_RENOVACION)*100, 1) AS RENOVACION,
        ROUND(AVG(CONVERSION_COTIZACIONES)*100, 1) AS CONVERSION,
        ROUND(SUM(PRIMAS_EMITIDAS_COP)/1e9, 1) AS PRIMAS_B,
        ROUND(AVG(RATIO_SINIESTRALIDAD_CARTERA)*100, 1) AS SINIESTRALIDAD,
        ROUND(AVG(NPS_INTERMEDIARIO), 1) AS NPS,
        ROUND(AVG(CRECIMIENTO_YOY_PCT), 1) AS CREC_YOY,
        ROUND(AVG(PROB_CHURN_INTERMEDIARIO)*100, 1) AS CHURN,
        COUNT(*) AS INTERMEDIARIOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Tasa renovacion", "Conversion", "Primas emitidas", "Siniestralidad", "NPS", "Crecimiento YoY", "Riesgo churn", "Intermediarios"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CORTE) AS MES,
        ROUND(AVG(CONVERSION_COTIZACIONES)*100, 1) AS CONVERSION,
        ROUND(AVG(PROB_CHURN_INTERMEDIARIO)*100, 1) AS CHURN
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Conversion (%)", "Riesgo churn (%)"],
    "treemap_query": """SELECT TIPO_INTERMEDIARIO, SUM(POLIZAS_VIGENTES) AS POLIZAS FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Polizas por tipo de intermediario"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(CONVERSION_COTIZACIONES)*100,1) AS CONVERSION FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "CONVERSION", "title": "Conversion por ciudad", "colorscale": ["#DE350B", "#FF8B00", "#36B37E"]},
    "diagnostics": None, "simulator": None,
}

render_page(config)
