from app_pages.page_template import render_page

config = {
    "key": "ict",
    "table": "MGG_TELCO.ANALITICA_DE_RED_Y_CLIENTE.INSIGHTS_CLIENTE_TELCO",
    "icon": ":material/psychology:",
    "title": "Insights Cliente",
    "subtitle": "Perfil digital 360 del cliente: consumo, engagement, estado emocional, valor estrategico y propension.",
    "cards": [
        "Construye un perfil digital integral de cada cliente para personalizar experiencia, anticipar necesidades y priorizar atencion.",
        "Integra consumo (datos, voz, streaming, gaming, social), engagement, NPS, QoE, churn risk, LTV y valor estrategico.",
        "Habilita hiperpersonalizacion. Mejora NPS al anticipar necesidades y permite priorizar por valor estrategico integral.",
    ],
    "date_col": "FECHA_ANALISIS",
    "filter_cols": ["PERFIL_DIGITAL", "MOMENTO_PEAK_USO", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU_K,
        ROUND(AVG(DATOS_GB_MES), 1) AS DATOS_GB,
        ROUND(AVG(ENGAGEMENT_DIGITAL)*100, 1) AS ENGAGE_PCT,
        ROUND(AVG(CHURN_RISK)*100, 1) AS CHURN_PCT,
        ROUND(AVG(LTV_COP)/1e6, 1) AS LTV_M,
        ROUND(AVG(NPS), 1) AS NPS_PROM,
        ROUND(AVG(UPSELL_PROPENSION)*100, 1) AS UPSELL_PCT,
        ROUND(AVG(VALOR_ESTRATEGICO)*100, 1) AS VALOR_EST
    FROM {table} WHERE {where}""",
    "kpi_labels": ["ARPU (K)", "Datos GB/mes", "Engagement", "Churn risk", "LTV (M)", "NPS", "Upsell prop.", "Valor estrategico"],
    "kpi_formats": ["${:.1f}K", "{:.1f}", "{:.1f}%", "{:.1f}%", "${:.1f}M", "{:.1f}", "{:.1f}%", "{:.1f}%"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_ANALISIS) AS MES,
        ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU,
        ROUND(AVG(ENGAGEMENT_DIGITAL)*100, 1) AS ENGAGEMENT
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["ARPU (K)", "Engagement (%)"],
    "treemap_query": """SELECT PERFIL_DIGITAL, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Clientes por perfil digital"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(LTV_COP)/1e6, 1) AS LTV
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "LTV por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "LTV", "caption": "Tamano: clientes | Color: LTV (M COP)"},
    "diagnostics": None,
    "simulator": None,
}

render_page(config)
