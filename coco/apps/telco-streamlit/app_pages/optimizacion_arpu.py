from app_pages.page_template import render_page

config = {
    "key": "oar",
    "table": "MGG_TELCO.MONETIZACION_Y_REVENUE.OPTIMIZACION_ARPU",
    "icon": ":material/trending_up:",
    "title": "Optimizacion ARPU",
    "subtitle": "Identificacion de oportunidades de incremento de ARPU por palanca (upgrade, datos, VAS) con medicion de ROI.",
    "cards": [
        "Identifica el potencial de incremento de ARPU por cliente y la mejor palanca (upgrade plan, datos adicionales, VAS).",
        "Modelo que estima ARPU potencial vs actual, propension de aceptacion, costo de accion y ROI esperado por segmento.",
        "Incrementa ARPU 8-15% con acciones dirigidas. Maximiza wallet share y profundiza relacion con clientes de alto potencial.",
    ],
    "date_col": "FECHA_ANALISIS",
    "filter_cols": ["PALANCA_PRINCIPAL", "SEGMENTO", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(ARPU_ACTUAL_COP)/1e3, 1) AS ARPU_ACT_K,
        ROUND(AVG(ARPU_POTENCIAL_COP)/1e3, 1) AS ARPU_POT_K,
        ROUND(AVG(INCREMENTO_PCT), 1) AS INC_PCT,
        ROUND(AVG(PROPENSION_ACEPTACION)*100, 1) AS PROPENSION,
        ROUND(AVG(ROI_ACCION), 1) AS ROI,
        ROUND(SUM(CASE WHEN ACCION_EXITOSA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS EXITO_PCT,
        ROUND(AVG(WALLET_SHARE)*100, 1) AS WALLET,
        COUNT(*) AS REGISTROS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["ARPU actual (K)", "ARPU potencial (K)", "Incremento", "Propension", "ROI", "Exito", "Wallet share", "Registros"],
    "kpi_formats": ["${:.1f}K", "${:.1f}K", "{:.1f}%", "{:.1f}%", "{:.1f}x", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_ANALISIS) AS MES,
        ROUND(AVG(ARPU_ACTUAL_COP)/1e3, 1) AS ARPU_ACTUAL,
        ROUND(AVG(ARPU_POTENCIAL_COP)/1e3, 1) AS ARPU_POTENCIAL
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["ARPU actual (K)", "ARPU potencial (K)"],
    "treemap_query": """SELECT PALANCA_PRINCIPAL, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Registros por palanca de ARPU"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(INCREMENTO_PCT), 1) AS INC
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Incremento ARPU por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "INC", "caption": "Tamano: clientes | Color: % incremento"},
    "diagnostics": None,
    "simulator": None,
}

render_page(config)
