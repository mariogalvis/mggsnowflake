from app_pages.page_template import render_page

config = {
    "key": "ucs",
    "table": "MGG_TELCO.MONETIZACION_Y_REVENUE.UPSELL_CROSS_SELL_TELCO",
    "icon": ":material/shopping_cart:",
    "title": "Upsell & Cross-Sell",
    "subtitle": "Identificacion de oportunidades de venta incremental con propension, relevancia y control de fatigue.",
    "cards": [
        "Identifica productos adicionales o upgrades que maximizan valor para el cliente y revenue para el operador.",
        "Motor de recomendacion que combina propension de compra, relevancia, churn risk, fatigue y profundidad de relacion.",
        "Incrementa productos por cliente en 0.3-0.5 unidades. Aumenta LTV y reduce churn via mayor engagement.",
    ],
    "date_col": "FECHA_IDENTIFICACION",
    "filter_cols": ["TIPO", "CANAL", "PRODUCTO_RECOMENDADO"],
    "kpi_query": """SELECT
        ROUND(SUM(CASE WHEN COMPRA_REALIZADA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS CONV_PCT,
        ROUND(AVG(PROPENSION_COMPRA)*100, 1) AS PROPENSION,
        ROUND(AVG(RELEVANCIA_SCORE)*100, 1) AS RELEVANCIA,
        ROUND(AVG(ARPU_INCREMENTAL_COP)/1e3, 1) AS INC_K,
        ROUND(AVG(ROI_OFERTA), 1) AS ROI,
        ROUND(AVG(FATIGUE_SCORE)*100, 1) AS FATIGUE,
        ROUND(AVG(PROFUNDIDAD_RELACION)*100, 1) AS PROFUNDIDAD,
        COUNT(*) AS OPORTUNIDADES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Conversion", "Propension", "Relevancia", "ARPU increm. (K)", "ROI", "Fatigue", "Profundidad", "Oportunidades"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}K", "{:.1f}x", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_IDENTIFICACION) AS MES,
        ROUND(SUM(CASE WHEN COMPRA_REALIZADA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS CONVERSION,
        ROUND(AVG(RELEVANCIA_SCORE)*100, 1) AS RELEVANCIA
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Conversion (%)", "Relevancia (%)"],
    "treemap_query": """SELECT TIPO, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Oportunidades por tipo"},
    "geo_query": None,
    "geo_config": None,
    "diagnostics": None,
    "simulator": None,
}

render_page(config)
