from app_pages.page_template import render_page

config = {
    "key": "pcom",
    "table": "MGG_SEGUROS.DISTRIBUCION_Y_VENTAS.PRICING_COMERCIAL",
    "icon": ":material/sell:",
    "title": "Pricing Comercial",
    "subtitle": "Ajuste de oferta comercial segun canal, segmento y posicion competitiva. Gestion de descuentos y probabilidad de cierre.",
    "cards": [
        "Controla la rentabilidad en cada oferta comercial. Evita descuentos excesivos y asegura que cada negocio cerrado cumple umbral de margen tecnico.",
        "Compara prima ofertada vs competencia, aplica elasticidad de precio y calcula probabilidad de cierre. Gestiona niveles de aprobacion de descuento por gobierno corporativo.",
        "Balance entre competitividad y rentabilidad. Cada punto de descuento innecesario es margen perdido. Visibilidad sobre prima perdida por ofertas rechazadas.",
    ],
    "date_col": "FECHA_OFERTA",
    "filter_cols": ["RAMO", "CANAL", "SEGMENTO", "RESULTADO"],
    "kpi_query": """SELECT
        ROUND(AVG(MARGEN_TECNICO_PCT)*100, 1) AS MARGEN,
        ROUND(AVG(PROB_CIERRE)*100, 1) AS PROB_CIERRE,
        ROUND(AVG(DESCUENTO_COMERCIAL_PCT), 1) AS DESCUENTO,
        ROUND(AVG(POSICION_VS_COMPETENCIA_PCT), 1) AS VS_COMP,
        ROUND(SUM(VALOR_PRIMA_PERDIDA_COP)/1e9, 1) AS PRIMA_PERD_B,
        ROUND(AVG(ELASTICIDAD_PRECIO), 2) AS ELASTICIDAD,
        ROUND(COUNT(CASE WHEN RESULTADO='Ganada' THEN 1 END)*100.0/COUNT(*), 1) AS WIN_RATE,
        COUNT(*) AS OFERTAS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Margen tecnico", "Prob. cierre", "Descuento prom.", "vs Competencia", "Prima perdida", "Elasticidad", "Win rate", "Ofertas"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.2f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_OFERTA) AS MES,
        ROUND(AVG(MARGEN_TECNICO_PCT)*100, 1) AS MARGEN,
        ROUND(AVG(PROB_CIERRE)*100, 1) AS CIERRE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Margen tecnico (%)", "Prob. cierre (%)"],
    "treemap_query": """SELECT RAMO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Ofertas por ramo"},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
