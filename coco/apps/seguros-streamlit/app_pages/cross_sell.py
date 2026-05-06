from app_pages.page_template import render_page

config = {
    "key": "cs",
    "table": "MGG_SEGUROS.CLIENTE_Y_CRECIMIENTO.CROSS_SELL_SEGUROS",
    "icon": ":material/shopping_cart:",
    "title": "Cross-Sell Seguros",
    "subtitle": "Identificacion de oportunidades de venta cruzada basada en eventos de vida, propension y relevancia.",
    "cards": [
        "Detecta el momento optimo para ofrecer productos adicionales. Maximiza profundidad de relacion y LTV con ofertas relevantes y no intrusivas.",
        "Motor de recomendacion que analiza productos vigentes, eventos de vida detectados (matrimonio, hijos, compra vivienda) y propension de compra para sugerir el producto mas relevante.",
        "Cross-sell tiene tasa de conversion 3-5x mayor que cold acquisition. Incrementa LTV, reduce churn por mayor vinculacion y diluye costos de servicio.",
    ],
    "date_col": "FECHA_IDENTIFICACION",
    "filter_cols": ["PRODUCTO_ACTUAL", "PRODUCTO_RECOMENDADO", "CANAL_OFERTA", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(PROPENSION_COMPRA)*100, 1) AS PROPENSION,
        ROUND(COUNT(CASE WHEN OFERTA_ACEPTADA THEN 1 END)*100.0/COUNT(*), 1) AS ACEPTACION,
        ROUND(AVG(PRIMA_ESTIMADA_COP)/1e6, 1) AS PRIMA_MM,
        ROUND(SUM(LTV_INCREMENTAL_COP)/1e9, 1) AS LTV_INCR_B,
        ROUND(AVG(ROI_ESTIMADO), 1) AS ROI,
        ROUND(AVG(PROFUNDIDAD_RELACION)*100, 1) AS PROFUNDIDAD,
        ROUND(AVG(SCORE_RELEVANCIA)*100, 1) AS RELEVANCIA,
        COUNT(*) AS OPORTUNIDADES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Propension compra", "Tasa aceptacion", "Prima estimada", "LTV incremental", "ROI estimado", "Prof. relacion", "Score relevancia", "Oportunidades"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:.1f}M", "${:.1f}B", "{:.1f}x", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_IDENTIFICACION) AS MES,
        ROUND(COUNT(CASE WHEN OFERTA_ACEPTADA THEN 1 END)*100.0/COUNT(*), 1) AS ACEPTACION,
        COUNT(*) AS OPORTUNIDADES
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Tasa aceptacion (%)", "Oportunidades"],
    "treemap_query": """SELECT PRODUCTO_RECOMENDADO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Productos recomendados"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(PROPENSION_COMPRA)*100,1) AS PROPENSION FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "PROPENSION", "title": "Propension por ciudad"},
    "diagnostics": None, "simulator": None,
}

render_page(config)
