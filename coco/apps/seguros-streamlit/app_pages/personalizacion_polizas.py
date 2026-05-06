from app_pages.page_template import render_page

config = {
    "key": "pp",
    "table": "MGG_SEGUROS.CLIENTE_Y_CRECIMIENTO.PERSONALIZACION_POLIZAS",
    "icon": ":material/tune:",
    "title": "Personalizacion Polizas",
    "subtitle": "Ajuste de coberturas segun perfil y uso real del cliente para maximizar satisfaccion y prima.",
    "cards": [
        "Ofrece la cobertura correcta al cliente correcto. Reduce sobre-aseguramiento (que genera cancelacion) y sub-aseguramiento (que genera insatisfaccion post-siniestro).",
        "Motor que analiza uso de coberturas, siniestros historicos y perfil para sugerir ajustes: upgrades, downgrades o cambios de cobertura con propension de aceptacion.",
        "Personalizacion aumenta prima promedio 8-15% con alta aceptacion. Mejora NPS al alinear cobertura con necesidades reales. Reduce cancelacion por precio.",
    ],
    "date_col": "FECHA_PERSONALIZACION",
    "filter_cols": ["RAMO", "TIPO_PERSONALIZACION", "CANAL_PRESENTACION", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_PROPENSION_UPGRADE)*100, 1) AS PROPENSION,
        ROUND(COUNT(CASE WHEN AJUSTE_ACEPTADO THEN 1 END)*100.0/COUNT(*), 1) AS ACEPTACION,
        ROUND(AVG(DIFERENCIA_PRIMA_PCT), 1) AS DIFF_PRIMA,
        ROUND(SUM(VALOR_INCREMENTAL_COP)/1e9, 1) AS VALOR_INCR_B,
        ROUND(AVG(USO_COBERTURAS_PCT)*100, 1) AS USO_COB,
        ROUND(AVG(SATISFACCION_COBERTURA)*100, 1) AS SATISF,
        ROUND(AVG(CONFIANZA_RECOMENDACION)*100, 1) AS CONFIANZA,
        COUNT(*) AS PERSONALIZACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Propension upgrade", "Tasa aceptacion", "Diff. prima", "Valor incremental", "Uso coberturas", "Satisfaccion", "Confianza", "Personalizaciones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_PERSONALIZACION) AS MES,
        ROUND(COUNT(CASE WHEN AJUSTE_ACEPTADO THEN 1 END)*100.0/COUNT(*), 1) AS ACEPTACION,
        ROUND(AVG(DIFERENCIA_PRIMA_PCT), 1) AS DIFF_PRIMA
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Tasa aceptacion (%)", "Diff. prima (%)"],
    "treemap_query": """SELECT TIPO_PERSONALIZACION, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Tipos de personalizacion"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_PROPENSION_UPGRADE)*100,1) AS PROPENSION FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "PROPENSION", "title": "Propension por ciudad"},
    "diagnostics": None, "simulator": None,
}

render_page(config)
