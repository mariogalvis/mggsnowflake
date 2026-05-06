from app_pages.page_template import render_page

config = {
    "key": "asat",
    "table": "MGG_SEGUROS.RETENCION_Y_EXPERIENCIA.ANALISIS_SATISFACCION",
    "icon": ":material/sentiment_satisfied:",
    "title": "Analisis Satisfaccion",
    "subtitle": "Medicion de experiencia del cliente en cada punto de contacto. NPS, sentimiento y drivers de satisfaccion.",
    "cards": [
        "Entiende que valoran los clientes y donde falla la experiencia. Prioriza mejoras por impacto en renovacion y referidos.",
        "Encuestas por touchpoint midiendo NPS, satisfaccion general, facilidad, claridad, amabilidad y velocidad. Analisis de sentimiento y correlacion con renovacion.",
        "NPS > 50 correlaciona con +15% tasa de renovacion. Clientes promotores generan 2-3 referidos. Detractores activos destruyen valor via redes sociales.",
    ],
    "date_col": "FECHA_ENCUESTA",
    "filter_cols": ["TOUCHPOINT", "RAMO", "CATEGORIA_NPS", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(NPS_SCORE), 0) AS NPS,
        ROUND(AVG(SATISFACCION_GENERAL)*100, 1) AS SATISF,
        ROUND(AVG(FACILIDAD_PROCESO)*100, 1) AS FACILIDAD,
        ROUND(AVG(VELOCIDAD_RESPUESTA)*100, 1) AS VELOCIDAD,
        ROUND(COUNT(CASE WHEN RESOLUCION_PRIMER_CONTACTO THEN 1 END)*100.0/COUNT(*), 1) AS FCR,
        ROUND(AVG(PROB_RENOVACION)*100, 1) AS PROB_RENOV,
        ROUND(AVG(TASA_RESPUESTA_ENCUESTA)*100, 1) AS TASA_RESP,
        COUNT(*) AS ENCUESTAS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["NPS", "Satisfaccion", "Facilidad", "Velocidad", "FCR", "Prob. renovacion", "Tasa respuesta", "Encuestas"],
    "kpi_formats": ["{:.0f}", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_ENCUESTA) AS MES,
        ROUND(AVG(NPS_SCORE), 0) AS NPS,
        ROUND(AVG(SATISFACCION_GENERAL)*100, 1) AS SATISFACCION
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["NPS", "Satisfaccion (%)"],
    "treemap_query": """SELECT TOUCHPOINT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Encuestas por touchpoint"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(NPS_SCORE),0) AS NPS FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "NPS", "title": "NPS por ciudad", "colorscale": ["#DE350B", "#FF8B00", "#36B37E"]},
    "diagnostics": None, "simulator": None,
}

render_page(config)
