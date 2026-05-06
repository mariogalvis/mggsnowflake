from app_pages.page_template import render_page

config = {
    "key": "orn",
    "table": "MGG_SEGUROS.RETENCION_Y_EXPERIENCIA.OPTIMIZACION_RENOVACIONES",
    "icon": ":material/autorenew:",
    "title": "Optimizacion Renovaciones",
    "subtitle": "Maximizacion de la tasa de renovacion mediante acciones de retencion proactivas y ajuste de prima.",
    "cards": [
        "Cada punto de mejora en renovacion impacta directamente el volumen de primas sin costo de adquisicion. Prioriza contactos en polizas con mayor LTV residual.",
        "Modelo que predice probabilidad de renovacion por poliza. Integra cambio de prima, siniestralidad, NPS, antiguedad y canal de contacto para sugerir accion optima.",
        "Renovar cuesta 1/5 de adquirir. La tasa de renovacion es el KPI mas importante para crecimiento sostenible del portafolio de seguros.",
    ],
    "date_col": "FECHA_VENCIMIENTO",
    "filter_cols": ["RAMO", "TIPO_RENOVACION", "CANAL_CONTACTO", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(COUNT(CASE WHEN RENOVADA THEN 1 END)*100.0/COUNT(*), 1) AS TASA_RENOV,
        ROUND(AVG(PROB_RENOVACION)*100, 1) AS PROB_RENOV,
        ROUND(AVG(CAMBIO_PRIMA_PCT), 1) AS CAMBIO_PRIMA,
        ROUND(SUM(PRIMA_RENOVACION_COP)/1e9, 1) AS PRIMAS_B,
        ROUND(AVG(NPS_CLIENTE), 1) AS NPS,
        ROUND(AVG(DIAS_ANTES_CONTACTO), 0) AS DIAS_ANTES,
        ROUND(SUM(LTV_RESIDUAL_COP)/1e9, 1) AS LTV_B,
        COUNT(*) AS RENOVACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Tasa renovacion", "Prob. renovacion", "Cambio prima", "Primas renovacion", "NPS cliente", "Dias antes contacto", "LTV residual", "Renovaciones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}", "{:.0f}", "${:.1f}B", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_VENCIMIENTO) AS MES,
        ROUND(COUNT(CASE WHEN RENOVADA THEN 1 END)*100.0/COUNT(*), 1) AS RENOVACION,
        ROUND(AVG(PROB_RENOVACION)*100, 1) AS PROBABILIDAD
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Tasa renovacion (%)", "Probabilidad (%)"],
    "treemap_query": """SELECT MOTIVO_NO_RENOVACION, COUNT(*) AS N FROM {table} WHERE {where} AND NOT RENOVADA GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Motivos de no renovacion", "colorscale": [[0, "#FFF3E0"], [0.5, "#FF8B00"], [1, "#DE350B"]]},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(COUNT(CASE WHEN RENOVADA THEN 1 END)*100.0/COUNT(*),1) AS RENOVACION FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "RENOVACION", "title": "Tasa renovacion por ciudad", "colorscale": ["#DE350B", "#FF8B00", "#36B37E"]},
    "diagnostics": None, "simulator": None,
}

render_page(config)
