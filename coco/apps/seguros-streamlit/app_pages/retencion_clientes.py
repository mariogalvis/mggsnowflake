from app_pages.page_template import render_page

config = {
    "key": "rc",
    "table": "MGG_SEGUROS.CLIENTE_Y_CRECIMIENTO.RETENCION_CLIENTES",
    "icon": ":material/person_off:",
    "title": "Retencion de Clientes",
    "subtitle": "Deteccion temprana de clientes con riesgo de cancelacion. Score de churn, motivos y acciones de retencion.",
    "cards": [
        "Identifica clientes en riesgo antes de que cancelen. Permite acciones proactivas de retencion con ROI medible por cada peso invertido.",
        "Score de churn que pondera cambio de prima, quejas recientes, NPS, antiguedad y uso de coberturas. Segmenta en bajo/moderado/alto riesgo y sugiere accion especifica.",
        "Retener un cliente cuesta 5-7x menos que adquirir uno nuevo. Cada punto de mejora en retencion impacta directamente el LTV del portafolio.",
    ],
    "date_col": "FECHA_EVALUACION",
    "filter_cols": ["RAMO_PRINCIPAL", "SEGMENTO_CHURN", "CIUDAD", "ESTADO_GESTION"],
    "kpi_query": """SELECT
        ROUND(AVG(CHURN_SCORE)*100, 1) AS CHURN_PCT,
        ROUND(COUNT(CASE WHEN CLIENTE_RETENIDO THEN 1 END)*100.0/COUNT(*), 1) AS TASA_RET,
        ROUND(AVG(PRIMAS_ANUALES_COP)/1e6, 1) AS PRIMA_MM,
        ROUND(SUM(LTV_CLIENTE_COP)/1e9, 1) AS LTV_B,
        ROUND(AVG(ROI_RETENCION), 1) AS ROI_RET,
        ROUND(AVG(CAMBIO_PRIMA_RENOVACION_PCT), 1) AS CAMBIO_PRIMA,
        ROUND(AVG(NPS_CLIENTE), 1) AS NPS,
        COUNT(*) AS EVALUACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Churn score", "Tasa retencion", "Prima anual prom.", "LTV portafolio", "ROI retencion", "Cambio prima", "NPS promedio", "Evaluaciones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:.1f}M", "${:.1f}B", "{:.1f}x", "{:.1f}%", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_EVALUACION) AS MES,
        ROUND(AVG(CHURN_SCORE)*100, 1) AS CHURN,
        ROUND(COUNT(CASE WHEN CLIENTE_RETENIDO THEN 1 END)*100.0/COUNT(*), 1) AS RETENCION
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Churn score (%)", "Retencion (%)"],
    "treemap_query": """SELECT MOTIVO_RIESGO_PRINCIPAL, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Motivos de riesgo de churn", "colorscale": [[0, "#FFF3E0"], [0.5, "#FF8B00"], [1, "#DE350B"]]},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(CHURN_SCORE)*100,1) AS CHURN FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "CHURN", "title": "Riesgo de churn por ciudad", "colorscale": ["#36B37E", "#FF8B00", "#DE350B"]},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Riesgo de Churn",
        "desc": "Predice la probabilidad de que un cliente cancele su poliza.",
        "features": [
            {"name": "Cambio Prima Renovacion (%)", "min": -20, "max": 50, "default": 8, "weight": 0.25},
            {"name": "Siniestros Ultimo Ano", "min": 0, "max": 5, "default": 0, "weight": -0.10},
            {"name": "NPS Cliente (-100 a 100)", "min": -100, "max": 100, "default": 30, "weight": -0.20},
            {"name": "Anos como Cliente", "min": 0, "max": 30, "default": 5, "weight": -0.20},
            {"name": "Quejas ultimos 12 meses", "min": 0, "max": 10, "default": 1, "weight": 0.25},
        ],
        "thresholds": [0.3, 0.6],
        "labels": ["Bajo Riesgo Churn", "Riesgo Moderado", "Alto Riesgo Churn"],
    },
}

render_page(config)
