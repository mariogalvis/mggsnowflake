from app_pages.page_template import render_page

config = {
    "key": "mpr",
    "table": "MGG_TELCO.OPERACIONES_Y_MANTENIMIENTO.MANTENIMIENTO_PREDICTIVO",
    "icon": ":material/build:",
    "title": "Mantenimiento Predictivo",
    "subtitle": "Prediccion de fallas en equipos de red para intervenir antes de la caida y reducir MTTR.",
    "cards": [
        "Predice que equipos fallaran en los proximos 30 dias para programar mantenimiento preventivo y evitar caidas no planificadas.",
        "Modelo que analiza sensores (temp, voltaje, CPU, memoria), edad, fallas historicas, horas de operacion y alarmas recientes.",
        "Reduce fallas no planificadas en 40-60%. Extiende vida util de equipos y minimiza impacto en revenue y usuarios.",
    ],
    "date_col": "FECHA_MEDICION",
    "filter_cols": ["TIPO_EQUIPO", "CIUDAD", "ESTADO"],
    "kpi_query": """SELECT
        ROUND(AVG(PROBABILIDAD_FALLA_30D)*100, 1) AS PROB_FALLA,
        ROUND(AVG(HEALTH_SCORE), 1) AS HEALTH_PROM,
        ROUND(SUM(CASE WHEN REQUIERE_INTERVENCION_INMEDIATA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS INTERV_PCT,
        ROUND(AVG(EDAD_EQUIPO_MESES)/12, 1) AS EDAD_ANOS,
        ROUND(AVG(ALARMAS_30D), 1) AS ALARMAS_PROM,
        ROUND(AVG(FALLAS_HISTORICAS), 1) AS FALLAS_HIST,
        ROUND(SUM(REVENUE_ASOCIADO_COP)/1e9, 1) AS REV_RIESGO_B,
        COUNT(*) AS EQUIPOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Prob. falla 30d", "Health score", "Intervencion inm.", "Edad (anos)", "Alarmas 30d", "Fallas hist.", "Revenue riesgo (B)", "Equipos"],
    "kpi_formats": ["{:.1f}%", "{:.1f}", "{:.1f}%", "{:.1f}", "{:.1f}", "{:.1f}", "${:.1f}B", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_MEDICION) AS MES,
        ROUND(AVG(PROBABILIDAD_FALLA_30D)*100, 1) AS PROB_FALLA,
        ROUND(AVG(HEALTH_SCORE), 1) AS HEALTH
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Prob. falla (%)", "Health score"],
    "treemap_query": """SELECT TIPO_EQUIPO, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Equipos por tipo"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(PROBABILIDAD_FALLA_30D)*100, 1) AS PROB
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Probabilidad de falla por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "PROB", "caption": "Tamano: equipos | Color: prob. falla"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Riesgo de Falla",
        "desc": "Estima la probabilidad de falla de un equipo en los proximos 30 dias.",
        "features": [
            {"name": "Health score (0-100)", "min": 0, "max": 100, "default": 75, "weight": -0.25, "desc": "Mayor health = menor riesgo"},
            {"name": "Alarmas ultimos 30d", "min": 0, "max": 20, "default": 3, "weight": 0.25, "desc": "Mas alarmas = mayor riesgo"},
            {"name": "Edad equipo (meses)", "min": 0, "max": 120, "default": 36, "weight": 0.20, "desc": "Mayor edad = mayor riesgo"},
            {"name": "Temperatura (C)", "min": 20, "max": 80, "default": 40, "weight": 0.15, "desc": "Temperatura del equipo"},
            {"name": "CPU utilizacion (%)", "min": 0, "max": 100, "default": 60, "weight": 0.15, "desc": "Carga de procesamiento"},
        ],
        "thresholds": [0.35, 0.65],
        "labels": ["Bajo riesgo", "Riesgo moderado", "Alto riesgo"],
    },
}

render_page(config)
