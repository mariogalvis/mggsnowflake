from app_pages.page_template import render_page

config = {
    "key": "dar",
    "table": "MGG_TELCO.GESTION_DE_RED.DETECCION_ANOMALIAS_RED",
    "icon": ":material/warning:",
    "title": "Deteccion Anomalias Red",
    "subtitle": "Identificacion en tiempo real de anomalias en la red mediante ML para reducir tiempo de deteccion y afectacion a usuarios.",
    "cards": [
        "Detecta degradaciones, caidas y comportamientos atipicos en la red antes de que impacten masivamente a los usuarios, reduciendo MTTR.",
        "Modelos de ML y reglas analizan metricas de latencia, throughput, packet loss y correlacionan alertas para identificar anomalias reales vs falsas alarmas.",
        "Reduce tiempo de deteccion de incidentes de horas a minutos. Disminuye falsos positivos y permite priorizar por impacto real en revenue y usuarios.",
    ],
    "date_col": "TIMESTAMP_DETECCION",
    "filter_cols": ["SEVERIDAD", "TECNOLOGIA_AFECTADA", "CIUDAD"],
    "kpi_query": """SELECT
        COUNT(*) AS ANOMALIAS,
        ROUND(AVG(SCORE_ANOMALIA), 1) AS SCORE_PROM,
        ROUND(AVG(USUARIOS_AFECTADOS), 0) AS USUARIOS_PROM,
        ROUND(AVG(DURACION_MIN), 0) AS DURACION_PROM,
        ROUND(SUM(CASE WHEN FALSA_ALARMA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS PCT_FALSA_ALARMA,
        ROUND(AVG(TIEMPO_DETECCION_MIN), 0) AS T_DETECCION_MIN,
        ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS SLA_PCT,
        ROUND(SUM(IMPACTO_REVENUE_COP)/1e6, 1) AS IMPACTO_MM
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Anomalias", "Score prom.", "Usuarios afect.", "Duracion (min)", "Falsas alarmas", "T. Deteccion (min)", "SLA %", "Impacto ($M)"],
    "kpi_formats": ["{:,.0f}", "{:.1f}", "{:,.0f}", "{:.0f}", "{:.1f}%", "{:.0f}", "{:.1f}%", "${:.1f}M"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', TIMESTAMP_DETECCION) AS MES,
        COUNT(*) AS ANOMALIAS,
        ROUND(AVG(SCORE_ANOMALIA), 1) AS SCORE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Anomalias", "Score prom."],
    "treemap_query": """SELECT TIPO_ANOMALIA, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC LIMIT 8""",
    "treemap_config": {"title": "Tipo de anomalia"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_ANOMALIA), 1) AS SCORE
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Anomalias por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "SCORE", "caption": "Tamano: volumen | Color: score anomalia"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Severidad de Anomalia",
        "desc": "Estima la severidad de una anomalia basado en metricas de red.",
        "features": [
            {"name": "Latencia actual (ms)", "min": 0, "max": 500, "default": 80, "weight": 0.25, "desc": "Latencia observada vs baseline"},
            {"name": "Usuarios afectados", "min": 0, "max": 10000, "default": 500, "weight": 0.25, "desc": "Numero de usuarios impactados"},
            {"name": "Packet loss (%)", "min": 0, "max": 20, "default": 2, "step": 0.5, "weight": 0.20, "desc": "Porcentaje de paquetes perdidos"},
            {"name": "Duracion (min)", "min": 0, "max": 480, "default": 30, "weight": 0.15, "desc": "Tiempo de la anomalia"},
            {"name": "Alertas correlacionadas", "min": 0, "max": 20, "default": 3, "weight": 0.15, "desc": "Alertas asociadas al evento"},
        ],
        "thresholds": [0.35, 0.65],
        "labels": ["Baja severidad", "Severidad media", "Alta severidad"],
    },
}

render_page(config)
