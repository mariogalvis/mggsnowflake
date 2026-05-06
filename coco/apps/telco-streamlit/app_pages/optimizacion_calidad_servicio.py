from app_pages.page_template import render_page

config = {
    "key": "ocs",
    "table": "MGG_TELCO.GESTION_DE_RED.OPTIMIZACION_CALIDAD_SERVICIO",
    "icon": ":material/high_quality:",
    "title": "Calidad de Servicio",
    "subtitle": "Monitoreo y optimizacion de KPIs de calidad (QoS/QoE) por servicio, ciudad y segmento para cumplir SLAs.",
    "cards": [
        "Garantiza que la experiencia del usuario cumple los estandares de calidad comprometidos, identificando degradaciones por servicio y region.",
        "Mide velocidad, latencia, jitter, packet loss, MOS de voz y buffering de video. Cruza con NPS y quejas para correlacionar calidad tecnica con percepcion.",
        "Reduce churn asociado a mala calidad de red. Prioriza inversiones en mejora donde mayor impacto en satisfaccion y revenue.",
    ],
    "date_col": "FECHA_MEDICION",
    "filter_cols": ["SERVICIO", "CIUDAD", "SEGMENTO"],
    "kpi_query": """SELECT
        ROUND(AVG(QOE_SCORE), 1) AS QOE_PROM,
        ROUND(AVG(QOS_SCORE), 1) AS QOS_PROM,
        ROUND(AVG(DISPONIBILIDAD)*100, 2) AS DISP_PCT,
        ROUND(AVG(VELOCIDAD_DL_MBPS), 1) AS VEL_DL,
        ROUND(AVG(LATENCIA_MS), 1) AS LATENCIA,
        ROUND(AVG(SLA_COMPLIANCE)*100, 1) AS SLA_PCT,
        ROUND(AVG(NPS_SERVICIO), 1) AS NPS,
        COUNT(*) AS MEDICIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["QoE Score", "QoS Score", "Disponibilidad", "Vel. Download", "Latencia (ms)", "SLA Compliance", "NPS Servicio", "Mediciones"],
    "kpi_formats": ["{:.1f}", "{:.1f}", "{:.2f}%", "{:.1f} Mbps", "{:.1f}", "{:.1f}%", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_MEDICION) AS MES,
        ROUND(AVG(QOE_SCORE), 1) AS QOE,
        ROUND(AVG(NPS_SERVICIO), 1) AS NPS
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["QoE Score", "NPS"],
    "treemap_query": """SELECT SERVICIO, ROUND(AVG(QOE_SCORE), 1) AS QOE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "QoE por servicio"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(QOE_SCORE), 1) AS QOE
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "QoE por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "QOE", "caption": "Tamano: mediciones | Color: QoE score"},
    "diagnostics": None,
    "simulator": None,
}

render_page(config)
