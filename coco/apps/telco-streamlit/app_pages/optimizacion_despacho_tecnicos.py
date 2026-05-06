from app_pages.page_template import render_page

config = {
    "key": "odt",
    "table": "MGG_TELCO.OPERACIONES_Y_MANTENIMIENTO.OPTIMIZACION_DESPACHO_TECNICOS",
    "icon": ":material/engineering:",
    "title": "Despacho Tecnicos",
    "subtitle": "Optimizacion de asignacion y rutas de tecnicos de campo para maximizar productividad y cumplimiento SLA.",
    "cards": [
        "Asigna el tecnico optimo a cada orden minimizando desplazamiento, maximizando resolucion en primera visita y cumpliendo SLA.",
        "Optimiza asignacion por skills, ubicacion, carga de trabajo, prioridad y ventana horaria. Mide eficiencia de ruta y productividad.",
        "Reduce costos de despacho 20-30%. Mejora first-time-fix rate y satisfaccion del cliente con tiempos de atencion menores.",
    ],
    "date_col": "FECHA_CREACION",
    "filter_cols": ["TIPO_TRABAJO", "PRIORIDAD", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS SLA_PCT,
        ROUND(SUM(CASE WHEN RESUELTO_PRIMERA_VISITA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS FTF_PCT,
        ROUND(AVG(TIEMPO_TOTAL_MIN)/60, 1) AS TIEMPO_H,
        ROUND(AVG(TIEMPO_DESPLAZAMIENTO_MIN), 0) AS DESPLAZ_MIN,
        ROUND(AVG(SATISFACCION_CLIENTE), 1) AS CSAT,
        ROUND(AVG(KM_DESPLAZAMIENTO), 1) AS KM_PROM,
        ROUND(AVG(EFICIENCIA_RUTA)*100, 1) AS EFIC_RUTA,
        COUNT(*) AS ORDENES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["SLA", "First-time-fix", "Tiempo total (h)", "Desplazamiento (min)", "CSAT", "KM prom.", "Efic. ruta", "Ordenes"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}", "{:.0f}", "{:.1f}", "{:.1f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CREACION) AS MES,
        ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS SLA,
        ROUND(SUM(CASE WHEN RESUELTO_PRIMERA_VISITA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS FTF
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["SLA (%)", "First-time-fix (%)"],
    "treemap_query": """SELECT TIPO_TRABAJO, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Ordenes por tipo de trabajo"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SATISFACCION_CLIENTE), 1) AS CSAT
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "CSAT por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "CSAT", "caption": "Tamano: ordenes | Color: satisfaccion"},
    "diagnostics": None,
    "simulator": None,
}

render_page(config)
