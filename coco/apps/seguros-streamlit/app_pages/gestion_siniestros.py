from app_pages.page_template import render_page

config = {
    "key": "gs",
    "table": "MGG_SEGUROS.SINIESTROS.OPTIMIZACION_GESTION_SINIESTROS",
    "icon": ":material/report:",
    "title": "Gestion de Siniestros",
    "subtitle": "Seguimiento del proceso de atencion desde el aviso hasta el pago. Optimizacion de tiempos, SLA y carga de trabajo.",
    "cards": [
        "Visibilidad end-to-end del pipeline de siniestros. Identifica cuellos de botella, casos fuera de SLA y oportunidades de automatizacion para reducir tiempos.",
        "Monitorea cada siniestro desde el aviso hasta el pago: tiempos por etapa, documentos pendientes, carga de ajustadores y cumplimiento de SLA. Genera alertas automaticas.",
        "Reduce tiempo de resolucion mejorando NPS del cliente. Optimiza carga de trabajo de ajustadores. Identifica procesos automatizables para reducir costos operativos.",
    ],
    "date_col": "FECHA_AVISO",
    "filter_cols": ["TIPO_SINIESTRO", "CANAL_AVISO", "COMPLEJIDAD", "ESTADO_ACTUAL"],
    "kpi_query": """SELECT
        ROUND(AVG(DIAS_DESDE_AVISO), 0) AS DIAS_PROM,
        ROUND(COUNT(CASE WHEN DENTRO_SLA THEN 1 END)*100.0/COUNT(*), 1) AS PCT_SLA,
        ROUND(AVG(NPS_SINIESTRO), 1) AS NPS_AVG,
        ROUND(SUM(MONTO_APROBADO_COP)/1e9, 1) AS MONTO_APROB_B,
        ROUND(AVG(DOCUMENTOS_PENDIENTES), 1) AS DOCS_PEND,
        ROUND(AVG(PCT_PROCESO_DIGITAL)*100, 1) AS PCT_DIGITAL,
        ROUND(SUM(AHORRO_EFICIENCIA_COP)/1e6, 0) AS AHORRO_MM,
        COUNT(*) AS SINIESTROS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Dias promedio", "Cumplimiento SLA", "NPS siniestro", "Monto aprobado", "Docs pendientes", "% Digital", "Ahorro eficiencia", "Siniestros"],
    "kpi_formats": ["{:.0f}", "{:.1f}%", "{:.1f}", "${:.1f}B", "{:.1f}", "{:.1f}%", "${:,.0f}M", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_AVISO) AS MES,
        ROUND(AVG(DIAS_DESDE_AVISO), 1) AS DIAS,
        ROUND(COUNT(CASE WHEN DENTRO_SLA THEN 1 END)*100.0/COUNT(*), 1) AS SLA
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Dias resolucion", "SLA (%)"],
    "treemap_query": """SELECT TIPO_SINIESTRO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Volumen por tipo de siniestro"},
    "geo_query": None, "geo_config": None, "diagnostics": None,
    "simulator": {
        "title": "Simulador de Complejidad de Siniestro",
        "desc": "Predice la complejidad y tiempo estimado de resolucion de un siniestro.",
        "features": [
            {"name": "Monto Reclamado (M COP)", "min": 1, "max": 500, "default": 50, "weight": 0.20, "step": 5},
            {"name": "Documentos Pendientes", "min": 0, "max": 10, "default": 2, "weight": 0.25},
            {"name": "Interacciones con Cliente", "min": 0, "max": 20, "default": 3, "weight": 0.15},
            {"name": "Requiere Perito (0=No, 1=Si)", "min": 0, "max": 1, "default": 0, "weight": 0.25},
            {"name": "Carga Trabajo Ajustador", "min": 1, "max": 50, "default": 15, "weight": 0.15},
        ],
        "thresholds": [0.3, 0.6],
        "labels": ["Baja Complejidad", "Complejidad Media", "Alta Complejidad"],
    },
}

render_page(config)
