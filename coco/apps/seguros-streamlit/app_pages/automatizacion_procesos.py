from app_pages.page_template import render_page

config = {
    "key": "apro",
    "table": "MGG_SEGUROS.OPERACIONES.AUTOMATIZACION_PROCESOS",
    "icon": ":material/smart_toy:",
    "title": "Automatizacion Procesos",
    "subtitle": "Medicion del nivel de automatizacion de cada proceso operativo. ROI, ahorro y oportunidades de mejora.",
    "cards": [
        "Identifica procesos con mayor potencial de automatizacion por volumen, costo y tasa de error. Prioriza inversiones por ROI y payback.",
        "Mide para cada proceso: volumen mensual, tiempo manual vs automatizado, tasa de error, personas involucradas y ahorro potencial. Calcula ROI e impacto en cliente.",
        "Automatizacion reduce costos operativos 40-70%, errores 80-95% y tiempos de proceso 60-90%. Libera talento para actividades de mayor valor.",
    ],
    "date_col": "FECHA_MEDICION",
    "filter_cols": ["AREA", "ESTADO_AUTOMATIZACION", "TECNOLOGIA"],
    "kpi_query": """SELECT
        ROUND(AVG(PCT_AUTOMATIZACION)*100, 1) AS AUTOMATIZACION,
        ROUND(AVG(REDUCCION_TIEMPO_PCT)*100, 1) AS REDUCCION_TIEMPO,
        ROUND(SUM(AHORRO_MENSUAL_COP)/1e6, 0) AS AHORRO_MM,
        ROUND(AVG(ROI_AUTOMATIZACION), 1) AS ROI,
        ROUND(AVG(TASA_ERROR_MANUAL)*100, 1) AS ERROR_MANUAL,
        ROUND(AVG(TASA_ERROR_AUTOMATIZADO)*100, 1) AS ERROR_AUTO,
        ROUND(AVG(PAYBACK_MESES), 0) AS PAYBACK,
        COUNT(*) AS PROCESOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["% Automatizacion", "Reduccion tiempo", "Ahorro mensual", "ROI", "Error manual", "Error automatizado", "Payback (meses)", "Procesos"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:,.0f}M", "{:.1f}x", "{:.1f}%", "{:.1f}%", "{:.0f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_MEDICION) AS MES,
        ROUND(AVG(PCT_AUTOMATIZACION)*100, 1) AS AUTOMATIZACION,
        ROUND(SUM(AHORRO_MENSUAL_COP)/1e6, 0) AS AHORRO_MM
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["% Automatizacion", "Ahorro (M COP)"],
    "treemap_query": """SELECT AREA, SUM(AHORRO_MENSUAL_COP)/1e6 AS AHORRO_MM FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Ahorro por area (M COP)"},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
