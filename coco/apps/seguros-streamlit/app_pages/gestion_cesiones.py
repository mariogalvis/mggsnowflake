from app_pages.page_template import render_page

config = {
    "key": "gces",
    "table": "MGG_SEGUROS.REASEGUROS.GESTION_CESIONES",
    "icon": ":material/assignment_turned_in:",
    "title": "Gestion Cesiones",
    "subtitle": "Registro y seguimiento de cesiones individuales de riesgo a reaseguradoras. Eficiencia operativa y SLA.",
    "cards": [
        "Visibilidad operativa de cada cesion. Detecta retrasos, problemas de documentacion y oportunidades para automatizar el proceso de cesion.",
        "Tracking de cada cesion: tipo, reaseguradora, monto cedido, comisiones, siniestros recuperados y dias de procesamiento. Mide cumplimiento de SLA por reaseguradora.",
        "Cesiones rapidas y eficientes mejoran relacion con reaseguradoras. Recuperacion oportuna de siniestros cedidos protege flujo de caja.",
    ],
    "date_col": "FECHA_CESION",
    "filter_cols": ["TIPO_CESION", "REASEGURADORA", "RAMO", "ESTADO"],
    "kpi_query": """SELECT
        ROUND(SUM(PRIMA_CEDIDA_COP)/1e9, 1) AS PRIMA_CED_B,
        ROUND(AVG(PCT_CESION)*100, 1) AS PCT_CESION,
        ROUND(AVG(COMISION_PCT), 1) AS COMISION,
        ROUND(SUM(SINIESTRO_RECUPERADO_COP)/1e9, 1) AS RECUP_B,
        ROUND(AVG(DIAS_PROCESAMIENTO), 0) AS DIAS,
        ROUND(COUNT(CASE WHEN DENTRO_SLA THEN 1 END)*100.0/COUNT(*), 1) AS SLA,
        ROUND(AVG(EFICIENCIA_CESION)*100, 1) AS EFICIENCIA,
        COUNT(*) AS CESIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Prima cedida", "% Cesion prom.", "Comision prom.", "Siniestros recup.", "Dias procesamiento", "Dentro SLA", "Eficiencia", "Cesiones"],
    "kpi_formats": ["${:.1f}B", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.0f}", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CESION) AS MES,
        ROUND(SUM(PRIMA_CEDIDA_COP)/1e6, 0) AS PRIMA_MM,
        ROUND(AVG(DIAS_PROCESAMIENTO), 1) AS DIAS
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Prima cedida (M)", "Dias procesamiento"],
    "treemap_query": """SELECT REASEGURADORA, ROUND(SUM(PRIMA_CEDIDA_COP)/1e6,0) AS PRIMA_MM FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Prima cedida por reaseguradora (M COP)"},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
