from app_pages.page_template import render_page

config = {
    "key": "pdoc",
    "table": "MGG_SEGUROS.OPERACIONES.PROCESAMIENTO_DOCUMENTOS",
    "icon": ":material/description:",
    "title": "Procesamiento Documentos",
    "subtitle": "Digitalizacion y extraccion inteligente de datos de documentos. OCR, precision y automatizacion.",
    "cards": [
        "Elimina la captura manual de datos. Reduce errores de transcripcion y acelera procesos de siniestros, suscripcion y servicio al cliente.",
        "Pipeline de IDP (Intelligent Document Processing): recepcion, clasificacion, extraccion de campos, validacion y enrutamiento. Mide precision, confianza y necesidad de revision humana.",
        "Procesamiento automatico reduce costo por documento 80-90%. Tiempo de extraccion pasa de minutos a segundos. Datos estructurados alimentan modelos de ML downstream.",
    ],
    "date_col": "FECHA_RECEPCION",
    "filter_cols": ["TIPO_DOCUMENTO", "CANAL_RECEPCION", "ESTADO", "TECNOLOGIA_PROCESAMIENTO"],
    "kpi_query": """SELECT
        ROUND(AVG(PRECISION_EXTRACCION)*100, 1) AS PRECISION,
        ROUND(AVG(CONFIANZA_EXTRACCION)*100, 1) AS CONFIANZA,
        ROUND(COUNT(CASE WHEN PROCESAMIENTO_AUTOMATICO THEN 1 END)*100.0/COUNT(*), 1) AS AUTO_PCT,
        ROUND(AVG(TIEMPO_PROCESAMIENTO_SEG), 1) AS TIEMPO_SEG,
        ROUND(COUNT(CASE WHEN DENTRO_SLA THEN 1 END)*100.0/COUNT(*), 1) AS SLA_PCT,
        ROUND(AVG(CAMPOS_EXTRAIDOS), 1) AS CAMPOS,
        ROUND(COUNT(CASE WHEN REQUIERE_REVISION_HUMANA THEN 1 END)*100.0/COUNT(*), 1) AS REV_HUMANA,
        COUNT(*) AS DOCUMENTOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Precision", "Confianza", "% Automatico", "Tiempo (seg)", "Dentro SLA", "Campos extraidos", "Rev. humana", "Documentos"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}s", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_RECEPCION) AS MES,
        ROUND(AVG(PRECISION_EXTRACCION)*100, 1) AS PRECISION,
        COUNT(*) AS DOCUMENTOS
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Precision (%)", "Documentos"],
    "treemap_query": """SELECT TIPO_DOCUMENTO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Volumen por tipo de documento"},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
