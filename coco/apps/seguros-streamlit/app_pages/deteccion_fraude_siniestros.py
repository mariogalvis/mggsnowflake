from app_pages.page_template import render_page

config = {
    "key": "dfs",
    "table": "MGG_SEGUROS.SINIESTROS.DETECCION_FRAUDE_SINIESTROS",
    "icon": ":material/warning:",
    "title": "Deteccion Fraude Siniestros",
    "subtitle": "Identificacion de reclamaciones potencialmente fraudulentas usando ML. Score de fraude, indicadores y acciones de investigacion.",
    "cards": [
        "Detecta fraude antes de pagar. Reduce perdidas por fraude y prioriza investigacion en casos con mayor probabilidad y mayor monto.",
        "Modelo ML que evalua cada siniestro: dias desde inicio poliza, monto vs cobertura, historial del reclamante, redes sospechosas e inconsistencias documentales.",
        "Cada dolar detectado en fraude es ahorro directo en siniestralidad. Mejora el ratio combinado y la rentabilidad tecnica del portafolio.",
    ],
    "date_col": "FECHA_DETECCION",
    "filter_cols": ["RAMO", "ESTADO_CASO", "TIPO_FRAUDE_SOSPECHADO"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE_FRAUDE_PCT,
        ROUND(COUNT(CASE WHEN FRAUDE_CONFIRMADO THEN 1 END)*100.0/NULLIF(COUNT(*),0), 1) AS TASA_CONFIRMACION,
        ROUND(COUNT(CASE WHEN FALSO_POSITIVO THEN 1 END)*100.0/NULLIF(COUNT(*),0), 1) AS FALSOS_POSITIVOS,
        ROUND(SUM(AHORRO_ESTIMADO_COP)/1e9, 1) AS AHORRO_B,
        ROUND(SUM(MONTO_RECUPERADO_COP)/1e6, 0) AS RECUPERADO_MM,
        ROUND(AVG(CONFIANZA_MODELO)*100, 1) AS CONFIANZA,
        ROUND(AVG(DIAS_INVESTIGACION), 0) AS DIAS_INV,
        COUNT(*) AS CASOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score fraude prom.", "Tasa confirmacion", "Falsos positivos", "Ahorro estimado", "Monto recuperado", "Confianza modelo", "Dias investigacion", "Casos"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}B", "${:,.0f}M", "{:.1f}%", "{:.0f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_DETECCION) AS MES,
        COUNT(*) AS CASOS,
        ROUND(SUM(AHORRO_ESTIMADO_COP)/1e6, 0) AS AHORRO_MM
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Casos detectados", "Ahorro (M COP)"],
    "treemap_query": """SELECT TIPO_FRAUDE_SOSPECHADO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Tipos de fraude detectado", "colorscale": [[0, "#FFF3E0"], [0.5, "#FF8B00"], [1, "#DE350B"]]},
    "geo_query": None, "geo_config": None, "diagnostics": None,
    "simulator": {
        "title": "Simulador de Score de Fraude",
        "desc": "Evalua la probabilidad de que un siniestro sea fraudulento.",
        "features": [
            {"name": "Dias desde inicio poliza", "min": 1, "max": 365, "default": 90, "weight": -0.15},
            {"name": "Monto Reclamado (M COP)", "min": 1, "max": 500, "default": 30, "weight": 0.20, "step": 5},
            {"name": "Siniestros Previos Cliente", "min": 0, "max": 10, "default": 1, "weight": 0.25},
            {"name": "Personas en Red Sospechosa", "min": 0, "max": 20, "default": 0, "weight": 0.25},
            {"name": "Inconsistencias Detectadas", "min": 0, "max": 10, "default": 1, "weight": 0.15},
        ],
        "thresholds": [0.3, 0.7],
        "labels": ["Fraude Improbable", "Requiere Revision", "Alta Probabilidad Fraude"],
    },
}

render_page(config)
