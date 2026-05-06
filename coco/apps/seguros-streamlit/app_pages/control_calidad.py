from app_pages.page_template import render_page

config = {
    "key": "ccal",
    "table": "MGG_SEGUROS.OPERACIONES.CONTROL_CALIDAD_OPERATIVA",
    "icon": ":material/verified:",
    "title": "Control Calidad Operativa",
    "subtitle": "Monitoreo de calidad de procesos operativos. Tasa de error, acciones correctivas y tendencia de mejora.",
    "cards": [
        "Detecta problemas de calidad antes de que impacten al cliente. Monitorea tendencias y efectividad de acciones correctivas para mejora continua.",
        "Revisiones periodicas por proceso midiendo errores detectados, tasa vs meta, causa raiz, impacto financiero y quejas asociadas. Tracking de acciones correctivas.",
        "Errores operativos generan re-trabajo (costo), quejas (churn) y riesgo regulatorio. Cada punto de mejora en calidad reduce costos y mejora NPS.",
    ],
    "date_col": "FECHA_REVISION",
    "filter_cols": ["PROCESO", "AREA", "TIPO_REVISION"],
    "kpi_query": """SELECT
        ROUND(AVG(TASA_ERROR)*100, 1) AS TASA_ERROR,
        ROUND(AVG(TASA_ERROR_META)*100, 1) AS META,
        ROUND(AVG(SCORE_CALIDAD)*100, 1) AS SCORE_CALIDAD,
        ROUND(SUM(IMPACTO_FINANCIERO_COP)/1e6, 0) AS IMPACTO_MM,
        ROUND(AVG(EFECTIVIDAD_CORRECTIVAS)*100, 1) AS EFECTIVIDAD,
        ROUND(COUNT(CASE WHEN CUMPLE_META THEN 1 END)*100.0/COUNT(*), 1) AS CUMPLE_META,
        SUM(ERRORES_DETECTADOS) AS ERRORES_TOTAL,
        COUNT(*) AS REVISIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Tasa error", "Meta", "Score calidad", "Impacto financiero", "Efectividad correct.", "Cumple meta", "Errores detectados", "Revisiones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:,.0f}M", "{:.1f}%", "{:.1f}%", "{:,.0f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_REVISION) AS MES,
        ROUND(AVG(TASA_ERROR)*100, 1) AS ERROR,
        ROUND(AVG(SCORE_CALIDAD)*100, 1) AS CALIDAD
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Tasa error (%)", "Score calidad (%)"],
    "treemap_query": """SELECT PROCESO, SUM(ERRORES_DETECTADOS) AS ERRORES FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Errores por proceso", "colorscale": [[0, "#E8F5E9"], [0.5, "#FF8B00"], [1, "#DE350B"]]},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
