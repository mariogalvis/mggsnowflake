from app_pages.page_template import render_page

config = {
    "key": "orea",
    "table": "MGG_SEGUROS.REASEGUROS.OPTIMIZACION_REASEGURO",
    "icon": ":material/swap_horiz:",
    "title": "Optimizacion Reaseguro",
    "subtitle": "Evaluacion de estructura de reaseguro por ramo y reaseguradora. Eficiencia, costo de transferencia y cobertura PML.",
    "cards": [
        "Optimiza el costo de transferencia de riesgo. Asegura cobertura adecuada del PML sin pagar de mas por capacidad no utilizada.",
        "Analiza contratos de reaseguro: tipo, retencion, cesion, comisiones, siniestralidad cedida y resultado tecnico. Detecta concentracion excesiva en una reaseguradora.",
        "Reaseguro eficiente libera capital para crecimiento. Reducir costo de transferencia 1pp equivale a millones en resultado tecnico adicional.",
    ],
    "date_col": "FECHA_INICIO",
    "filter_cols": ["TIPO_CONTRATO", "REASEGURADORA", "RAMO", "ESTADO"],
    "kpi_query": """SELECT
        ROUND(AVG(RETENCION_PCT)*100, 1) AS RETENCION,
        ROUND(AVG(COSTO_TRANSFERENCIA_PCT)*100, 1) AS COSTO_TRANSF,
        ROUND(AVG(RESULTADO_TECNICO_PCT)*100, 1) AS RESULTADO,
        ROUND(SUM(PRIMA_CEDIDA_COP)/1e9, 1) AS PRIMA_CED_B,
        ROUND(AVG(EFICIENCIA_COBERTURA)*100, 1) AS EFICIENCIA,
        ROUND(AVG(CONCENTRACION_REASEGURADORA)*100, 1) AS CONCENTRACION,
        ROUND(AVG(RATIO_SINIESTRALIDAD_CEDIDA)*100, 1) AS SINIESTRALIDAD,
        COUNT(*) AS CONTRATOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Retencion", "Costo transferencia", "Resultado tecnico", "Prima cedida", "Eficiencia", "Concentracion", "Siniestralidad ced.", "Contratos"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_INICIO) AS MES,
        ROUND(AVG(RESULTADO_TECNICO_PCT)*100, 1) AS RESULTADO,
        ROUND(AVG(COSTO_TRANSFERENCIA_PCT)*100, 1) AS COSTO
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Resultado tecnico (%)", "Costo transferencia (%)"],
    "treemap_query": """SELECT REASEGURADORA, ROUND(SUM(PRIMA_CEDIDA_COP)/1e6,0) AS PRIMA_MM FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Prima cedida por reaseguradora (M COP)"},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
