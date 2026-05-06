from app_pages.page_template import render_page

config = {
    "key": "op",
    "table": "MGG_SEGUROS.SUSCRIPCION_Y_RIESGO.OPTIMIZACION_PORTAFOLIO_RIESGO",
    "icon": ":material/pie_chart:",
    "title": "Optimizacion Portafolio",
    "subtitle": "Analisis del portafolio de polizas para balancear riesgo versus rentabilidad por ramo, region y segmento.",
    "cards": [
        "Identifica concentraciones de riesgo y oportunidades de diversificacion. Permite decisiones estrategicas sobre donde crecer y donde reducir exposicion.",
        "Monitorea siniestralidad, concentracion geografica/sectorial, PML y cobertura de reaseguro. Sugiere rebalanceo cuando se superan umbrales de riesgo.",
        "Mejora el ratio combinado al balancear el mix de negocios. Reduce PML y optimiza el uso del capital de reaseguro.",
    ],
    "date_col": "FECHA_CORTE",
    "filter_cols": ["RAMO", "REGION", "SEGMENTO"],
    "kpi_query": """SELECT
        ROUND(AVG(RATIO_SINIESTRALIDAD)*100, 1) AS SINIESTRALIDAD,
        ROUND(AVG(RATIO_COMBINADO)*100, 1) AS RATIO_COMB,
        ROUND(SUM(PRIMAS_EMITIDAS_COP)/1e9, 1) AS PRIMAS_B,
        ROUND(AVG(INDICE_DIVERSIFICACION)*100, 1) AS DIVERSIF,
        ROUND(AVG(CONCENTRACION_GEOGRAFICA)*100, 1) AS CONC_GEO,
        ROUND(AVG(TASA_RENOVACION)*100, 1) AS RENOV,
        ROUND(AVG(CRECIMIENTO_PRIMAS_YOY_PCT), 1) AS CREC_YOY,
        SUM(POLIZAS_VIGENTES) AS POLIZAS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Siniestralidad", "Ratio combinado", "Primas emitidas", "Diversificacion", "Conc. geografica", "Tasa renovacion", "Crecimiento YoY", "Polizas vigentes"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CORTE) AS MES,
        ROUND(AVG(RATIO_COMBINADO)*100, 1) AS RATIO_COMB,
        ROUND(AVG(RATIO_SINIESTRALIDAD)*100, 1) AS SINIESTRALIDAD
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Ratio combinado (%)", "Siniestralidad (%)"],
    "treemap_query": """SELECT RAMO, ROUND(SUM(PRIMAS_EMITIDAS_COP)/1e9, 1) AS PRIMAS_B FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Primas por ramo (B COP)"},
    "geo_query": """SELECT REGION AS CIUDAD, SUM(POLIZAS_VIGENTES) AS VOLUMEN, ROUND(AVG(RATIO_COMBINADO)*100,1) AS RATIO FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "RATIO", "title": "Ratio combinado por region", "caption": "Tamano: polizas vigentes | Color: rojo = ratio alto"},
    "diagnostics": None, "simulator": None,
}

render_page(config)
