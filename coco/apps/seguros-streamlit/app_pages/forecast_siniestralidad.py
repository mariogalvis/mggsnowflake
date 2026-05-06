from app_pages.page_template import render_page

config = {
    "key": "fsn",
    "table": "MGG_SEGUROS.FINANZAS_TECNICAS.FORECAST_SINIESTRALIDAD",
    "icon": ":material/show_chart:",
    "title": "Forecast Siniestralidad",
    "subtitle": "Proyeccion de siniestralidad ultima por ramo usando metodos actuariales. IBNR, factores de desarrollo y escenarios.",
    "cards": [
        "Anticipa la siniestralidad final antes de que se materialice completamente. Permite pricing proactivo y reservas suficientes desde el primer dia.",
        "Aplica Chain Ladder, Bornhuetter-Ferguson, Cape Cod y Bootstrap. Estima IBNR, factores de desarrollo, intervalos de confianza y alerta sobre desviaciones.",
        "Forecast preciso = reservas suficientes sin sobre-reservar. Evita sorpresas de desarrollo, protege resultado tecnico y cumple requisitos de Superfinanciera.",
    ],
    "date_col": "FECHA_CALCULO",
    "filter_cols": ["RAMO", "METODO_ACTUARIAL", "ESCENARIO"],
    "kpi_query": """SELECT
        ROUND(AVG(SINIESTRALIDAD_PROYECTADA)*100, 1) AS SINIESTRALIDAD_PROY,
        ROUND(AVG(IBNR_PCT)*100, 1) AS IBNR_PCT,
        ROUND(SUM(IBNR_COP)/1e9, 1) AS IBNR_B,
        ROUND(AVG(CONFIANZA_ESTIMACION)*100, 1) AS CONFIANZA,
        ROUND(AVG(MARGEN_ERROR_PCT), 1) AS ERROR_PCT,
        ROUND(AVG(FACTOR_DESARROLLO), 3) AS FACTOR_DEV,
        ROUND(AVG(FRECUENCIA_SINIESTRAL)*100, 1) AS FRECUENCIA,
        COUNT(*) AS FORECASTS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Siniestralidad proy.", "IBNR %", "IBNR total", "Confianza", "Margen error", "Factor desarrollo", "Frecuencia", "Forecasts"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}%", "{:.1f}%", "{:.3f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CALCULO) AS MES,
        ROUND(AVG(SINIESTRALIDAD_PROYECTADA)*100, 1) AS PROYECTADA,
        ROUND(AVG(SINIESTRALIDAD_OBSERVADA)*100, 1) AS OBSERVADA
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Siniestralidad proyectada (%)", "Observada (%)"],
    "treemap_query": """SELECT RAMO, ROUND(SUM(IBNR_COP)/1e6,0) AS IBNR_MM FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "IBNR por ramo (M COP)", "colorscale": [[0, "#E3F2FD"], [0.5, "#FF8B00"], [1, "#DE350B"]]},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
