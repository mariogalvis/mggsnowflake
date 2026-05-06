from app_pages.page_template import render_page

config = {
    "key": "rcomb",
    "table": "MGG_SEGUROS.FINANZAS_TECNICAS.OPTIMIZACION_RATIO_COMBINADO",
    "icon": ":material/analytics:",
    "title": "Ratio Combinado",
    "subtitle": "Analisis del ratio combinado por ramo y region. Palancas de mejora y cuadrantes de rentabilidad.",
    "cards": [
        "El ratio combinado es EL indicador de rentabilidad tecnica. Descompone en siniestralidad + gastos + comisiones para identificar la palanca mas efectiva de mejora.",
        "Monitorea ratio combinado (meta <100%), resultado tecnico, tendencia trimestral y market share. Clasifica ramos en cuadrantes: rentable/no rentable x crecimiento.",
        "Ratio combinado <95% = negocio altamente rentable. Cada punto de mejora impacta directamente el resultado tecnico y el valor para accionistas.",
    ],
    "date_col": "FECHA_CORTE",
    "filter_cols": ["RAMO", "REGION", "CUADRANTE"],
    "kpi_query": """SELECT
        ROUND(AVG(RATIO_COMBINADO)*100, 1) AS RATIO_COMB,
        ROUND(AVG(RATIO_SINIESTRALIDAD)*100, 1) AS SINIESTRALIDAD,
        ROUND(AVG(RATIO_GASTOS_ADMIN)*100, 1) AS GASTOS,
        ROUND(AVG(RATIO_COMISIONES)*100, 1) AS COMISIONES,
        ROUND(AVG(RESULTADO_TECNICO_PCT)*100, 1) AS RESULTADO,
        ROUND(SUM(PRIMAS_DEVENGADAS_COP)/1e9, 1) AS PRIMAS_B,
        ROUND(AVG(CRECIMIENTO_PRIMAS_YOY_PCT), 1) AS CREC_YOY,
        ROUND(COUNT(CASE WHEN RAMO_RENTABLE THEN 1 END)*100.0/COUNT(*), 1) AS PCT_RENTABLE
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Ratio combinado", "Siniestralidad", "Gastos admin.", "Comisiones", "Resultado tecnico", "Primas devengadas", "Crecimiento YoY", "% Ramos rentables"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.1f}%", "{:.1f}%"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CORTE) AS MES,
        ROUND(AVG(RATIO_COMBINADO)*100, 1) AS RATIO_COMB,
        ROUND(AVG(RATIO_COMBINADO_META)*100, 1) AS META
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Ratio combinado (%)", "Meta (%)"],
    "treemap_query": """SELECT RAMO, ROUND(SUM(PRIMAS_DEVENGADAS_COP)/1e9,1) AS PRIMAS_B FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Primas devengadas por ramo (B COP)"},
    "geo_query": """SELECT REGION AS CIUDAD, SUM(POLIZAS_VIGENTES) AS VOLUMEN, ROUND(AVG(RATIO_COMBINADO)*100,1) AS RATIO FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "RATIO", "title": "Ratio combinado por region", "colorscale": ["#36B37E", "#FF8B00", "#DE350B"]},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Ratio Combinado",
        "desc": "Proyecta el ratio combinado segun palancas de gestion tecnica.",
        "features": [
            {"name": "Siniestralidad (%)", "min": 30, "max": 90, "default": 62, "weight": 0.35},
            {"name": "Gastos Administracion (%)", "min": 5, "max": 25, "default": 12, "weight": 0.25},
            {"name": "Comisiones (%)", "min": 5, "max": 25, "default": 15, "weight": 0.20},
            {"name": "Crecimiento Primas YoY (%)", "min": -10, "max": 40, "default": 8, "weight": -0.10},
            {"name": "Resultado Inversiones (%)", "min": 0, "max": 10, "default": 3, "step": 0.5, "weight": -0.10},
        ],
        "thresholds": [0.4, 0.65],
        "labels": ["Altamente Rentable", "Rentabilidad Aceptable", "No Rentable"],
    },
}

render_page(config)
