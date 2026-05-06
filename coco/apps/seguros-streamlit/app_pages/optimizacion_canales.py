from app_pages.page_template import render_page

config = {
    "key": "oc",
    "table": "MGG_SEGUROS.DISTRIBUCION_Y_VENTAS.OPTIMIZACION_CANALES_VENTA",
    "icon": ":material/store:",
    "title": "Optimizacion Canales",
    "subtitle": "Analisis de efectividad de cada canal de venta por producto y segmento. ROI, conversion y eficiencia operativa.",
    "cards": [
        "Invierte donde genera mayor retorno. Identifica canales sub-optimos para redirigir presupuesto y canales saturados donde escalar generaria rendimientos decrecientes.",
        "Mide conversion, prima promedio, costo de adquisicion, comisiones, siniestralidad y NPS por canal. Genera recomendacion de canal optimo por ramo y segmento.",
        "Optimizar mix de canales puede mejorar conversion 15-25% y reducir CAC 20-30%. Permite pricing diferenciado por canal segun eficiencia.",
    ],
    "date_col": "FECHA_PERIODO",
    "filter_cols": ["CANAL", "RAMO", "SEGMENTO_CLIENTE"],
    "kpi_query": """SELECT
        ROUND(AVG(TASA_CONVERSION)*100, 1) AS CONVERSION,
        ROUND(AVG(ROI_CANAL), 1) AS ROI,
        ROUND(AVG(PRIMA_PROMEDIO_COP)/1e6, 1) AS PRIMA_MM,
        ROUND(AVG(PCT_DIGITAL)*100, 1) AS DIGITAL,
        ROUND(AVG(NPS_CANAL), 1) AS NPS,
        ROUND(AVG(CRECIMIENTO_YOY_PCT), 1) AS CREC_YOY,
        ROUND(AVG(EFICIENCIA_OPERATIVA)*100, 1) AS EFICIENCIA,
        SUM(POLIZAS_EMITIDAS) AS POLIZAS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Conversion", "ROI canal", "Prima promedio", "% Digital", "NPS canal", "Crecimiento YoY", "Eficiencia op.", "Polizas emitidas"],
    "kpi_formats": ["{:.1f}%", "{:.1f}x", "${:.1f}M", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_PERIODO) AS MES,
        ROUND(AVG(TASA_CONVERSION)*100, 1) AS CONVERSION,
        ROUND(AVG(ROI_CANAL), 1) AS ROI
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Conversion (%)", "ROI"],
    "treemap_query": """SELECT CANAL, SUM(POLIZAS_EMITIDAS) AS POLIZAS FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Polizas por canal"},
    "geo_query": None, "geo_config": None, "diagnostics": None,
    "simulator": {
        "title": "Simulador de Efectividad de Canal",
        "desc": "Predice la conversion y ROI esperado de un canal de venta.",
        "features": [
            {"name": "Inversion Marketing (M COP)", "min": 1, "max": 500, "default": 50, "weight": 0.15, "step": 5},
            {"name": "NPS Canal (-100 a 100)", "min": -100, "max": 100, "default": 40, "weight": 0.20},
            {"name": "% Digital del Proceso", "min": 0, "max": 100, "default": 60, "weight": 0.25},
            {"name": "Tiempo Cierre (min)", "min": 5, "max": 120, "default": 30, "weight": -0.20},
            {"name": "Comision Canal (%)", "min": 0, "max": 30, "default": 12, "weight": -0.20},
        ],
        "thresholds": [0.35, 0.65],
        "labels": ["Canal No Rentable", "Canal Aceptable", "Canal Optimo"],
    },
}

render_page(config)
