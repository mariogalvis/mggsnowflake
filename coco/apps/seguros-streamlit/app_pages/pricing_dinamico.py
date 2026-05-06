from app_pages.page_template import render_page

config = {
    "key": "pd",
    "table": "MGG_SEGUROS.SUSCRIPCION_Y_RIESGO.PRICING_DINAMICO",
    "icon": ":material/paid:",
    "title": "Pricing Dinamico",
    "subtitle": "Calculo de prima optima segun riesgo del cliente, costos esperados y condiciones de mercado.",
    "cards": [
        "Elimina el pricing subjetivo. Determina la prima comercial optima balanceando rentabilidad tecnica, probabilidad de cierre y posicion competitiva con datos en tiempo real.",
        "Modelo que integra prima pura (frecuencia x severidad), gastos, margen tecnico y elasticidad precio. Compara contra competencia y sugiere prima optima con probabilidad de cierre.",
        "Maximiza la rentabilidad tecnica sin perder negocios. Reduce descuentos innecesarios y permite reaccionar a movimientos competitivos con precision actuarial.",
    ],
    "date_col": "FECHA_COTIZACION",
    "filter_cols": ["RAMO", "CANAL_VENTA", "TIPO_NEGOCIO", "ESTADO"],
    "kpi_query": """SELECT
        ROUND(AVG(PRIMA_COMERCIAL_COP)/1e6, 1) AS PRIMA_PROM_MM,
        ROUND(AVG(MARGEN_TECNICO_PCT)*100, 1) AS MARGEN_PCT,
        ROUND(AVG(PROBABILIDAD_CIERRE)*100, 1) AS PROB_CIERRE_PCT,
        ROUND(AVG(DIFERENCIA_VS_COMPETENCIA_PCT), 1) AS DIFF_COMP_PCT,
        ROUND(AVG(DESCUENTO_APLICADO_PCT), 1) AS DESC_PCT,
        ROUND(AVG(RATIO_SINIESTRALIDAD_ESPERADA)*100, 1) AS SINIESTRALIDAD_PCT,
        ROUND(AVG(RATIO_COMBINADO_ESPERADO)*100, 1) AS RATIO_COMB_PCT,
        COUNT(*) AS COTIZACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Prima promedio", "Margen tecnico", "Prob. cierre", "Diff vs competencia", "Descuento prom.", "Siniestralidad esp.", "Ratio combinado esp.", "Cotizaciones"],
    "kpi_formats": ["${:.1f}M", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_COTIZACION) AS MES,
        ROUND(AVG(MARGEN_TECNICO_PCT)*100, 1) AS MARGEN,
        ROUND(AVG(PROBABILIDAD_CIERRE)*100, 1) AS CIERRE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Margen tecnico (%)", "Prob. cierre (%)"],
    "treemap_query": """SELECT RAMO, ROUND(SUM(PRIMA_COMERCIAL_COP)/1e9, 1) AS PRIMAS_B
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
    "treemap_config": {"title": "Primas por ramo (B COP)", "colorscale": [[0, "#D8F3DC"], [0.5, "#2A9D8F"], [1, "#264653"]]},
    "geo_query": None,
    "geo_config": None,
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Prima Optima",
        "desc": "Calcula la prima comercial optima balanceando rentabilidad y probabilidad de cierre.",
        "features": [
            {"name": "Score Riesgo (0-1000)", "min": 0, "max": 1000, "default": 500, "weight": 0.25, "desc": "Mayor riesgo requiere mayor prima pura"},
            {"name": "Valor Asegurado (M COP)", "min": 10, "max": 2000, "default": 150, "weight": 0.20, "desc": "Base para calculo de prima pura"},
            {"name": "Descuento Comercial (%)", "min": 0, "max": 40, "default": 10, "weight": -0.20, "desc": "Reduce prima pero afecta margen tecnico"},
            {"name": "Elasticidad Precio", "min": 0.1, "max": 3.0, "default": 1.2, "step": 0.1, "weight": -0.15, "desc": "Sensibilidad del cliente al precio"},
            {"name": "Posicion vs Competencia (%)", "min": -30, "max": 30, "default": 5, "weight": -0.20, "desc": "Negativo = mas barato que competencia"},
        ],
        "thresholds": [0.35, 0.65],
        "labels": ["Margen Insuficiente", "Margen Aceptable", "Margen Optimo"],
    },
}

render_page(config)
