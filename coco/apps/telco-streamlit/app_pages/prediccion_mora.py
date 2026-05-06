from app_pages.page_template import render_page

config = {
    "key": "pmo",
    "table": "MGG_TELCO.FACTURACION_Y_COBRANZA.PREDICCION_MORA",
    "icon": ":material/credit_card_off:",
    "title": "Prediccion Mora",
    "subtitle": "Modelo predictivo de morosidad para acciones preventivas y reduccion de provision de cartera.",
    "cards": [
        "Anticipa que clientes entraran en mora para activar acciones preventivas antes del vencimiento de factura.",
        "Integra score crediticio, historial de mora, ARPU, eventos de mora recientes, LTV y permanencia para estimar probabilidad de impago.",
        "Reduce provision de cartera con acciones preventivas oportunas. Protege revenue y reduce costo de cobranza reactiva.",
    ],
    "date_col": "FECHA_PREDICCION",
    "filter_cols": ["CATEGORIA_RIESGO", "SEGMENTO", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_MORA)*100, 1) AS SCORE_PROM,
        ROUND(AVG(PROBABILIDAD_PAGO_30D)*100, 1) AS PROB_PAGO,
        ROUND(AVG(SALDO_PENDIENTE_COP)/1e3, 1) AS SALDO_K,
        ROUND(AVG(DIAS_MORA_ACTUAL), 1) AS MORA_DIAS,
        ROUND(AVG(CONFIANZA)*100, 1) AS CONFIANZA,
        ROUND(SUM(PROVISION_ESTIMADA_COP)/1e9, 1) AS PROVISION_B,
        ROUND(AVG(CHURN_RISK)*100, 1) AS CHURN_PCT,
        COUNT(*) AS PREDICCIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score mora", "Prob. pago 30d", "Saldo pend. (K)", "Mora (dias)", "Confianza", "Provision (B)", "Churn risk", "Predicciones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:.1f}K", "{:.1f}", "{:.1f}%", "${:.1f}B", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_PREDICCION) AS MES,
        ROUND(AVG(SCORE_MORA)*100, 1) AS SCORE_MORA,
        ROUND(AVG(PROBABILIDAD_PAGO_30D)*100, 1) AS PROB_PAGO
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Score mora (%)", "Prob. pago 30d (%)"],
    "treemap_query": """SELECT CATEGORIA_RIESGO, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Clientes por categoria de riesgo"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_MORA)*100, 1) AS SCORE
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Score mora por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "SCORE", "caption": "Tamano: clientes | Color: score mora"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Riesgo de Mora",
        "desc": "Estima la probabilidad de que un cliente entre en mora.",
        "features": [
            {"name": "Score crediticio (0-1000)", "min": 0, "max": 1000, "default": 600, "weight": -0.25, "desc": "Mayor score = menor riesgo"},
            {"name": "Eventos mora 12M", "min": 0, "max": 10, "default": 1, "weight": 0.25, "desc": "Historial de impagos"},
            {"name": "Saldo pendiente (K COP)", "min": 0, "max": 500, "default": 80, "weight": 0.20, "desc": "Monto adeudado"},
            {"name": "Meses como cliente", "min": 1, "max": 120, "default": 18, "weight": -0.15, "desc": "Mayor antiguedad = menor riesgo"},
            {"name": "Dias mora actual", "min": 0, "max": 180, "default": 15, "weight": 0.15, "desc": "Dias de atraso actual"},
        ],
        "thresholds": [0.35, 0.65],
        "labels": ["Bajo riesgo", "Riesgo moderado", "Alto riesgo"],
    },
}

render_page(config)
