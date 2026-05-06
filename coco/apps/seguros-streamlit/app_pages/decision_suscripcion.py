from app_pages.page_template import render_page

config = {
    "key": "ds",
    "table": "MGG_SEGUROS.SUSCRIPCION_Y_RIESGO.DECISION_SUSCRIPCION",
    "icon": ":material/gavel:",
    "title": "Decision de Suscripcion",
    "subtitle": "Decision de aprobar, rechazar o condicionar una poliza basada en score de riesgo, exposicion acumulada y reglas de underwriting.",
    "cards": [
        "Automatiza la decision de suscripcion con criterios objetivos. Reduce tiempo de respuesta y garantiza consistencia en decisiones de aceptacion o rechazo.",
        "Motor de reglas que combina score de riesgo, exposicion acumulada por ramo, concentracion de riesgo y capacidad de reaseguro para emitir decision automatica o derivar a revision manual.",
        "Acelera el cierre de negocios de bajo riesgo. Libera suscriptores senior para casos complejos. Reduce override manual y mejora auditabilidad.",
    ],
    "date_col": "FECHA_DECISION",
    "filter_cols": ["RAMO", "DECISION", "TIPO_DECISION", "TIPO_NEGOCIO"],
    "kpi_query": """SELECT
        ROUND(COUNT(CASE WHEN DECISION='Aprobada' THEN 1 END)*100.0/COUNT(*), 1) AS TASA_APROB,
        ROUND(AVG(TIEMPO_DECISION_MIN), 0) AS TIEMPO_MIN,
        ROUND(AVG(SCORE_RIESGO), 0) AS SCORE_AVG,
        ROUND(AVG(CONFIANZA_DECISION)*100, 1) AS CONFIANZA_PCT,
        ROUND(SUM(PRIMA_APROBADA_COP)/1e9, 1) AS PRIMAS_APROB_B,
        ROUND(AVG(FACTOR_RECARGO), 2) AS RECARGO_AVG,
        SUM(CASE WHEN OVERRIDE_MANUAL THEN 1 ELSE 0 END) AS OVERRIDES,
        COUNT(*) AS DECISIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Tasa aprobacion", "Tiempo decision (min)", "Score riesgo prom.", "Confianza decision", "Primas aprobadas", "Factor recargo", "Overrides manuales", "Decisiones"],
    "kpi_formats": ["{:.1f}%", "{:.0f}", "{:.0f}", "{:.1f}%", "${:.1f}B", "{:.2f}x", "{:,.0f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_DECISION) AS MES,
        ROUND(COUNT(CASE WHEN DECISION='Aprobada' THEN 1 END)*100.0/COUNT(*), 1) AS TASA_APROB,
        ROUND(AVG(TIEMPO_DECISION_MIN), 0) AS TIEMPO
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Tasa aprobacion (%)", "Tiempo (min)"],
    "treemap_query": """SELECT DECISION, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion de decisiones", "colorscale": [[0, "#36B37E"], [0.5, "#FFAB00"], [1, "#DE350B"]]},
    "geo_query": None, "geo_config": None, "diagnostics": None,
    "simulator": {
        "title": "Simulador de Decision de Suscripcion",
        "desc": "Predice la decision automatica del motor de underwriting para una solicitud.",
        "features": [
            {"name": "Score Riesgo (0-1000)", "min": 0, "max": 1000, "default": 400, "weight": 0.30},
            {"name": "Suma Asegurada (M COP)", "min": 10, "max": 5000, "default": 500, "weight": 0.20, "step": 50},
            {"name": "Concentracion Riesgo (%)", "min": 0, "max": 100, "default": 20, "weight": 0.20},
            {"name": "Exclusiones Aplicadas", "min": 0, "max": 10, "default": 1, "weight": 0.15},
            {"name": "Factor Recargo", "min": 0.5, "max": 3.0, "default": 1.2, "step": 0.1, "weight": 0.15},
        ],
        "thresholds": [0.4, 0.7],
        "labels": ["Aprobacion Directa", "Revision Manual", "Rechazo Probable"],
    },
}

render_page(config)
