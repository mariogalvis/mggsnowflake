from app_pages.page_template import render_page

config = {
    "key": "gres",
    "table": "MGG_SEGUROS.FINANZAS_TECNICAS.GESTION_RESERVAS",
    "icon": ":material/savings:",
    "title": "Gestion Reservas",
    "subtitle": "Control de reservas tecnicas por ramo y tipo. Suficiencia, cumplimiento regulatorio y run-off.",
    "cards": [
        "Reservas insuficientes = riesgo regulatorio y sorpresas de resultado. Reservas excesivas = capital inmovilizado. Encuentra el balance optimo con datos.",
        "Monitorea reservas por tipo (IBNR, pendientes, desviacion catastrofe), suficiencia vs regulatorio, run-off y tendencia. Alerta cuando hay riesgo de incumplimiento.",
        "Reservas precisas protegen resultado tecnico y cumplen Superfinanciera. Run-off favorable libera capital para crecimiento o dividendos.",
    ],
    "date_col": "FECHA_CALCULO",
    "filter_cols": ["TIPO_RESERVA", "RAMO", "EVALUACION"],
    "kpi_query": """SELECT
        ROUND(SUM(RESERVA_ACTUAL_COP)/1e9, 1) AS RESERVA_B,
        ROUND(AVG(SUFICIENCIA_RESERVA)*100, 1) AS SUFICIENCIA,
        ROUND(AVG(VARIACION_PCT), 1) AS VARIACION,
        ROUND(AVG(RATIO_RESERVA_PRIMAS)*100, 1) AS RATIO_R_P,
        ROUND(COUNT(CASE WHEN CUMPLE_REGULACION THEN 1 END)*100.0/COUNT(*), 1) AS CUMPLE_REG,
        ROUND(AVG(RUNOFF_PCT), 1) AS RUNOFF,
        ROUND(AVG(CONFIANZA_CALCULO)*100, 1) AS CONFIANZA,
        COUNT(*) AS REGISTROS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Reserva total", "Suficiencia", "Variacion", "Ratio reserva/primas", "Cumple regulacion", "Run-off", "Confianza", "Registros"],
    "kpi_formats": ["${:.1f}B", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CALCULO) AS MES,
        ROUND(SUM(RESERVA_ACTUAL_COP)/1e9, 1) AS RESERVA_B,
        ROUND(AVG(SUFICIENCIA_RESERVA)*100, 1) AS SUFICIENCIA
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Reserva (B COP)", "Suficiencia (%)"],
    "treemap_query": """SELECT TIPO_RESERVA, ROUND(SUM(RESERVA_ACTUAL_COP)/1e9,1) AS RESERVA_B FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Reservas por tipo (B COP)"},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
