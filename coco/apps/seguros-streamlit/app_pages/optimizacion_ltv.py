from app_pages.page_template import render_page

config = {
    "key": "ltv",
    "table": "MGG_SEGUROS.CLIENTE_Y_CRECIMIENTO.OPTIMIZACION_LTV_CLIENTE",
    "icon": ":material/trending_up:",
    "title": "Optimizacion LTV",
    "subtitle": "Calculo del valor de vida del cliente y acciones para maximizarlo: retencion, cross-sell, upgrade.",
    "cards": [
        "Cuantifica el valor real de cada cliente considerando primas futuras, costos y probabilidad de permanencia. Permite invertir proporcionalmente al valor esperado.",
        "Modelo que integra primas anuales, siniestralidad, costos de adquisicion/servicio/retencion, probabilidad de renovacion y permanencia esperada para calcular LTV actual y potencial.",
        "Decisiones de inversion en cliente basadas en LTV generan mayor ROI. Permite diferenciar servicio y retencion por segmento de valor real.",
    ],
    "date_col": "FECHA_CALCULO",
    "filter_cols": ["SEGMENTO_VALOR", "ACCION_OPTIMIZACION", "TENDENCIA_VALOR", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(LTV_ACTUAL_COP)/1e6, 1) AS LTV_ACTUAL_MM,
        ROUND(AVG(LTV_POTENCIAL_COP)/1e6, 1) AS LTV_POT_MM,
        ROUND(AVG(PROB_RENOVACION)*100, 1) AS PROB_RENOV,
        ROUND(AVG(MARGEN_NETO_ANUAL_COP)/1e6, 1) AS MARGEN_MM,
        ROUND(AVG(ROI_ACCION), 1) AS ROI,
        ROUND(AVG(RATIO_SINIESTRALIDAD)*100, 1) AS SINIESTRALIDAD,
        ROUND(AVG(PERMANENCIA_ESPERADA_ANIOS), 1) AS PERM_ANIOS,
        COUNT(*) AS CLIENTES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["LTV actual", "LTV potencial", "Prob. renovacion", "Margen neto anual", "ROI accion", "Siniestralidad", "Permanencia esp.", "Clientes"],
    "kpi_formats": ["${:.1f}M", "${:.1f}M", "{:.1f}%", "${:.1f}M", "{:.1f}x", "{:.1f}%", "{:.1f} anos", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_CALCULO) AS MES,
        ROUND(AVG(LTV_ACTUAL_COP)/1e6, 1) AS LTV_ACTUAL,
        ROUND(AVG(LTV_POTENCIAL_COP)/1e6, 1) AS LTV_POTENCIAL
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["LTV actual (M)", "LTV potencial (M)"],
    "treemap_query": """SELECT SEGMENTO_VALOR, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Segmentos de valor"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(LTV_ACTUAL_COP)/1e6,1) AS LTV FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "LTV", "title": "LTV por ciudad", "colorscale": ["#D8F3DC", "#2A9D8F", "#264653"]},
    "diagnostics": None, "simulator": None,
}

render_page(config)
