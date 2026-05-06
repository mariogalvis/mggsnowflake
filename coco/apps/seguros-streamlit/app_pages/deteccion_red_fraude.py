from app_pages.page_template import render_page

config = {
    "key": "drf",
    "table": "MGG_SEGUROS.FRAUDE_EN_SEGUROS.DETECCION_RED_FRAUDE",
    "icon": ":material/hub:",
    "title": "Deteccion Red Fraude",
    "subtitle": "Identificacion de redes organizadas de fraude mediante analisis de grafos y conexiones entre actores.",
    "cards": [
        "Descubre organizaciones criminales que el analisis individual no detecta. Permite desmantelar redes completas en lugar de solo casos individuales.",
        "Analisis de grafos que conecta siniestros, talleres, medicos, abogados y asegurados. Detecta patrones de colusion, actores repetidos y montos anormales en clusters.",
        "Una red de fraude desmantelada puede significar ahorro de miles de millones. Denuncias penales tienen efecto disuasivo. Colaboracion entre aseguradoras multiplica efectividad.",
    ],
    "date_col": "FECHA_DETECCION",
    "filter_cols": ["TIPO_RED", "ESTADO", "METODO_DETECCION"],
    "kpi_query": """SELECT
        ROUND(AVG(ACTORES_IDENTIFICADOS), 1) AS ACTORES,
        ROUND(SUM(MONTO_TOTAL_FRAUDE_COP)/1e9, 1) AS FRAUDE_B,
        ROUND(SUM(MONTO_RECUPERADO_COP)/1e9, 1) AS RECUP_B,
        ROUND(AVG(SCORE_CONFIANZA_RED)*100, 1) AS CONFIANZA,
        ROUND(AVG(ROI_INVESTIGACION), 1) AS ROI,
        ROUND(AVG(DIAS_INVESTIGACION), 0) AS DIAS,
        SUM(CASE WHEN DENUNCIA_PENAL THEN 1 ELSE 0 END) AS DENUNCIAS,
        COUNT(*) AS REDES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Actores prom.", "Fraude total", "Recuperado", "Confianza red", "ROI investigacion", "Dias investigacion", "Denuncias penales", "Redes detectadas"],
    "kpi_formats": ["{:.1f}", "${:.1f}B", "${:.1f}B", "{:.1f}%", "{:.1f}x", "{:.0f}", "{:,.0f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_DETECCION) AS MES,
        COUNT(*) AS REDES,
        ROUND(SUM(MONTO_TOTAL_FRAUDE_COP)/1e6, 0) AS FRAUDE_MM
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Redes detectadas", "Fraude (M COP)"],
    "treemap_query": """SELECT TIPO_RED, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Tipos de red de fraude", "colorscale": [[0, "#FFF3E0"], [0.5, "#FF8B00"], [1, "#DE350B"]]},
    "geo_query": None, "geo_config": None, "diagnostics": None, "simulator": None,
}

render_page(config)
