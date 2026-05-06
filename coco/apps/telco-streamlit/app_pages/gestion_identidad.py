from app_pages.page_template import render_page

config = {
    "key": "gid",
    "table": "MGG_TELCO.FRAUDE_Y_SEGURIDAD.GESTION_IDENTIDAD",
    "icon": ":material/fingerprint:",
    "title": "Gestion Identidad",
    "subtitle": "Verificacion de identidad multicanal con biometria, deteccion de documentos falsos y prevencion de suplantacion.",
    "cards": [
        "Previene fraude de suplantacion verificando la identidad en cada evento critico (activacion, cambio SIM, credito) con multiples factores.",
        "Integra validacion de documento, biometria, listas negras, PEP, cambios de dispositivo e IP sospechosa en un score unificado.",
        "Reduce suplantacion de identidad en 90%. Protege al cliente y evita perdidas por fraude de suscripcion.",
    ],
    "date_col": "FECHA_VERIFICACION",
    "filter_cols": ["EVENTO", "METODO_VERIFICACION", "RESULTADO"],
    "kpi_query": """SELECT
        ROUND(AVG(CONFIANZA_IDENTIDAD)*100, 1) AS CONFIANZA_PCT,
        ROUND(SUM(CASE WHEN DOCUMENTO_VALIDO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS DOC_VALIDO_PCT,
        ROUND(SUM(CASE WHEN BIOMETRIA_MATCH THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS BIO_MATCH_PCT,
        ROUND(SUM(CASE WHEN FRAUDE_DETECTADO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS FRAUDE_PCT,
        ROUND(AVG(TIEMPO_VERIFICACION_SEG), 0) AS T_VERIF_SEG,
        ROUND(SUM(CASE WHEN AUTOMATIZADO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS AUTO_PCT,
        ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS SLA_PCT,
        COUNT(*) AS VERIFICACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Confianza", "Doc. valido", "Biometria match", "Fraude detectado", "T. Verif. (seg)", "Automatizado", "SLA", "Verificaciones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.0f}", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_VERIFICACION) AS MES,
        ROUND(AVG(CONFIANZA_IDENTIDAD)*100, 1) AS CONFIANZA,
        ROUND(SUM(CASE WHEN FRAUDE_DETECTADO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS FRAUDE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Confianza (%)", "Fraude (%)"],
    "treemap_query": """SELECT EVENTO, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Verificaciones por evento"},
    "geo_query": None,
    "geo_config": None,
    "diagnostics": None,
    "simulator": None,
}

render_page(config)
