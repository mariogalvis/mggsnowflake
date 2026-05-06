from app_pages.page_template import render_page

config = {
    "key": "kyc",
    "table": "MGG_BANCA.FRAUDE_Y_SEGURIDAD.KYC_ONBOARDING",
    "icon": ":material/verified_user:",
    "title": "KYC Onboarding",
    "subtitle": "Validacion de identidad en onboarding digital con biometria facial, OCR y listas restrictivas.",
    "cards": [
        "Valida identidad del cliente con biometria facial y OCR de documentos en tiempo real.",
        "Cruza automaticamente contra listas restrictivas, PEP y alertas previas para mitigar riesgo.",
        "Reduce tiempo de onboarding en 70% manteniendo cumplimiento regulatorio completo.",
    ],
    "date_col": "FECHA_SOLICITUD",
    "filter_cols": ["CANAL_ONBOARDING", "RESULTADO_VALIDACION", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(SUM(CASE WHEN CUENTA_CREADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_APROBADOS,
        ROUND(AVG(SCORE_BIOMETRIA_FACIAL), 3) AS SCORE_BIOMETRIA_PROM,
        ROUND(AVG(SCORE_OCR_DOCUMENTO), 3) AS SCORE_OCR_PROM,
        ROUND(SUM(CASE WHEN ES_PEP = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_PEP,
        ROUND(AVG(TIEMPO_PROCESO_MINUTOS), 1) AS TIEMPO_PROCESO_PROM,
        ROUND(AVG(SCORE_RIESGO_KYC), 3) AS SCORE_RIESGO_KYC_PROM,
        ROUND(SUM(CASE WHEN DOCUMENTO_AUTENTICADO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS PCT_DOCS_AUTH,
        COUNT(*) AS TOTAL_SOLICITUDES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["% Aprobados", "Score Biometria Prom", "Score OCR Prom", "% PEP", "Tiempo Proceso (min)", "Score Riesgo KYC", "% Docs Autenticados", "Total Solicitudes"],
    "kpi_formats": ["{:.2f}%", "{:.3f}", "{:.3f}", "{:.2f}%", "{:.1f} min", "{:.3f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_SOLICITUD) AS MES,
        ROUND(SUM(CASE WHEN CUENTA_CREADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_APROBADOS,
        ROUND(AVG(SCORE_RIESGO_KYC), 3) AS SCORE_RIESGO
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["% Aprobados", "Score Riesgo"],
    "treemap_query": """SELECT CANAL_ONBOARDING, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Canal de Onboarding"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_RIESGO_KYC), 3) AS SCORE_RIESGO FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa KYC por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "SCORE_RIESGO", "caption": "Tamano: volumen solicitudes | Color: score riesgo KYC"},
    "diagnostics": None,
    "simulator": None,
}

render_page(config)

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from app_pages.conn_helper import run_query

TABLE = config["table"]
CHART_LAYOUT = dict(paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC", font=dict(family="Inter, sans-serif", color="#334155"))
NAVY = ["#0F2B46", "#1B3A5C", "#2E7D8C", "#29B5E8", "#0F4C75", "#3282B8", "#11567F", "#1A5276"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de KYC Onboarding")

df_funnel = run_query(f"""
    SELECT 'Solicitudes' AS ETAPA, COUNT(*) AS N FROM {TABLE}
    UNION ALL SELECT 'Doc Autenticado', COUNT(*) FROM {TABLE} WHERE DOCUMENTO_AUTENTICADO = TRUE
    UNION ALL SELECT 'Verificado', COUNT(*) FROM {TABLE} WHERE RESULTADO_VALIDACION = 'Aprobado'
    UNION ALL SELECT 'Cuenta Creada', COUNT(*) FROM {TABLE} WHERE CUENTA_CREADA = TRUE
""")
df_heat = run_query(f"""
    SELECT CANAL_ONBOARDING, RESULTADO_VALIDACION, ROUND(AVG(SCORE_BIOMETRIA_FACIAL), 3) AS SCORE_BIO
    FROM {TABLE} GROUP BY 1, 2
""")

order = ["Solicitudes", "Doc Autenticado", "Verificado", "Cuenta Creada"]
df_funnel["ETAPA"] = pd.Categorical(df_funnel["ETAPA"], categories=order, ordered=True)
df_funnel = df_funnel.sort_values("ETAPA")
pivot = df_heat.pivot(index="CANAL_ONBOARDING", columns="RESULTADO_VALIDACION", values="SCORE_BIO").fillna(0)

c1, c2 = st.columns(2)
with c1:
    fig_fun = go.Figure(go.Funnel(y=df_funnel["ETAPA"], x=df_funnel["N"], marker=dict(color=NAVY[:4])))
    fig_fun.update_layout(**CHART_LAYOUT, title="Funnel: Solicitud → Cuenta Creada", height=420)
    st.plotly_chart(fig_fun, use_container_width=True)
with c2:
    fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, "#FAFBFC"], [1, NAVY[0]]], texttemplate="%{z:.3f}", hovertemplate="Canal: %{y}<br>Resultado: %{x}<br>Score: %{z:.3f}<extra></extra>"))
    fig_heat.update_layout(**CHART_LAYOUT, title="Score Biometria por Canal x Resultado", height=420)
    st.plotly_chart(fig_heat, use_container_width=True)

df_scatter = run_query(f"SELECT SCORE_BIOMETRIA_FACIAL, SCORE_OCR_DOCUMENTO, RESULTADO_VALIDACION FROM {TABLE}")
df_donut = run_query(f"SELECT MOTIVO_RECHAZO, COUNT(*) AS N FROM {TABLE} WHERE RESULTADO_VALIDACION != 'Aprobado' AND MOTIVO_RECHAZO IS NOT NULL GROUP BY 1 ORDER BY 2 DESC")

c3, c4 = st.columns(2)
with c3:
    fig_sc = px.scatter(df_scatter, x="SCORE_BIOMETRIA_FACIAL", y="SCORE_OCR_DOCUMENTO", color="RESULTADO_VALIDACION", color_discrete_sequence=NAVY, labels={"SCORE_BIOMETRIA_FACIAL": "Score Biometria", "SCORE_OCR_DOCUMENTO": "Score OCR"})
    fig_sc.update_layout(**CHART_LAYOUT, title="Score Biometria vs Score OCR", height=420)
    st.plotly_chart(fig_sc, use_container_width=True)
with c4:
    fig_donut = go.Figure(go.Pie(labels=df_donut["MOTIVO_RECHAZO"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Motivos de Rechazo", height=420)
    st.plotly_chart(fig_donut, use_container_width=True)

df_box = run_query(f"SELECT CANAL_ONBOARDING, TIEMPO_PROCESO_MINUTOS FROM {TABLE}")
fig_box = px.box(df_box, x="CANAL_ONBOARDING", y="TIEMPO_PROCESO_MINUTOS", color="CANAL_ONBOARDING", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Tiempo Proceso (min) por Canal Onboarding", showlegend=False, height=420)
st.plotly_chart(fig_box, use_container_width=True)

df_hist = run_query(f"SELECT SCORE_RIESGO_KYC FROM {TABLE}")
fig_hist = px.histogram(df_hist, x="SCORE_RIESGO_KYC", nbins=30, color_discrete_sequence=[NAVY[0]], labels={"SCORE_RIESGO_KYC": "Score Riesgo KYC"})
fig_hist.update_layout(**CHART_LAYOUT, title="Distribucion Score Riesgo KYC", height=400)
st.plotly_chart(fig_hist, use_container_width=True)
