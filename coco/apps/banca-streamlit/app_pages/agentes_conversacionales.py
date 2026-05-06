from app_pages.page_template import render_page

config = {
    "key": "agc",
    "table": "MGG_BANCA.DATOS_Y_AI.AGENTES_CONVERSACIONALES",
    "icon": ":material/psychology:",
    "title": "Agentes Conversacionales",
    "subtitle": "Monitoreo de agentes GenAI para consultas analiticas con SQL generativo y guardrails.",
    "cards": ["TIPO_CONSULTA", "AREA_NEGOCIO", "MODELO_USADO"],
    "date_col": "FECHA_HORA_CONSULTA",
    "filter_cols": ["TIPO_CONSULTA", "AREA_NEGOCIO", "MODELO_USADO"],
    "kpi_query": """SELECT
        AVG(CASE WHEN SQL_EJECUTADO_EXITOSO THEN 1 ELSE 0 END)*100,
        AVG(RATING_RESPUESTA),
        AVG(LATENCIA_MS),
        AVG(CONFIANZA_RESPUESTA),
        AVG(CASE WHEN RESPUESTA_UTIL THEN 1 ELSE 0 END)*100,
        AVG(TOKENS_INPUT + TOKENS_OUTPUT),
        AVG(COSTO_CONSULTA_COP),
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["% SQL exitoso", "Rating prom", "Latencia (ms)", "Confianza prom", "% respuesta util", "Tokens prom", "Costo prom (COP)", "Total consultas"],
    "kpi_formats": [",.1f", ",.2f", ",.0f", ",.2f", ",.1f", ",.0f", ",.0f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_CONSULTA::DATE) AS MES, AVG(CASE WHEN SQL_EJECUTADO_EXITOSO THEN 1 ELSE 0 END)*100 AS PCT_SQL_EXITOSO, AVG(LATENCIA_MS) AS LATENCIA_MS FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["PCT_SQL_EXITOSO", "LATENCIA_MS"],
    "treemap_query": """SELECT TIPO_CONSULTA AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Consulta"},
    "geo_query": None,
    "geo_config": None,
    "diagnostics": None,
    "simulator": None,
}

render_page(config)

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from app_pages.conn_helper import run_query

TABLE = config["table"]
CHART_LAYOUT = dict(paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC", font=dict(family="Inter, sans-serif", color="#334155"))
NAVY = ["#0F2B46", "#1B3A5C", "#2E7D8C", "#29B5E8", "#0F4C75", "#3282B8", "#11567F", "#1A5276"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de Agentes Conversacionales")

df_hm = run_query(f"SELECT TIPO_CONSULTA, AREA_NEGOCIO, AVG(RATING_RESPUESTA) AS RATING FROM {TABLE} GROUP BY 1,2")
df_piv = df_hm.pivot(index="TIPO_CONSULTA", columns="AREA_NEGOCIO", values="RATING").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=df_piv.values, x=df_piv.columns.tolist(), y=df_piv.index.tolist(), colorscale=[[0,"#FAFBFC"],[1,"#0F2B46"]]))
fig_hm.update_layout(**CHART_LAYOUT, title="Rating por Tipo Consulta y Area de Negocio", xaxis_title="Area Negocio", yaxis_title="Tipo Consulta")
st.plotly_chart(fig_hm, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT LATENCIA_MS, CONFIANZA_RESPUESTA, MODELO_USADO FROM {TABLE}")
    fig_sc = px.scatter(df_sc, x="LATENCIA_MS", y="CONFIANZA_RESPUESTA", color="MODELO_USADO", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="Latencia vs Confianza", xaxis_title="Latencia (ms)", yaxis_title="Confianza Respuesta")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT MODELO_USADO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="MODELO_USADO", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Modelo Usado")
    st.plotly_chart(fig_dn, use_container_width=True)

df_fn = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN SQL_GENERADO THEN 1 ELSE 0 END) AS SQL_GEN, SUM(CASE WHEN SQL_EJECUTADO_EXITOSO THEN 1 ELSE 0 END) AS SQL_OK, SUM(CASE WHEN RESPUESTA_UTIL THEN 1 ELSE 0 END) AS UTIL FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total Consultas", "SQL Generado", "SQL Exitoso", "Respuesta Util"], x=[df_fn["TOTAL"].iloc[0], df_fn["SQL_GEN"].iloc[0], df_fn["SQL_OK"].iloc[0], df_fn["UTIL"].iloc[0]], marker=dict(color=NAVY[:4])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel de Efectividad")
st.plotly_chart(fig_fn, use_container_width=True)

df_box = run_query(f"SELECT TIPO_CONSULTA, TOKENS_OUTPUT FROM {TABLE}")
fig_box = px.box(df_box, x="TIPO_CONSULTA", y="TOKENS_OUTPUT", color="TIPO_CONSULTA", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Tokens Output por Tipo de Consulta", showlegend=False, xaxis_title="Tipo Consulta", yaxis_title="Tokens")
st.plotly_chart(fig_box, use_container_width=True)
