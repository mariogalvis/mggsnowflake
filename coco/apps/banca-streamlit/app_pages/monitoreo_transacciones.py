from app_pages.page_template import render_page

config = {
    "key": "mtx",
    "table": "MGG_BANCA.CUMPLIMIENTO_Y_REGULACION.MONITOREO_TRANSACCIONES",
    "icon": ":material/monitoring:",
    "title": "Monitoreo Transacciones",
    "subtitle": "Monitoreo de transacciones sospechosas con generacion de ROS y gestion de alertas UIAF.",
    "cards": ["REGLA_ACTIVADA", "DISPOSICION", "CIUDAD"],
    "date_col": "FECHA_ALERTA",
    "filter_cols": ["REGLA_ACTIVADA", "DISPOSICION", "CIUDAD"],
    "kpi_query": """SELECT
        AVG(SCORE_RIESGO),
        AVG(CASE WHEN ROS_GENERADO THEN 1 ELSE 0 END)*100,
        AVG(CASE WHEN ES_PEP THEN 1 ELSE 0 END)*100,
        AVG(TIEMPO_RESOLUCION_HORAS),
        AVG(MONTO_TRANSACCION_COP)/1e6,
        AVG(CASE WHEN EN_LISTA_RESTRICTIVA THEN 1 ELSE 0 END)*100,
        AVG(NIVEL_PRIORIDAD),
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score riesgo prom", "% ROS generado", "% PEP", "Tiempo resolucion (h)", "Monto prom (M)", "% lista restrictiva", "Prioridad prom", "Total alertas"],
    "kpi_formats": [",.2f", ",.1f", ",.1f", ",.1f", ",.1f", ",.1f", ",.2f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_ALERTA) AS MES, AVG(SCORE_RIESGO) AS SCORE_RIESGO, COUNT(*) AS TOTAL_ALERTAS FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["SCORE_RIESGO", "TOTAL_ALERTAS"],
    "treemap_query": """SELECT REGLA_ACTIVADA AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Regla Activada"},
    "geo_query": """SELECT CIUDAD AS CITY, AVG(SCORE_RIESGO) AS VALUE FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Score Riesgo por Ciudad", "metric": "SCORE_RIESGO"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Monitoreo de Transacciones")

df_hm = run_query(f"SELECT REGLA_ACTIVADA, DISPOSICION, AVG(SCORE_RIESGO) AS SCORE FROM {TABLE} GROUP BY 1,2")
df_piv = df_hm.pivot(index="REGLA_ACTIVADA", columns="DISPOSICION", values="SCORE").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=df_piv.values, x=df_piv.columns.tolist(), y=df_piv.index.tolist(), colorscale=[[0,"#FAFBFC"],[1,"#0F2B46"]]))
fig_hm.update_layout(**CHART_LAYOUT, title="Score Riesgo por Regla y Disposicion", xaxis_title="Disposicion", yaxis_title="Regla Activada")
st.plotly_chart(fig_hm, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT SCORE_RIESGO, MONTO_TRANSACCION_COP/1e6 AS MONTO_M, ESTADO_ALERTA, NIVEL_PRIORIDAD FROM {TABLE}")
    fig_sc = px.scatter(df_sc, x="SCORE_RIESGO", y="MONTO_M", color="ESTADO_ALERTA", size="NIVEL_PRIORIDAD", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="Score Riesgo vs Monto", xaxis_title="Score Riesgo", yaxis_title="Monto (Millones COP)")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT DISPOSICION, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="DISPOSICION", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Disposicion")
    st.plotly_chart(fig_dn, use_container_width=True)

df_fn = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ES_PEP THEN 1 ELSE 0 END) AS PEP, SUM(CASE WHEN EN_LISTA_RESTRICTIVA THEN 1 ELSE 0 END) AS LISTA, SUM(CASE WHEN ROS_GENERADO THEN 1 ELSE 0 END) AS ROS FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total Alertas", "PEP", "Lista Restrictiva", "ROS Generado"], x=[df_fn["TOTAL"].iloc[0], df_fn["PEP"].iloc[0], df_fn["LISTA"].iloc[0], df_fn["ROS"].iloc[0]], marker=dict(color=NAVY[:4])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel de Alertas")
st.plotly_chart(fig_fn, use_container_width=True)

df_box = run_query(f"SELECT REGLA_ACTIVADA, TIEMPO_RESOLUCION_HORAS FROM {TABLE} WHERE REGLA_ACTIVADA IN (SELECT REGLA_ACTIVADA FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 6)")
fig_box = px.box(df_box, x="REGLA_ACTIVADA", y="TIEMPO_RESOLUCION_HORAS", color="REGLA_ACTIVADA", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Tiempo Resolucion por Regla (Top 6)", showlegend=False, xaxis_title="Regla Activada", yaxis_title="Horas")
st.plotly_chart(fig_box, use_container_width=True)
