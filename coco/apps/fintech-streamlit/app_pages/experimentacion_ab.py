from app_pages.page_template import render_page

config = {
    "key": "exp",
    "table": "MGG_FINTECH.PRODUCTO_Y_CRECIMIENTO.EXPERIMENTACION_AB",
    "icon": ":material/science:",
    "title": "Experimentacion A/B",
    "subtitle": "Plataforma de experimentacion con poder estadistico, lift y decision data-driven.",
    "cards": ["Registro de experimentos A/B en producto, pricing y UX con significancia estadistica", "Captura hipotesis, variantes, metricas de exito y decision final basada en datos reales", "Iteracion rapida de producto midiendo impacto real de cada cambio en conversion y revenue"],
    "date_col": "FECHA_INICIO",
    "filter_cols": ["AREA_PRODUCTO", "ESTADO", "DECISION_FINAL"],
    "kpi_query": """SELECT ROUND(AVG(LIFT_PCT),2), ROUND(AVG(P_VALUE),4), ROUND(SUM(CASE WHEN ESTADISTICAMENTE_SIGNIFICATIVO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(STATISTICAL_POWER)*100,1), ROUND(SUM(IMPACTO_REVENUE_MENSUAL_COP)/1e6,2), ROUND(AVG(DURACION_DIAS),1), ROUND(AVG(TOTAL_USUARIOS),0), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Lift %", "P-Value", "Significativo %", "Power %", "Impacto Rev M", "Duracion Dias", "Usuarios Prom", "Total"],
    "kpi_formats": ["{:.2f}%", "{:.4f}", "{:.1f}%", "{:.1f}%", "${:.2f}M", "{:.1f}", "{:,.0f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_INICIO) AS MES, ROUND(AVG(LIFT_PCT),2) AS LIFT, ROUND(AVG(STATISTICAL_POWER)*100,1) AS POWER FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["LIFT", "POWER"],
    "treemap_query": """SELECT AREA_PRODUCTO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Area Producto"},
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
SEMAFORO = ["#2D9B2D", "#6ABF4B", "#F5D63D", "#F5A623", "#E63946", "#1B8C1B", "#8BC34A", "#D32F2F"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de Experimentacion")

df_lift = run_query(f"SELECT DECISION_FINAL, ROUND(AVG(LIFT_PCT),2) AS LIFT_PROMEDIO, COUNT(*) AS EXPERIMENTOS FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
color_map = {"Variante ganadora implementada": "#2D9B2D", "Control ganador - no cambio": "#6ABF4B", "Sin significancia - extender": "#F5D63D", "Rollback aplicado": "#F5A623", "Experimento cancelado": "#E63946"}
fig_lift = px.bar(df_lift, x="DECISION_FINAL", y="LIFT_PROMEDIO", color="DECISION_FINAL", color_discrete_map=color_map, text="EXPERIMENTOS")
fig_lift.update_traces(texttemplate="%{text} exp", textposition="outside")
fig_lift.update_layout(**CHART_LAYOUT, title="Lift Promedio por Decision Final", showlegend=False, xaxis_title="", yaxis_title="Lift %")
st.plotly_chart(fig_lift, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_bar = run_query(f"SELECT AREA_PRODUCTO, ROUND(AVG(LIFT_PCT),2) AS LIFT_AVG FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
    fig_bar = px.bar(df_bar, x="AREA_PRODUCTO", y="LIFT_AVG", color_discrete_sequence=SEMAFORO)
    fig_bar.update_layout(**CHART_LAYOUT, title="Lift Promedio por Area")
    st.plotly_chart(fig_bar, use_container_width=True)
with col2:
    df_donut = run_query(f"SELECT DECISION_FINAL, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["DECISION_FINAL"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Decision Final")
    st.plotly_chart(fig_donut, use_container_width=True)

df_hist = run_query(f"SELECT P_VALUE FROM {TABLE} LIMIT 5000")
fig_hist = px.histogram(df_hist, x="P_VALUE", nbins=40, color_discrete_sequence=SEMAFORO)
fig_hist.update_layout(**CHART_LAYOUT, title="Distribucion de P-Value")
st.plotly_chart(fig_hist, use_container_width=True)

df_box = run_query(f"SELECT AREA_PRODUCTO, DURACION_DIAS FROM {TABLE} LIMIT 3000")
fig_box = px.box(df_box, x="AREA_PRODUCTO", y="DURACION_DIAS", color_discrete_sequence=SEMAFORO)
fig_box.update_layout(**CHART_LAYOUT, title="Duracion (Dias) por Area")
st.plotly_chart(fig_box, use_container_width=True)
