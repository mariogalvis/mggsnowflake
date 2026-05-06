from app_pages.page_template import render_page

config = {
    "key": "rai",
    "table": "MGG_FINTECH.SOPORTE_FINANCIERO_INTELIGENTE.RECOMENDACIONES_AHORRO_INVERSION",
    "icon": ":material/savings:",
    "title": "Ahorro e Inversion",
    "subtitle": "Recomendaciones personalizadas de ahorro e inversion basadas en perfil y metas financieras.",
    "cards": ["Recomendaciones personalizadas de ahorro e inversion segun perfil y comportamiento", "Sugiere montos, productos (CDT, fondos, ahorro programado), momento optimo y meta financiera", "Democratiza acceso a asesoria financiera y aumenta penetracion de productos de ahorro e inversion"],
    "date_col": "FECHA_RECOMENDACION",
    "filter_cols": ["PRODUCTO_RECOMENDADO", "META_FINANCIERA", "PERFIL_RIESGO_USUARIO"],
    "kpi_query": """SELECT ROUND(SUM(CASE WHEN RECOMENDACION_VISTA THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN RECOMENDACION_ACEPTADA THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(MONTO_SUGERIDO_COP)/1e6,1), ROUND(SUM(MONTO_REAL_INVERTIDO_COP)/1e6,1), ROUND(AVG(TASA_ESTIMADA_EA_PCT),1), ROUND(AVG(SCORE_RELEVANCIA)*100,1), ROUND(AVG(UTILIDAD_REPORTADA),1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Vista %", "Aceptada %", "Monto sugerido (M)", "Invertido real (M)", "Tasa EA %", "Relevancia %", "Utilidad", "Recomendaciones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:.1f}M", "${:.1f}M", "{:.1f}%", "{:.1f}%", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_RECOMENDACION) AS MES, ROUND(SUM(CASE WHEN RECOMENDACION_ACEPTADA THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS ACEPTADA_PCT, ROUND(AVG(TASA_ESTIMADA_EA_PCT),1) AS TASA_ESTIMADA_EA_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["ACEPTADA_PCT", "TASA_ESTIMADA_EA_PCT"],
    "treemap_query": """SELECT PRODUCTO_RECOMENDADO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Producto Recomendado"},
    "geo_query": """SELECT CIUDAD, AVG(SCORE_RELEVANCIA) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Score Relevancia por Ciudad", "color_col": "VALOR"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Recomendaciones")

df_heatmap = run_query(f"SELECT PRODUCTO_RECOMENDADO, META_FINANCIERA, ROUND(SUM(CASE WHEN RECOMENDACION_ACEPTADA THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS ACEPTADA_PCT FROM {TABLE} GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="PRODUCTO_RECOMENDADO", columns="META_FINANCIERA", values="ACEPTADA_PCT", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.1f}%"))
fig_hm.update_layout(**CHART_LAYOUT, title="% Aceptada por Producto × Meta Financiera", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT SCORE_RELEVANCIA, UTILIDAD_REPORTADA, PERFIL_RIESGO_USUARIO FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="SCORE_RELEVANCIA", y="UTILIDAD_REPORTADA", color="PERFIL_RIESGO_USUARIO", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Score Relevancia vs Utilidad Reportada")
col1.plotly_chart(fig_sc, use_container_width=True)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN RECOMENDACION_VISTA THEN 1 ELSE 0 END) AS VISTA, SUM(CASE WHEN RECOMENDACION_ACEPTADA THEN 1 ELSE 0 END) AS ACEPTADA FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total", "Vista", "Aceptada"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["VISTA"].iloc[0], df_funnel["ACEPTADA"].iloc[0]], marker=dict(color=SEMAFORO[:3])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel: Total → Vista → Aceptada")
col2.plotly_chart(fig_fn, use_container_width=True)

col3, col4 = st.columns(2)
df_donut = run_query(f"SELECT PERFIL_RIESGO_USUARIO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
fig_dn = px.pie(df_donut, names="PERFIL_RIESGO_USUARIO", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Perfil de Riesgo Usuario")
col3.plotly_chart(fig_dn, use_container_width=True)

df_box = run_query(f"SELECT PRODUCTO_RECOMENDADO, MONTO_SUGERIDO_COP/1e6 AS MONTO_M FROM {TABLE}")
fig_bx = px.box(df_box, x="PRODUCTO_RECOMENDADO", y="MONTO_M", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Monto Sugerido (M) por Producto")
col4.plotly_chart(fig_bx, use_container_width=True)
