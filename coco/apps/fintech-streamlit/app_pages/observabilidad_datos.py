from app_pages.page_template import render_page

config = {
    "key": "obs",
    "table": "MGG_FINTECH.OPERACION_CLOUD_NATIVE.OBSERVABILIDAD_DATOS",
    "icon": ":material/monitoring:",
    "title": "Observabilidad Datos",
    "subtitle": "Monitoreo de pipelines de datos con SLA, freshness, calidad y costos compute.",
    "cards": ["Monitoreo en tiempo real de la salud de pipelines de datos, tablas y modelos ML", "Registra pipeline, estado, latencia, registros procesados, errores y alertas automaticas", "Detecta al instante fallos en flujo de datos antes de que impacten al usuario o decisiones de negocio"],
    "date_col": "FECHA_HORA_INICIO",
    "filter_cols": ["PIPELINE", "ESTADO", "ORQUESTADOR"],
    "kpi_query": """SELECT ROUND(AVG(TASA_EXITO)*100,1), ROUND(AVG(DURACION_SEGUNDOS),0), ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(COSTO_COMPUTE_USD),2), ROUND(AVG(REGISTROS_PROCESADOS)/1e3,1), ROUND(SUM(CASE WHEN ALERTA_DISPARADA THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(FRESHNESS_MINUTOS),1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Exito %", "Duracion (s)", "SLA %", "Costo (USD)", "Registros (K)", "Alertas %", "Freshness (min)", "Ejecuciones"],
    "kpi_formats": ["{:.1f}%", "{:.0f}", "{:.1f}%", "${:.2f}", "{:.1f}K", "{:.1f}%", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_INICIO) AS MES, ROUND(AVG(TASA_EXITO)*100,1) AS TASA_EXITO, ROUND(AVG(FRESHNESS_MINUTOS),1) AS FRESHNESS FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["TASA_EXITO", "FRESHNESS"],
    "treemap_query": """SELECT PIPELINE AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Pipeline"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Observabilidad")

df_heatmap = run_query(f"SELECT PIPELINE, ORQUESTADOR, ROUND(AVG(TASA_EXITO)*100,1) AS TASA_EXITO FROM {TABLE} GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="PIPELINE", columns="ORQUESTADOR", values="TASA_EXITO", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.1f}%"))
fig_hm.update_layout(**CHART_LAYOUT, title="Tasa Exito por Pipeline × Orquestador", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT DURACION_SEGUNDOS, COSTO_COMPUTE_USD, ESTADO FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="DURACION_SEGUNDOS", y="COSTO_COMPUTE_USD", color="ESTADO", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Duracion vs Costo Compute (USD)")
col1.plotly_chart(fig_sc, use_container_width=True)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ESTADO='EXITOSO' THEN 1 ELSE 0 END) AS EXITOSO, SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END) AS DENTRO_SLA, SUM(CASE WHEN DQ_SCORE >= 0.8 THEN 1 ELSE 0 END) AS DQ_PASS FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total", "Exitoso", "Dentro SLA", "DQ pass"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["EXITOSO"].iloc[0], df_funnel["DENTRO_SLA"].iloc[0], df_funnel["DQ_PASS"].iloc[0]], marker=dict(color=SEMAFORO[:4])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel: Total → Exitoso → SLA → DQ")
col2.plotly_chart(fig_fn, use_container_width=True)

col3, col4 = st.columns(2)
df_donut = run_query(f"SELECT SEVERIDAD_ALERTA, COUNT(*) AS N FROM {TABLE} WHERE ALERTA_DISPARADA GROUP BY 1")
fig_dn = px.pie(df_donut, names="SEVERIDAD_ALERTA", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Severidad de Alertas")
col3.plotly_chart(fig_dn, use_container_width=True)

df_box = run_query(f"SELECT PIPELINE, FRESHNESS_MINUTOS FROM {TABLE} WHERE PIPELINE IN (SELECT PIPELINE FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 6)")
fig_bx = px.box(df_box, x="PIPELINE", y="FRESHNESS_MINUTOS", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Freshness (min) por Pipeline Top 6")
col4.plotly_chart(fig_bx, use_container_width=True)
