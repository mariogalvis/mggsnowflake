from app_pages.page_template import render_page

config = {
    "key": "gev",
    "table": "MGG_FINTECH.OPERACION_CLOUD_NATIVE.GESTION_EVENTOS",
    "icon": ":material/bolt:",
    "title": "Gestion Eventos",
    "subtitle": "Plataforma de eventos con tracking de latencia, dead letters, schema validation y consumidores.",
    "cards": ["Arquitectura event-driven donde cada accion genera un evento inmutable procesable", "Registra tipo evento, payload, timestamp, origen, consumidores y estado de procesamiento", "Sistemas reactivos en tiempo real en lugar de batch, habilitando experiencias instantaneas"],
    "date_col": "TIMESTAMP_EVENTO",
    "filter_cols": ["EVENT_TYPE", "ESTADO_PROCESAMIENTO", "TOPIC"],
    "kpi_query": """SELECT ROUND(AVG(LATENCIA_PUBLICACION_MS),0), ROUND(AVG(LATENCIA_CONSUMO_MS),0), ROUND(SUM(CONSUMIDORES_ERROR)*100.0/NULLIF(SUM(TOTAL_CONSUMIDORES),0),1), ROUND(SUM(CASE WHEN DEAD_LETTER THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(REINTENTOS),1), ROUND(SUM(CASE WHEN SCHEMA_VALID THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(TOTAL_CONSUMIDORES),1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Lat. pub (ms)", "Lat. consumo (ms)", "Error cons. %", "Dead letter %", "Reintentos", "Schema valid %", "Consumidores", "Total eventos"],
    "kpi_formats": ["{:.0f}", "{:.0f}", "{:.1f}%", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', TIMESTAMP_EVENTO) AS MES, ROUND(AVG(LATENCIA_PUBLICACION_MS),1) AS LATENCIA_PUBLICACION, ROUND(SUM(CASE WHEN DEAD_LETTER THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS DEAD_LETTER_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["LATENCIA_PUBLICACION", "DEAD_LETTER_PCT"],
    "treemap_query": """SELECT EVENT_TYPE AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Event Type"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Gestion de Eventos")

df_heatmap = run_query(f"SELECT EVENT_TYPE, TOPIC, ROUND(SUM(CONSUMIDORES_ERROR)*100.0/NULLIF(SUM(TOTAL_CONSUMIDORES),0),1) AS ERROR_PCT FROM {TABLE} WHERE EVENT_TYPE IN (SELECT EVENT_TYPE FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 6) AND TOPIC IN (SELECT TOPIC FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 6) GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="EVENT_TYPE", columns="TOPIC", values="ERROR_PCT", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.1f}%"))
fig_hm.update_layout(**CHART_LAYOUT, title="% Error por Event Type × Topic (Top 6)", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT LATENCIA_PUBLICACION_MS, LATENCIA_CONSUMO_MS, ESTADO_PROCESAMIENTO FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="LATENCIA_PUBLICACION_MS", y="LATENCIA_CONSUMO_MS", color="ESTADO_PROCESAMIENTO", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Latencia Publicacion vs Consumo")
col1.plotly_chart(fig_sc, use_container_width=True)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN SCHEMA_VALID THEN 1 ELSE 0 END) AS SCHEMA_VALID, SUM(CASE WHEN CONSUMIDORES_ERROR=0 THEN 1 ELSE 0 END) AS OK_CONSUMIDORES, SUM(CASE WHEN NOT DEAD_LETTER THEN 1 ELSE 0 END) AS SIN_DEAD_LETTER FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total", "Schema valid", "OK consumidores", "Sin dead letter"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["SCHEMA_VALID"].iloc[0], df_funnel["OK_CONSUMIDORES"].iloc[0], df_funnel["SIN_DEAD_LETTER"].iloc[0]], marker=dict(color=SEMAFORO[:4])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel: Total → Schema → OK → Sin DL")
col2.plotly_chart(fig_fn, use_container_width=True)

col3, col4 = st.columns(2)
df_donut = run_query(f"SELECT BROKER, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
fig_dn = px.pie(df_donut, names="BROKER", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Broker")
col3.plotly_chart(fig_dn, use_container_width=True)

df_box = run_query(f"SELECT EVENT_TYPE, LATENCIA_CONSUMO_MS FROM {TABLE} WHERE EVENT_TYPE IN (SELECT EVENT_TYPE FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 6)")
fig_bx = px.box(df_box, x="EVENT_TYPE", y="LATENCIA_CONSUMO_MS", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Latencia Consumo (ms) por Event Type")
col4.plotly_chart(fig_bx, use_container_width=True)
