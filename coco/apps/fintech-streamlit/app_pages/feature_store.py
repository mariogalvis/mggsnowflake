from app_pages.page_template import render_page

config = {
    "key": "fst",
    "table": "MGG_FINTECH.OPERACION_CLOUD_NATIVE.FEATURE_STORE",
    "icon": ":material/storage:",
    "title": "Feature Store",
    "subtitle": "Monitoreo de servicio de features con latencia, drift, disponibilidad y SLA.",
    "cards": ["Repositorio centralizado de features calculadas y servidas en tiempo real para modelos ML", "Registra feature, valor, timestamp, modelo consumidor, latencia de servicio y freshness", "Garantiza que todos los modelos usen las mismas variables consistentes y actualizadas en produccion"],
    "date_col": "TIMESTAMP_COMPUTED",
    "filter_cols": ["MODELO_CONSUMIDOR", "ESTADO_FEATURE", "TIPO_COMPUTACION"],
    "kpi_query": """SELECT ROUND(AVG(LATENCIA_SERVING_MS),0), ROUND(AVG(DISPONIBILIDAD)*100,1), ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN VALOR_NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN VALOR_OUTLIER THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(DRIFT_SCORE)*100,1), ROUND(AVG(IMPORTANCIA_MODELO)*100,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Latencia (ms)", "Disponibilidad %", "SLA %", "Nulls %", "Outliers %", "Drift %", "Importancia %", "Total servings"],
    "kpi_formats": ["{:.0f}", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', TIMESTAMP_COMPUTED) AS MES, ROUND(AVG(LATENCIA_SERVING_MS),1) AS LATENCIA, ROUND(AVG(DRIFT_SCORE)*100,1) AS DRIFT_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["LATENCIA", "DRIFT_PCT"],
    "treemap_query": """SELECT MODELO_CONSUMIDOR AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Modelo Consumidor"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Feature Store")

df_heatmap = run_query(f"SELECT FEATURE_NAME, MODELO_CONSUMIDOR, ROUND(AVG(DRIFT_SCORE)*100,1) AS DRIFT FROM {TABLE} WHERE FEATURE_NAME IN (SELECT FEATURE_NAME FROM {TABLE} GROUP BY 1 ORDER BY AVG(DRIFT_SCORE) DESC LIMIT 8) GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="FEATURE_NAME", columns="MODELO_CONSUMIDOR", values="DRIFT", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.1f}%"))
fig_hm.update_layout(**CHART_LAYOUT, title="Drift Score por Feature × Modelo (Top 8)", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT LATENCIA_SERVING_MS, IMPORTANCIA_MODELO, ESTADO_FEATURE FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="LATENCIA_SERVING_MS", y="IMPORTANCIA_MODELO", color="ESTADO_FEATURE", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Latencia Serving vs Importancia Modelo")
col1.plotly_chart(fig_sc, use_container_width=True)

df_donut = run_query(f"SELECT TIPO_COMPUTACION, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
fig_dn = px.pie(df_donut, names="TIPO_COMPUTACION", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Tipo de Computacion")
col2.plotly_chart(fig_dn, use_container_width=True)

col3, col4 = st.columns(2)
df_bar = run_query(f"SELECT MODELO_CONSUMIDOR, ROUND(SUM(CASE WHEN DRIFT_SCORE > 0.3 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS DRIFT_ALERTA_PCT FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 6")
fig_bar = px.bar(df_bar, x="MODELO_CONSUMIDOR", y="DRIFT_ALERTA_PCT", color_discrete_sequence=[SEMAFORO[3]])
fig_bar.update_layout(**CHART_LAYOUT, title="% Drift Alerta por Modelo Top 6")
col3.plotly_chart(fig_bar, use_container_width=True)

df_box = run_query(f"SELECT TIPO_COMPUTACION, LATENCIA_SERVING_MS FROM {TABLE}")
fig_bx = px.box(df_box, x="TIPO_COMPUTACION", y="LATENCIA_SERVING_MS", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Latencia Serving (ms) por Tipo Computacion")
col4.plotly_chart(fig_bx, use_container_width=True)
