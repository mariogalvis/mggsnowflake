from app_pages.page_template import render_page

config = {
    "key": "enr",
    "table": "MGG_FINTECH.OPEN_BANKING.ENRIQUECIMIENTO_DATOS",
    "icon": ":material/auto_fix_high:",
    "title": "Enriquecimiento Datos",
    "subtitle": "Categorizacion y enriquecimiento de transacciones con ML para insights de gasto.",
    "cards": ["Categoriza transacciones automaticamente con AI: alimentacion, transporte, entretenimiento", "Detecta suscripciones recurrentes, identifica comercios y clasifica cada gasto con modelos ML", "Insights de gasto personalizados que alimentan scoring alternativo y mejoran la experiencia del usuario"],
    "date_col": "FECHA_TRANSACCION",
    "filter_cols": ["CATEGORIA_ASIGNADA", "CONFIANZA_CATEGORIZACION", "MODELO_CATEGORIZACION"],
    "kpi_query": """SELECT ROUND(AVG(SCORE_CONFIANZA)*100,1), ROUND(SUM(CASE WHEN CORRECCION_USUARIO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(LATENCIA_ENRIQUECIMIENTO_MS),1), ROUND(SUM(CASE WHEN ES_SUSCRIPCION_RECURRENTE THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN ES_GASTO_ESENCIAL THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(AHORRO_POTENCIAL_COP)/1e3,1), ROUND(AVG(MONTO_COP)/1e3,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Confianza %", "Correccion %", "Latencia ms", "Suscripcion %", "Esencial %", "Ahorro K", "Monto K", "Total"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:.1f}%", "${:.1f}K", "${:.1f}K", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_TRANSACCION) AS MES, ROUND(AVG(SCORE_CONFIANZA)*100,1) AS CONFIANZA, ROUND(SUM(CASE WHEN CORRECCION_USUARIO THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS CORRECCION_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["CONFIANZA", "CORRECCION_PCT"],
    "treemap_query": """SELECT CATEGORIA_ASIGNADA AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Categoria Asignada"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Enriquecimiento")

df_heat = run_query(f"SELECT CATEGORIA_ASIGNADA, MODELO_CATEGORIZACION, ROUND(AVG(SCORE_CONFIANZA)*100,1) AS SCORE FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="CATEGORIA_ASIGNADA", columns="MODELO_CATEGORIZACION", values="SCORE").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.1f}%"))
fig_heat.update_layout(**CHART_LAYOUT, title="Score Confianza: Categoria × Modelo")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT SCORE_CONFIANZA, AHORRO_POTENCIAL_COP/1e3 AS AHORRO_K, CONFIANZA_CATEGORIZACION FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="SCORE_CONFIANZA", y="AHORRO_K", color="CONFIANZA_CATEGORIZACION", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="Score Confianza vs Ahorro Potencial")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_donut = run_query(f"SELECT CATEGORIA_ASIGNADA, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
    fig_donut = go.Figure(go.Pie(labels=df_donut["CATEGORIA_ASIGNADA"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Top 8 Categorias")
    st.plotly_chart(fig_donut, use_container_width=True)

df_hist = run_query(f"SELECT SCORE_CONFIANZA FROM {TABLE} LIMIT 5000")
fig_hist = px.histogram(df_hist, x="SCORE_CONFIANZA", nbins=40, color_discrete_sequence=SEMAFORO)
fig_hist.update_layout(**CHART_LAYOUT, title="Distribucion de Score Confianza")
st.plotly_chart(fig_hist, use_container_width=True)

df_box = run_query(f"SELECT MODELO_CATEGORIZACION AS MODELO, LATENCIA_ENRIQUECIMIENTO_MS AS LATENCIA FROM {TABLE} LIMIT 3000")
fig_box = px.box(df_box, x="MODELO", y="LATENCIA", color_discrete_sequence=SEMAFORO)
fig_box.update_layout(**CHART_LAYOUT, title="Latencia por Modelo")
st.plotly_chart(fig_box, use_container_width=True)
