from app_pages.page_template import render_page

config = {
    "key": "bsc",
    "table": "MGG_FINTECH.RIESGO_AMPLIADO.BEHAVIORAL_SCORING",
    "icon": ":material/psychology:",
    "title": "Behavioral Scoring",
    "subtitle": "Scoring basado en comportamiento digital, patrones transaccionales y estabilidad financiera.",
    "cards": ["Scoring basado en patrones de comportamiento digital: frecuencia, horarios, geolocalizacion", "Va mas alla del historial crediticio usando datos de uso de la app y patron de tecleo", "Detecta anomalias, valida identidad pasivamente y amplia acceso a credito de forma segura"],
    "date_col": "FECHA_EVALUACION",
    "filter_cols": ["SEGMENTO_COMPORTAMIENTO", "DISPOSITIVO_PRINCIPAL", "CIUDAD"],
    "kpi_query": """SELECT ROUND(AVG(SCORE_BEHAVIORAL),0), ROUND(AVG(PROB_DEFAULT_BEHAVIORAL)*100,1), ROUND(AVG(PROB_FRAUDE_BEHAVIORAL)*100,1), ROUND(AVG(ESTABILIDAD_FINANCIERA)*100,1), ROUND(AVG(REGULARIDAD_INGRESOS)*100,1), ROUND(AVG(CONSISTENCIA_GASTOS)*100,1), ROUND(AVG(CONFIANZA_IDENTIDAD_PASIVA)*100,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Score behavioral", "PD behavioral %", "Prob fraude %", "Estabilidad %", "Regularidad %", "Consistencia %", "Confianza ID %", "Evaluaciones"],
    "kpi_formats": ["{:.0f}", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_EVALUACION) AS MES, ROUND(AVG(SCORE_BEHAVIORAL),1) AS SCORE_BEHAVIORAL, ROUND(AVG(PROB_DEFAULT_BEHAVIORAL)*100,1) AS PD_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["SCORE_BEHAVIORAL", "PD_PCT"],
    "treemap_query": """SELECT SEGMENTO_COMPORTAMIENTO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Segmento Comportamiento"},
    "geo_query": """SELECT CIUDAD, AVG(SCORE_BEHAVIORAL) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Score Behavioral por Ciudad", "color_col": "VALOR"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Scoring Comportamental",
        "desc": "Ajusta las variables de comportamiento digital para estimar el riesgo crediticio del usuario.",
        "features": [
            {"name": "Score Behavioral", "min": 0, "max": 100, "default": 65, "weight": -0.3, "step": 1},
            {"name": "Prob Default Behavioral", "min": 0, "max": 1, "default": 0.12, "weight": 0.25, "step": 0.01},
            {"name": "Estabilidad Financiera", "min": 0, "max": 100, "default": 70, "weight": -0.2, "step": 1},
            {"name": "Regularidad Ingresos", "min": 0, "max": 100, "default": 75, "weight": -0.15, "step": 1},
            {"name": "Consistencia Gastos", "min": 0, "max": 100, "default": 60, "weight": -0.1, "step": 1},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Riesgo Bajo", "Riesgo Medio", "Riesgo Alto"],
    },
}

render_page(config)

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from app_pages.conn_helper import run_query

TABLE = config["table"]
CHART_LAYOUT = dict(paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC", font=dict(family="Inter, sans-serif", color="#334155"))
SEMAFORO = ["#2D9B2D", "#6ABF4B", "#F5D63D", "#F5A623", "#E63946", "#1B8C1B", "#8BC34A", "#D32F2F"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de Behavioral Scoring")

df_heatmap = run_query(f"SELECT SEGMENTO_COMPORTAMIENTO, DISPOSITIVO_PRINCIPAL, ROUND(AVG(SCORE_BEHAVIORAL),1) AS SCORE FROM {TABLE} GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="SEGMENTO_COMPORTAMIENTO", columns="DISPOSITIVO_PRINCIPAL", values="SCORE", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.0f}"))
fig_hm.update_layout(**CHART_LAYOUT, title="Score por Segmento × Dispositivo", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT ESTABILIDAD_FINANCIERA, PROB_DEFAULT_BEHAVIORAL, SEGMENTO_COMPORTAMIENTO FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="ESTABILIDAD_FINANCIERA", y="PROB_DEFAULT_BEHAVIORAL", color="SEGMENTO_COMPORTAMIENTO", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Estabilidad vs Prob Default")
col1.plotly_chart(fig_sc, use_container_width=True)

df_radar = run_query(f"SELECT SEGMENTO_COMPORTAMIENTO, ROUND(AVG(REGULARIDAD_INGRESOS)*100,1) AS REGULARIDAD, ROUND(AVG(CONSISTENCIA_GASTOS)*100,1) AS CONSISTENCIA, ROUND(AVG(ESTABILIDAD_FINANCIERA)*100,1) AS ESTABILIDAD, ROUND(AVG(CONFIANZA_IDENTIDAD_PASIVA)*100,1) AS CONFIANZA_ID, ROUND(AVG(SCORE_BEHAVIORAL)/10,1) AS SCORE_10 FROM {TABLE} GROUP BY 1")
fig_radar = go.Figure()
for i, row in df_radar.iterrows():
    fig_radar.add_trace(go.Scatterpolar(r=[row["REGULARIDAD"], row["CONSISTENCIA"], row["ESTABILIDAD"], row["CONFIANZA_ID"], row["SCORE_10"]], theta=["Regularidad", "Consistencia", "Estabilidad", "Confianza ID", "Score/10"], fill="toself", name=row["SEGMENTO_COMPORTAMIENTO"], line=dict(color=SEMAFORO[i % len(SEMAFORO)])))
fig_radar.update_layout(**CHART_LAYOUT, title="Radar Metricas por Segmento", polar=dict(bgcolor="#FAFBFC"))
col2.plotly_chart(fig_radar, use_container_width=True)

col3, col4 = st.columns(2)
df_donut = run_query(f"SELECT SEGMENTO_COMPORTAMIENTO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
fig_dn = px.pie(df_donut, names="SEGMENTO_COMPORTAMIENTO", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Segmento")
col3.plotly_chart(fig_dn, use_container_width=True)

df_box = run_query(f"SELECT SEGMENTO_COMPORTAMIENTO, SCORE_BEHAVIORAL FROM {TABLE}")
fig_bx = px.box(df_box, x="SEGMENTO_COMPORTAMIENTO", y="SCORE_BEHAVIORAL", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Score Behavioral por Segmento")
col4.plotly_chart(fig_bx, use_container_width=True)
