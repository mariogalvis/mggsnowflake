from app_pages.page_template import render_page

config = {
    "key": "rrt",
    "table": "MGG_FINTECH.RIESGO_AMPLIADO.RIESGO_TRANSACCIONAL_RT",
    "icon": ":material/shield:",
    "title": "Riesgo Transaccional RT",
    "subtitle": "Evaluacion de riesgo en tiempo real con ML, biometria de comportamiento y contexto.",
    "cards": ["Evaluacion de riesgo en tiempo real a nivel de cada transaccion individual en <100ms", "Analiza monto, dispositivo, velocidad, patron, comercio y contexto para score instantaneo", "Decisiones de aprobacion/bloqueo en milisegundos protegiendo al usuario sin friccion innecesaria"],
    "date_col": "FECHA_HORA",
    "filter_cols": ["TIPO_EVENTO", "DECISION", "PLATAFORMA"],
    "kpi_query": """SELECT ROUND(AVG(SCORE_RIESGO_RT)*100,1), ROUND(AVG(LATENCIA_EVALUACION_MS),0), ROUND(SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN FALSO_POSITIVO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN DISPOSITIVO_CONOCIDO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN UBICACION_HABITUAL THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(CONFIANZA_MODELO)*100,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Score riesgo %", "Latencia (ms)", "Fraude %", "Falso positivo %", "Disp. conocido %", "Ubic. habitual %", "Confianza %", "Total eventos"],
    "kpi_formats": ["{:.1f}%", "{:.0f}", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES, ROUND(AVG(SCORE_RIESGO_RT)*100,1) AS SCORE_RIESGO, ROUND(SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS FRAUDE_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["SCORE_RIESGO", "FRAUDE_PCT"],
    "treemap_query": """SELECT TIPO_EVENTO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo Evento"},
    "geo_query": """SELECT CIUDAD, AVG(SCORE_RIESGO_RT) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Score Riesgo por Ciudad", "color_col": "VALOR"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Riesgo Transaccional",
        "desc": "Ajusta las variables para estimar el nivel de riesgo en tiempo real de una transaccion.",
        "features": [
            {"name": "Score Riesgo RT", "min": 0, "max": 100, "default": 50, "weight": 0.35, "step": 1},
            {"name": "Latencia Evaluacion (ms)", "min": 10, "max": 500, "default": 80, "weight": -0.15, "step": 10},
            {"name": "Confianza Modelo", "min": 0, "max": 1, "default": 0.7, "weight": -0.2, "step": 0.05},
            {"name": "Dispositivo Conocido", "min": 0, "max": 1, "default": 1, "weight": -0.15, "step": 1},
            {"name": "Ubicacion Habitual", "min": 0, "max": 1, "default": 1, "weight": -0.15, "step": 1},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Riesgo Bajo", "Riesgo Medio", "Riesgo Alto"],
    },
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
st.subheader(":material/bar_chart: Analisis Avanzado de Riesgo Transaccional")

df_heatmap = run_query(f"SELECT TIPO_EVENTO, DECISION, ROUND(AVG(SCORE_RIESGO_RT)*100,1) AS SCORE FROM {TABLE} GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="TIPO_EVENTO", columns="DECISION", values="SCORE", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.1f}%"))
fig_hm.update_layout(**CHART_LAYOUT, title="Score Riesgo por Tipo Evento × Decision", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT SCORE_RIESGO_RT, MONTO_COP/1e3 AS MONTO_K, DECISION FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="SCORE_RIESGO_RT", y="MONTO_K", color="DECISION", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Score Riesgo vs Monto (K)")
col1.plotly_chart(fig_sc, use_container_width=True)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN DECISION='ALERTA' OR DECISION='RECHAZAR' THEN 1 ELSE 0 END) AS ALERTA, SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END) AS FRAUDE_CONFIRMADO FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total", "Alerta", "Fraude confirmado"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["ALERTA"].iloc[0], df_funnel["FRAUDE_CONFIRMADO"].iloc[0]], marker=dict(color=SEMAFORO[:3])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel: Total → Alerta → Fraude")
col2.plotly_chart(fig_fn, use_container_width=True)

col3, col4 = st.columns(2)
df_donut = run_query(f"SELECT REGLA_ACTIVADA, COUNT(*) AS N FROM {TABLE} WHERE REGLA_ACTIVADA IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
fig_dn = px.pie(df_donut, names="REGLA_ACTIVADA", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Regla Activada")
col3.plotly_chart(fig_dn, use_container_width=True)

df_box = run_query(f"SELECT TIPO_EVENTO, LATENCIA_EVALUACION_MS FROM {TABLE}")
fig_bx = px.box(df_box, x="TIPO_EVENTO", y="LATENCIA_EVALUACION_MS", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Latencia Evaluacion (ms) por Tipo Evento")
col4.plotly_chart(fig_bx, use_container_width=True)
