from app_pages.page_template import render_page

config = {
    "key": "scc",
    "table": "MGG_BANCA.CANALES_Y_EXPERIENCIA.SENTIMIENTO_CALL_CENTER",
    "icon": ":material/call:",
    "title": "Sentimiento Call Center",
    "subtitle": "Analisis de sentimiento en llamadas con NLP para detectar insatisfaccion y riesgo de perdida.",
    "cards": [
        "Analiza sentimiento en tiempo real durante llamadas detectando escalamiento emocional y riesgo de perdida.",
        "Mide calidad de agentes con score integrado de resolucion, empatia y venta en llamada.",
        "Correlaciona CSAT post-llamada con indicadores de sentimiento para mejorar protocolos de atencion.",
    ],
    "date_col": "FECHA_HORA_LLAMADA",
    "filter_cols": ["TEMA_PRINCIPAL", "TIPO_RESOLUCION", "SENTIMIENTO_FIN"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_SENTIMIENTO_PROMEDIO), 3) AS SCORE_SENTIMIENTO_PROM,
        ROUND(AVG(CSAT_POST_LLAMADA), 2) AS CSAT_PROM,
        ROUND(SUM(CASE WHEN RIESGO_PERDIDA_CLIENTE = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_RIESGO_PERDIDA,
        ROUND(SUM(CASE WHEN ESCALADO_SUPERVISOR = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_ESCALADO,
        ROUND(AVG(DURACION_SEGUNDOS) / 60.0, 1) AS DURACION_PROM_MIN,
        ROUND(AVG(TIEMPO_ESPERA_COLA_SEG), 1) AS ESPERA_COLA_SEG,
        ROUND(SUM(CASE WHEN VENTA_EN_LLAMADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_VENTA,
        COUNT(*) AS TOTAL_LLAMADAS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score Sentimiento Prom", "CSAT Prom", "% Riesgo Perdida", "% Escalado Supervisor", "Duracion Prom (min)", "Espera Cola (seg)", "% Venta en Llamada", "Total Llamadas"],
    "kpi_formats": ["{:.3f}", "{:.2f}", "{:.2f}%", "{:.2f}%", "{:.1f} min", "{:.1f} seg", "{:.2f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_LLAMADA) AS MES,
        ROUND(AVG(SCORE_SENTIMIENTO_PROMEDIO), 3) AS SCORE_SENTIMIENTO,
        ROUND(AVG(CSAT_POST_LLAMADA), 2) AS CSAT_PROM
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Score Sentimiento", "CSAT Prom"],
    "treemap_query": """SELECT TEMA_PRINCIPAL, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tema Principal"},
    "geo_query": """SELECT CIUDAD_CLIENTE AS CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_SENTIMIENTO_PROMEDIO), 3) AS SCORE_SENTIMIENTO FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Sentimiento por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "SCORE_SENTIMIENTO", "caption": "Tamano: volumen llamadas | Color: score sentimiento promedio"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Sentimiento Call Center")

df_heatmap = run_query(f"""SELECT TEMA_PRINCIPAL, SENTIMIENTO_FIN, ROUND(AVG(SCORE_SENTIMIENTO_PROMEDIO), 3) AS SCORE FROM {TABLE} GROUP BY 1, 2""")
df_scatter = run_query(f"""SELECT SCORE_SENTIMIENTO_PROMEDIO, CSAT_POST_LLAMADA, TIPO_RESOLUCION, DURACION_SEGUNDOS FROM {TABLE} SAMPLE (2000 ROWS)""")
df_donut = run_query(f"""SELECT SENTIMIENTO_FIN, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
df_funnel = run_query(f"""SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN RIESGO_PERDIDA_CLIENTE = TRUE THEN 1 ELSE 0 END) AS RIESGO_PERDIDA, SUM(CASE WHEN ESCALADO_SUPERVISOR = TRUE THEN 1 ELSE 0 END) AS ESCALADO FROM {TABLE}""")
df_box = run_query(f"""SELECT TEMA_PRINCIPAL, TIEMPO_ESPERA_COLA_SEG FROM {TABLE} WHERE TEMA_PRINCIPAL IN (SELECT TEMA_PRINCIPAL FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 8)""")
df_bar = run_query(f"""SELECT TIPO_RESOLUCION, ROUND(AVG(CSAT_POST_LLAMADA), 2) AS CSAT, ROUND(AVG(SCORE_CALIDAD_AGENTE), 2) AS CALIDAD_AGENTE FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")

pivot = df_heatmap.pivot_table(index="TEMA_PRINCIPAL", columns="SENTIMIENTO_FIN", values="SCORE", aggfunc="mean")
fig = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.3f}", textfont=dict(size=10)))
fig.update_layout(**CHART_LAYOUT, title="Score Sentimiento por Tema x Sentimiento Final", height=450)
st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    fig = go.Figure()
    for i, tipo in enumerate(df_scatter["TIPO_RESOLUCION"].unique()):
        subset = df_scatter[df_scatter["TIPO_RESOLUCION"] == tipo]
        fig.add_trace(go.Scatter(x=subset["SCORE_SENTIMIENTO_PROMEDIO"], y=subset["CSAT_POST_LLAMADA"],
            mode="markers", name=tipo, marker=dict(opacity=0.6, size=subset["DURACION_SEGUNDOS"] / subset["DURACION_SEGUNDOS"].max() * 15 + 3, color=NAVY[i % len(NAVY)])))
    fig.update_layout(**CHART_LAYOUT, title="Score Sentimiento vs CSAT", xaxis_title="Score Sentimiento", yaxis_title="CSAT Post-Llamada")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig = go.Figure(go.Pie(labels=df_donut["SENTIMIENTO_FIN"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
    fig.update_layout(**CHART_LAYOUT, title="Distribucion Sentimiento Final")
    st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    fig = go.Figure(go.Funnel(
        y=["Total Llamadas", "Riesgo Perdida", "Escalado Supervisor"],
        x=[df_funnel["TOTAL"].iloc[0], df_funnel["RIESGO_PERDIDA"].iloc[0], df_funnel["ESCALADO"].iloc[0]],
        marker=dict(color=NAVY[:3])
    ))
    fig.update_layout(**CHART_LAYOUT, title="Funnel de Riesgo")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df_bar["TIPO_RESOLUCION"], y=df_bar["CSAT"], name="CSAT", marker_color=NAVY[0]))
    fig.add_trace(go.Bar(x=df_bar["TIPO_RESOLUCION"], y=df_bar["CALIDAD_AGENTE"], name="Calidad Agente", marker_color=NAVY[3]))
    fig.update_layout(**CHART_LAYOUT, barmode="group", title="CSAT y Calidad Agente por Resolucion", yaxis_title="Score")
    st.plotly_chart(fig, use_container_width=True)

fig = go.Figure()
for i, tema in enumerate(df_box["TEMA_PRINCIPAL"].unique()):
    subset = df_box[df_box["TEMA_PRINCIPAL"] == tema]
    fig.add_trace(go.Box(y=subset["TIEMPO_ESPERA_COLA_SEG"], name=tema, marker_color=NAVY[i % len(NAVY)]))
fig.update_layout(**CHART_LAYOUT, title="Tiempo Espera Cola (seg) por Tema Principal", yaxis_title="Segundos")
st.plotly_chart(fig, use_container_width=True)
