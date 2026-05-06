from app_pages.page_template import render_page

config = {
    "key": "avb",
    "table": "MGG_BANCA.CANALES_Y_EXPERIENCIA.ASISTENTES_VIRTUALES",
    "icon": ":material/smart_toy:",
    "title": "Asistentes Virtuales",
    "subtitle": "Rendimiento de chatbots bancarios con tasa de resolucion, satisfaccion y ahorro operativo.",
    "cards": [
        "Monitorea rendimiento de chatbots midiendo CSAT, confianza de intencion y tasa de resolucion.",
        "Detecta conversaciones escaladas y analiza motivos de falla para mejorar modelos continuamente.",
        "Cuantifica ahorro operativo por transacciones completadas sin intervencion humana.",
    ],
    "date_col": "FECHA_HORA_INICIO",
    "filter_cols": ["INTENCION_DETECTADA", "CANAL", "TIPO_RESOLUCION"],
    "kpi_query": """SELECT
        ROUND(AVG(SATISFACCION_CSAT), 2) AS CSAT_PROM,
        ROUND(AVG(CONFIANZA_INTENCION), 3) AS CONFIANZA_INTENCION,
        ROUND(SUM(CASE WHEN ESCALADO_A_HUMANO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_ESCALADO,
        ROUND(SUM(CASE WHEN TRANSACCION_COMPLETADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_TXN_COMPLETADA,
        ROUND(AVG(TURNOS_CONVERSACION), 1) AS TURNOS_PROM,
        ROUND(AVG(DURACION_SEGUNDOS), 1) AS DURACION_SEG,
        ROUND(SUM(CASE WHEN FEEDBACK_POSITIVO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_FEEDBACK_POSITIVO,
        COUNT(*) AS TOTAL_CONVERSACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["CSAT Prom", "Confianza Intencion", "% Escalado Humano", "% TXN Completada", "Turnos Prom", "Duracion (seg)", "% Feedback Positivo", "Total Conversaciones"],
    "kpi_formats": ["{:.2f}", "{:.3f}", "{:.2f}%", "{:.2f}%", "{:.1f}", "{:.1f} seg", "{:.2f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_INICIO) AS MES,
        ROUND(AVG(SATISFACCION_CSAT), 2) AS CSAT_PROM,
        ROUND(SUM(CASE WHEN ESCALADO_A_HUMANO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_ESCALADO
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["CSAT Prom", "% Escalado"],
    "treemap_query": """SELECT INTENCION_DETECTADA, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Intencion Detectada"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SATISFACCION_CSAT), 2) AS CSAT FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Satisfaccion por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "CSAT", "caption": "Tamano: volumen conversaciones | Color: CSAT promedio"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Asistentes Virtuales")

df_funnel = run_query(f"""SELECT
    COUNT(*) AS TOTAL,
    SUM(CASE WHEN ESCALADO_A_HUMANO = FALSE THEN 1 ELSE 0 END) AS RESUELTO_SIN_ESCALADO,
    SUM(CASE WHEN SATISFACCION_CSAT >= 4 THEN 1 ELSE 0 END) AS CSAT_4_PLUS,
    SUM(CASE WHEN FEEDBACK_POSITIVO = TRUE THEN 1 ELSE 0 END) AS FEEDBACK_POSITIVO
FROM {TABLE}""")
df_scatter = run_query(f"""SELECT CONFIANZA_INTENCION, SATISFACCION_CSAT, TIPO_RESOLUCION FROM {TABLE} SAMPLE (2000 ROWS)""")
df_donut = run_query(f"""SELECT SENTIMIENTO_CLIENTE, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
df_heatmap = run_query(f"""SELECT INTENCION_DETECTADA, CANAL, ROUND(AVG(SATISFACCION_CSAT), 2) AS CSAT FROM {TABLE} WHERE INTENCION_DETECTADA IN (SELECT INTENCION_DETECTADA FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 8) GROUP BY 1, 2""")
df_box = run_query(f"""SELECT CANAL, DURACION_SEGUNDOS / 60.0 AS DURACION_MIN FROM {TABLE}""")
df_bar = run_query(f"""SELECT INTENCION_DETECTADA, ROUND(AVG(ERRORES_BOT_CONVERSACION), 2) AS ERRORES_PROM FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 8""")

col1, col2 = st.columns(2)
with col1:
    fig = go.Figure(go.Funnel(
        y=["Total", "Resuelto sin escalado", "CSAT >= 4", "Feedback positivo"],
        x=[df_funnel["TOTAL"].iloc[0], df_funnel["RESUELTO_SIN_ESCALADO"].iloc[0], df_funnel["CSAT_4_PLUS"].iloc[0], df_funnel["FEEDBACK_POSITIVO"].iloc[0]],
        marker=dict(color=NAVY[:4])
    ))
    fig.update_layout(**CHART_LAYOUT, title="Funnel de Calidad")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig = go.Figure(go.Scatter(
        x=df_scatter["CONFIANZA_INTENCION"], y=df_scatter["SATISFACCION_CSAT"],
        mode="markers", marker=dict(opacity=0.6, size=5,
        color=[NAVY[i % len(NAVY)] for i, _ in enumerate(df_scatter["TIPO_RESOLUCION"])])
    ))
    for i, tipo in enumerate(df_scatter["TIPO_RESOLUCION"].unique()):
        subset = df_scatter[df_scatter["TIPO_RESOLUCION"] == tipo]
        fig.add_trace(go.Scatter(x=subset["CONFIANZA_INTENCION"], y=subset["SATISFACCION_CSAT"],
            mode="markers", name=tipo, marker=dict(opacity=0.6, size=5, color=NAVY[i % len(NAVY)])))
    fig.data = fig.data[1:]
    fig.update_layout(**CHART_LAYOUT, title="Confianza vs CSAT", xaxis_title="Confianza Intencion", yaxis_title="CSAT")
    st.plotly_chart(fig, use_container_width=True)

pivot = df_heatmap.pivot_table(index="INTENCION_DETECTADA", columns="CANAL", values="CSAT", aggfunc="mean")
fig = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.2f}", textfont=dict(size=10)))
fig.update_layout(**CHART_LAYOUT, title="CSAT por Intencion x Canal", height=450)
st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    fig = go.Figure(go.Pie(labels=df_donut["SENTIMIENTO_CLIENTE"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
    fig.update_layout(**CHART_LAYOUT, title="Distribucion Sentimiento Cliente")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig = go.Figure(go.Bar(x=df_bar["INTENCION_DETECTADA"], y=df_bar["ERRORES_PROM"], marker_color=NAVY[2]))
    fig.update_layout(**CHART_LAYOUT, title="Errores Promedio por Intencion (Top 8)", xaxis_title="Intencion", yaxis_title="Errores Prom")
    st.plotly_chart(fig, use_container_width=True)

fig = go.Figure()
for i, canal in enumerate(df_box["CANAL"].unique()):
    subset = df_box[df_box["CANAL"] == canal]
    fig.add_trace(go.Box(y=subset["DURACION_MIN"], name=canal, marker_color=NAVY[i % len(NAVY)]))
fig.update_layout(**CHART_LAYOUT, title="Duracion (min) por Canal", yaxis_title="Minutos")
st.plotly_chart(fig, use_container_width=True)
