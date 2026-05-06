from app_pages.page_template import render_page

config = {
    "key": "jrd",
    "table": "MGG_BANCA.CANALES_Y_EXPERIENCIA.JOURNEYS_DIGITALES",
    "icon": ":material/route:",
    "title": "Journeys Digitales",
    "subtitle": "Analisis de funnels digitales para reducir abandono y mejorar conversion end-to-end.",
    "cards": [
        "Rastrea journeys digitales completos midiendo pasos completados, abandonos y puntos de friccion.",
        "Compara variantes A/B test con tasa de conversion predicha y experiencia score por funnel.",
        "Identifica patrones de retorno post-abandono para optimizar retargeting y recuperacion de journeys.",
    ],
    "date_col": "FECHA_HORA_INICIO",
    "filter_cols": ["TIPO_FUNNEL", "DISPOSITIVO", "VARIANTE_AB_TEST"],
    "kpi_query": """SELECT
        ROUND(SUM(CASE WHEN JOURNEY_COMPLETADO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_COMPLETADO,
        ROUND(AVG(TASA_CONVERSION_PREDICHA), 3) AS TASA_CONVERSION_PREDICHA,
        ROUND(AVG(EXPERIENCIA_SCORE), 2) AS EXPERIENCIA_SCORE,
        ROUND(AVG(TIEMPO_TOTAL_SEGUNDOS) / 60.0, 1) AS TIEMPO_TOTAL_MIN,
        ROUND(SUM(CASE WHEN JOURNEY_COMPLETADO = FALSE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_ABANDONO,
        ROUND(AVG(ERRORES_ENCONTRADOS), 1) AS ERRORES_PROM,
        ROUND(AVG(PANTALLAS_VISITADAS), 1) AS PANTALLAS_PROM,
        COUNT(*) AS TOTAL_JOURNEYS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["% Completado", "Tasa Conversion Predicha", "Experiencia Score", "Tiempo Total (min)", "% Abandono", "Errores Prom", "Pantallas Prom", "Total Journeys"],
    "kpi_formats": ["{:.2f}%", "{:.3f}", "{:.2f}", "{:.1f} min", "{:.2f}%", "{:.1f}", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_INICIO) AS MES,
        ROUND(SUM(CASE WHEN JOURNEY_COMPLETADO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_COMPLETADO,
        ROUND(AVG(EXPERIENCIA_SCORE), 2) AS EXPERIENCIA_SCORE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["% Completado", "Experiencia Score"],
    "treemap_query": """SELECT TIPO_FUNNEL, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Funnel"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(EXPERIENCIA_SCORE), 2) AS EXPERIENCIA_SCORE FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Experiencia por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "EXPERIENCIA_SCORE", "caption": "Tamano: volumen journeys | Color: experiencia score promedio"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Journeys Digitales")

df_funnel = run_query(f"""SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN PASOS_COMPLETADOS::FLOAT / NULLIF(TOTAL_PASOS_FUNNEL, 0) >= 0.75 THEN 1 ELSE 0 END) AS PASO_75, SUM(CASE WHEN JOURNEY_COMPLETADO = TRUE THEN 1 ELSE 0 END) AS COMPLETADO FROM {TABLE}""")
df_heatmap = run_query(f"""SELECT TIPO_FUNNEL, DISPOSITIVO, ROUND(SUM(CASE WHEN JOURNEY_COMPLETADO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_COMPLETADO FROM {TABLE} GROUP BY 1, 2""")
df_scatter = run_query(f"""SELECT TASA_CONVERSION_PREDICHA, EXPERIENCIA_SCORE, TIPO_FUNNEL, PANTALLAS_VISITADAS FROM {TABLE} SAMPLE (2000 ROWS)""")
df_donut = run_query(f"""SELECT PUNTO_ABANDONO, COUNT(*) AS N FROM {TABLE} WHERE PUNTO_ABANDONO IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 6""")
df_box = run_query(f"""SELECT TIPO_FUNNEL, TIEMPO_TOTAL_SEGUNDOS / 60.0 AS TIEMPO_MIN FROM {TABLE}""")
df_bar = run_query(f"""SELECT VARIANTE_AB_TEST, ROUND(SUM(CASE WHEN JOURNEY_COMPLETADO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_CONVERSION FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")

col1, col2 = st.columns(2)
with col1:
    fig = go.Figure(go.Funnel(
        y=["Total Journeys", ">= 75% pasos", "Completado"],
        x=[df_funnel["TOTAL"].iloc[0], df_funnel["PASO_75"].iloc[0], df_funnel["COMPLETADO"].iloc[0]],
        marker=dict(color=NAVY[:3])
    ))
    fig.update_layout(**CHART_LAYOUT, title="Funnel de Completitud")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig = go.Figure(go.Pie(labels=df_donut["PUNTO_ABANDONO"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
    fig.update_layout(**CHART_LAYOUT, title="Distribucion Punto de Abandono (Top 6)")
    st.plotly_chart(fig, use_container_width=True)

pivot = df_heatmap.pivot_table(index="TIPO_FUNNEL", columns="DISPOSITIVO", values="PCT_COMPLETADO", aggfunc="mean")
fig = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.1f}%", textfont=dict(size=10)))
fig.update_layout(**CHART_LAYOUT, title="% Completado por Funnel x Dispositivo", height=400)
st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    fig = go.Figure()
    for i, tipo in enumerate(df_scatter["TIPO_FUNNEL"].unique()):
        subset = df_scatter[df_scatter["TIPO_FUNNEL"] == tipo]
        fig.add_trace(go.Scatter(x=subset["TASA_CONVERSION_PREDICHA"], y=subset["EXPERIENCIA_SCORE"],
            mode="markers", name=tipo, marker=dict(opacity=0.6, size=subset["PANTALLAS_VISITADAS"] / subset["PANTALLAS_VISITADAS"].max() * 15 + 3, color=NAVY[i % len(NAVY)])))
    fig.update_layout(**CHART_LAYOUT, title="Conversion Predicha vs Experiencia", xaxis_title="Tasa Conversion Predicha", yaxis_title="Experiencia Score")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig = go.Figure(go.Bar(x=df_bar["VARIANTE_AB_TEST"], y=df_bar["PCT_CONVERSION"], marker_color=NAVY[2]))
    fig.update_layout(**CHART_LAYOUT, title="% Conversion por Variante A/B", xaxis_title="Variante", yaxis_title="% Conversion")
    st.plotly_chart(fig, use_container_width=True)

fig = go.Figure()
for i, funnel in enumerate(df_box["TIPO_FUNNEL"].unique()):
    subset = df_box[df_box["TIPO_FUNNEL"] == funnel]
    fig.add_trace(go.Box(y=subset["TIEMPO_MIN"], name=funnel, marker_color=NAVY[i % len(NAVY)]))
fig.update_layout(**CHART_LAYOUT, title="Tiempo Total (min) por Tipo Funnel", yaxis_title="Minutos")
st.plotly_chart(fig, use_container_width=True)
