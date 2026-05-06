from app_pages.page_template import render_page

config = {
    "key": "rrt",
    "table": "MGG_BANCA.CANALES_Y_EXPERIENCIA.RECOMENDACIONES_REAL_TIME",
    "icon": ":material/auto_awesome:",
    "title": "Recomendaciones RT",
    "subtitle": "Motor de recomendaciones en tiempo real con estrategias bandit y medicion de uplift.",
    "cards": [
        "Genera recomendaciones en tiempo real con latencia sub-segundo usando estrategias multi-armed bandit.",
        "Mide funnel completo: vista, click y conversion con revenue generado por recomendacion.",
        "Optimiza fatigue score y posicion en carousel para maximizar CTR y uplift estimado.",
    ],
    "date_col": "FECHA_HORA_RECOMENDACION",
    "filter_cols": ["PRODUCTO_RECOMENDADO", "CANAL_ENTREGA", "ESTRATEGIA_BANDIT"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_RELEVANCIA), 3) AS SCORE_RELEVANCIA,
        ROUND(SUM(CASE WHEN RECOMENDACION_VISTA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_VISTA,
        ROUND(SUM(CASE WHEN RECOMENDACION_CLICK = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_CLICK,
        ROUND(SUM(CASE WHEN CONVERSION = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_CONVERSION,
        ROUND(SUM(REVENUE_GENERADO_COP) / 1e6, 1) AS REVENUE_M,
        ROUND(AVG(LATENCIA_RESPUESTA_MS), 1) AS LATENCIA_MS,
        ROUND(AVG(UPLIFT_ESTIMADO), 3) AS UPLIFT_ESTIMADO,
        COUNT(*) AS TOTAL_RECOMENDACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score Relevancia", "% Vista", "% Click", "% Conversion", "Revenue (M)", "Latencia (ms)", "Uplift Estimado", "Total Recomendaciones"],
    "kpi_formats": ["{:.3f}", "{:.2f}%", "{:.2f}%", "{:.2f}%", "${:.1f}M", "{:.1f} ms", "{:.3f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_RECOMENDACION) AS MES,
        ROUND(SUM(CASE WHEN CONVERSION = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_CONVERSION,
        ROUND(AVG(UPLIFT_ESTIMADO), 3) AS UPLIFT_ESTIMADO
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["% Conversion", "Uplift Estimado"],
    "treemap_query": """SELECT PRODUCTO_RECOMENDADO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Producto Recomendado"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(SUM(CASE WHEN CONVERSION = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS TASA_CONVERSION FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Conversion por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "TASA_CONVERSION", "caption": "Tamano: volumen recomendaciones | Color: tasa conversion"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Recomendaciones Real-Time")

df_funnel = run_query(f"""SELECT COUNT(*) AS RECOMENDADAS, SUM(CASE WHEN RECOMENDACION_VISTA = TRUE THEN 1 ELSE 0 END) AS VISTAS, SUM(CASE WHEN RECOMENDACION_CLICK = TRUE THEN 1 ELSE 0 END) AS CLICKS, SUM(CASE WHEN CONVERSION = TRUE THEN 1 ELSE 0 END) AS CONVERSIONES FROM {TABLE}""")
df_heatmap = run_query(f"""SELECT PRODUCTO_RECOMENDADO, CANAL_ENTREGA, ROUND(SUM(CASE WHEN CONVERSION = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_CONVERSION FROM {TABLE} GROUP BY 1, 2""")
df_scatter = run_query(f"""SELECT SCORE_RELEVANCIA, UPLIFT_ESTIMADO, ESTRATEGIA_BANDIT, REVENUE_GENERADO_COP FROM {TABLE} SAMPLE (2000 ROWS)""")
df_donut = run_query(f"""SELECT CONTEXTO_TRIGGER, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
df_box = run_query(f"""SELECT CANAL_ENTREGA, LATENCIA_RESPUESTA_MS FROM {TABLE}""")
df_bar = run_query(f"""SELECT PRODUCTO_RECOMENDADO, ROUND(SUM(REVENUE_GENERADO_COP) / 1e6, 1) AS REVENUE_M FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 8""")

col1, col2 = st.columns(2)
with col1:
    fig = go.Figure(go.Funnel(
        y=["Recomendadas", "Vistas", "Click", "Conversion"],
        x=[df_funnel["RECOMENDADAS"].iloc[0], df_funnel["VISTAS"].iloc[0], df_funnel["CLICKS"].iloc[0], df_funnel["CONVERSIONES"].iloc[0]],
        marker=dict(color=NAVY[:4])
    ))
    fig.update_layout(**CHART_LAYOUT, title="Funnel de Conversion")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig = go.Figure(go.Pie(labels=df_donut["CONTEXTO_TRIGGER"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
    fig.update_layout(**CHART_LAYOUT, title="Distribucion por Contexto Trigger")
    st.plotly_chart(fig, use_container_width=True)

pivot = df_heatmap.pivot_table(index="PRODUCTO_RECOMENDADO", columns="CANAL_ENTREGA", values="PCT_CONVERSION", aggfunc="mean")
fig = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.1f}%", textfont=dict(size=10)))
fig.update_layout(**CHART_LAYOUT, title="% Conversion por Producto x Canal", height=450)
st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    fig = go.Figure()
    for i, strat in enumerate(df_scatter["ESTRATEGIA_BANDIT"].unique()):
        subset = df_scatter[df_scatter["ESTRATEGIA_BANDIT"] == strat]
        fig.add_trace(go.Scatter(x=subset["SCORE_RELEVANCIA"], y=subset["UPLIFT_ESTIMADO"],
            mode="markers", name=strat, marker=dict(opacity=0.6, size=subset["REVENUE_GENERADO_COP"] / subset["REVENUE_GENERADO_COP"].max() * 15 + 3, color=NAVY[i % len(NAVY)])))
    fig.update_layout(**CHART_LAYOUT, title="Relevancia vs Uplift", xaxis_title="Score Relevancia", yaxis_title="Uplift Estimado")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig = go.Figure(go.Bar(x=df_bar["PRODUCTO_RECOMENDADO"], y=df_bar["REVENUE_M"], marker_color=NAVY[2]))
    fig.update_layout(**CHART_LAYOUT, title="Revenue Total por Producto (Top 8, M COP)", xaxis_title="Producto", yaxis_title="Revenue (M COP)")
    st.plotly_chart(fig, use_container_width=True)

fig = go.Figure()
for i, canal in enumerate(df_box["CANAL_ENTREGA"].unique()):
    subset = df_box[df_box["CANAL_ENTREGA"] == canal]
    fig.add_trace(go.Box(y=subset["LATENCIA_RESPUESTA_MS"], name=canal, marker_color=NAVY[i % len(NAVY)]))
fig.update_layout(**CHART_LAYOUT, title="Latencia Respuesta (ms) por Canal", yaxis_title="Milisegundos")
st.plotly_chart(fig, use_container_width=True)
