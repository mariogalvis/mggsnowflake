from app_pages.page_template import render_page

config = {
    "key": "nbo",
    "table": "MGG_BANCA.CLIENTE_Y_CRECIMIENTO.NEXT_BEST_OFFER",
    "icon": ":material/recommend:",
    "title": "Next Best Offer",
    "subtitle": "Recomendacion inteligente de siguiente mejor producto basada en propension y contexto del cliente.",
    "cards": [
        "Identifica el producto ideal para cada cliente usando modelos de propension y contexto transaccional.",
        "Prioriza contactos por canal optimo y momento adecuado maximizando probabilidad de aceptacion.",
        "Genera revenue incremental estimado por oferta con seguimiento de conversion end-to-end.",
    ],
    "date_col": "FECHA_RECOMENDACION",
    "filter_cols": ["OFERTA_RECOMENDADA", "SEGMENTO_CLIENTE", "CANAL_RECOMENDADO"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_PROPENSION), 3) AS SCORE_PROPENSION_PROM,
        ROUND(SUM(CASE WHEN OFERTA_ACEPTADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_ACEPTADAS,
        ROUND(SUM(CASE WHEN OFERTA_VISTA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_VISTAS,
        ROUND(SUM(REVENUE_POTENCIAL_COP) / 1e6, 1) AS REVENUE_POTENCIAL_M,
        ROUND(AVG(ENGAGEMENT_SCORE), 2) AS ENGAGEMENT_PROM,
        ROUND(AVG(SATISFACCION_NPS_NORM), 2) AS NPS_PROM,
        ROUND(AVG(OFERTAS_PREVIAS_RECHAZADAS), 1) AS OFERTAS_RECHAZADAS_PROM,
        COUNT(*) AS TOTAL_RECOMENDACIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score Propension Prom", "% Aceptadas", "% Vistas", "Revenue Potencial (M)", "Engagement Prom", "NPS Prom", "Ofertas Rechazadas Prom", "Total Recomendaciones"],
    "kpi_formats": ["{:.3f}", "{:.2f}%", "{:.2f}%", "${:.1f}M", "{:.2f}", "{:.2f}", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_RECOMENDACION) AS MES,
        ROUND(AVG(SCORE_PROPENSION), 3) AS SCORE_PROM,
        ROUND(SUM(CASE WHEN OFERTA_ACEPTADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_ACEPTADAS
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Score Prom", "% Aceptadas"],
    "treemap_query": """SELECT OFERTA_RECOMENDADA, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Oferta Recomendada"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_PROPENSION), 3) AS SCORE_PROPENSION FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Propension por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "SCORE_PROPENSION", "caption": "Tamano: volumen recomendaciones | Color: score propension promedio"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Next Best Offer")

# FUNNEL - FULL WIDTH
df_funnel = run_query(f"""SELECT
    COUNT(*) AS RECOMENDADAS,
    SUM(CASE WHEN OFERTA_VISTA = TRUE THEN 1 ELSE 0 END) AS VISTAS,
    SUM(CASE WHEN OFERTA_ACEPTADA = TRUE THEN 1 ELSE 0 END) AS ACEPTADAS
FROM {TABLE}""")
fig_funnel = go.Figure(go.Funnel(
    y=["Recomendadas", "Vistas", "Aceptadas"],
    x=[df_funnel["RECOMENDADAS"].iloc[0], df_funnel["VISTAS"].iloc[0], df_funnel["ACEPTADAS"].iloc[0]],
    marker=dict(color=NAVY[:3])
))
fig_funnel.update_layout(**CHART_LAYOUT, title="Funnel de Conversion de Ofertas")
st.plotly_chart(fig_funnel, use_container_width=True)

# HEATMAP - FULL WIDTH
df_heat = run_query(f"""SELECT OFERTA_RECOMENDADA, CANAL_RECOMENDADO,
    ROUND(SUM(CASE WHEN OFERTA_ACEPTADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS PCT_ACEPTACION
FROM {TABLE} GROUP BY 1, 2""")
pivot = df_heat.pivot(index="OFERTA_RECOMENDADA", columns="CANAL_RECOMENDADO", values="PCT_ACEPTACION").fillna(0)
fig_heat = go.Figure(go.Heatmap(
    z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
    colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.1f}%"
))
fig_heat.update_layout(**CHART_LAYOUT, title="% Aceptacion por Oferta × Canal")
st.plotly_chart(fig_heat, use_container_width=True)

c1, c2 = st.columns(2)

df_scatter = run_query(f"""SELECT SCORE_PROPENSION, ENGAGEMENT_SCORE, SEGMENTO_CLIENTE, REVENUE_POTENCIAL_COP FROM {TABLE}""")
fig_scatter = px.scatter(df_scatter, x="SCORE_PROPENSION", y="ENGAGEMENT_SCORE",
    color="SEGMENTO_CLIENTE", size="REVENUE_POTENCIAL_COP",
    color_discrete_sequence=NAVY)
fig_scatter.update_layout(**CHART_LAYOUT, title="Propension vs Engagement")
c1.plotly_chart(fig_scatter, use_container_width=True)

df_donut = run_query(f"""SELECT OFERTA_RECOMENDADA, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
fig_donut = go.Figure(go.Pie(labels=df_donut["OFERTA_RECOMENDADA"], values=df_donut["N"], hole=0.5,
    marker=dict(colors=NAVY)))
fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Oferta")
c2.plotly_chart(fig_donut, use_container_width=True)

# BOX PLOT - FULL WIDTH
df_box = run_query(f"""SELECT SEGMENTO_CLIENTE, SCORE_PROPENSION FROM {TABLE}""")
fig_box = px.box(df_box, x="SEGMENTO_CLIENTE", y="SCORE_PROPENSION", color="SEGMENTO_CLIENTE",
    color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Score Propension por Segmento", showlegend=False)
st.plotly_chart(fig_box, use_container_width=True)

df_rev = run_query(f"""SELECT OFERTA_RECOMENDADA, ROUND(SUM(REVENUE_POTENCIAL_COP)/1e6, 1) AS REVENUE_M
FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
fig_rev = go.Figure(go.Bar(x=df_rev["OFERTA_RECOMENDADA"], y=df_rev["REVENUE_M"], marker_color=NAVY[3]))
fig_rev.update_layout(**CHART_LAYOUT, title="Revenue Potencial por Oferta (M COP)", yaxis_title="Millones COP")
st.plotly_chart(fig_rev, use_container_width=True)
