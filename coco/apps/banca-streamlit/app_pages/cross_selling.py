from app_pages.page_template import render_page

config = {
    "key": "crs",
    "table": "MGG_BANCA.CLIENTE_Y_CRECIMIENTO.CROSS_SELLING",
    "icon": ":material/shopping_cart:",
    "title": "Cross Selling",
    "subtitle": "Identificacion de oportunidades de venta cruzada con scoring de afinidad por producto.",
    "cards": [
        "Detecta oportunidades de venta cruzada basadas en afinidad de producto y momento optimo de contacto.",
        "Prioriza oportunidades por probabilidad de conversion y revenue estimado por canal optimo.",
        "Mide conversion real vs estimada para calibrar modelos y optimizar estrategias de contacto.",
    ],
    "date_col": "FECHA_IDENTIFICACION",
    "filter_cols": ["PRODUCTO_DESTINO", "CANAL_OPTIMO", "ESTADO_OPORTUNIDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(PROBABILIDAD_CONVERSION), 3) AS PROB_CONVERSION_PROM,
        ROUND(SUM(CASE WHEN CONVERSION_EXITOSA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_CONVERSION,
        ROUND(SUM(REVENUE_ESTIMADO_COP) / 1e6, 1) AS REVENUE_EST_M,
        ROUND(SUM(REVENUE_REAL_COP) / 1e6, 1) AS REVENUE_REAL_M,
        ROUND(AVG(AFINIDAD_PRODUCTO), 3) AS AFINIDAD_PROM,
        ROUND(AVG(INTENTOS_CONTACTO), 1) AS INTENTOS_PROM,
        ROUND(AVG(ENGAGEMENT_DIGITAL), 2) AS ENGAGEMENT_DIGITAL_PROM,
        COUNT(*) AS TOTAL_OPORTUNIDADES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Prob Conversion Prom", "% Conversion Exitosa", "Revenue Est (M)", "Revenue Real (M)", "Afinidad Prom", "Intentos Prom", "Engagement Digital Prom", "Total Oportunidades"],
    "kpi_formats": ["{:.3f}", "{:.2f}%", "${:.1f}M", "${:.1f}M", "{:.3f}", "{:.1f}", "{:.2f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_IDENTIFICACION) AS MES,
        ROUND(AVG(PROBABILIDAD_CONVERSION), 3) AS PROB_CONVERSION,
        ROUND(SUM(CASE WHEN CONVERSION_EXITOSA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_CONVERSION
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Prob Conversion", "% Conversion"],
    "treemap_query": """SELECT PRODUCTO_DESTINO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Producto Destino"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(PROBABILIDAD_CONVERSION), 3) AS PROB_CONVERSION FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Conversion por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "PROB_CONVERSION", "caption": "Tamano: volumen oportunidades | Color: probabilidad conversion promedio"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Cross Selling")

# HEATMAP - FULL WIDTH
df_heat = run_query(f"""SELECT PRODUCTO_ORIGEN, PRODUCTO_DESTINO,
    ROUND(SUM(CASE WHEN CONVERSION_EXITOSA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS PCT_CONVERSION
FROM {TABLE} GROUP BY 1, 2""")
pivot = df_heat.pivot(index="PRODUCTO_ORIGEN", columns="PRODUCTO_DESTINO", values="PCT_CONVERSION").fillna(0)
fig_heat = go.Figure(go.Heatmap(
    z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
    colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.1f}%"
))
fig_heat.update_layout(**CHART_LAYOUT, title="% Conversion por Producto Origen × Destino")
st.plotly_chart(fig_heat, use_container_width=True)

c1, c2 = st.columns(2)

df_scatter = run_query(f"""SELECT PROBABILIDAD_CONVERSION, AFINIDAD_PRODUCTO, ESTADO_OPORTUNIDAD FROM {TABLE}""")
fig_scatter = px.scatter(df_scatter, x="PROBABILIDAD_CONVERSION", y="AFINIDAD_PRODUCTO",
    color="ESTADO_OPORTUNIDAD", color_discrete_sequence=NAVY)
fig_scatter.update_layout(**CHART_LAYOUT, title="Probabilidad vs Afinidad")
c1.plotly_chart(fig_scatter, use_container_width=True)

df_funnel = run_query(f"""SELECT
    SUM(CASE WHEN ESTADO_OPORTUNIDAD IN ('Identificada','Contactada','Convertida') THEN 1 ELSE 0 END) AS IDENTIFICADAS,
    SUM(CASE WHEN ESTADO_OPORTUNIDAD IN ('Contactada','Convertida') THEN 1 ELSE 0 END) AS CONTACTADAS,
    SUM(CASE WHEN CONVERSION_EXITOSA = TRUE THEN 1 ELSE 0 END) AS CONVERTIDAS
FROM {TABLE}""")
fig_funnel = go.Figure(go.Funnel(
    y=["Identificadas", "Contactadas", "Convertidas"],
    x=[df_funnel["IDENTIFICADAS"].iloc[0], df_funnel["CONTACTADAS"].iloc[0], df_funnel["CONVERTIDAS"].iloc[0]],
    marker=dict(color=NAVY[:3])
))
fig_funnel.update_layout(**CHART_LAYOUT, title="Funnel de Oportunidades")
c2.plotly_chart(fig_funnel, use_container_width=True)

c3, c4 = st.columns(2)

df_donut = run_query(f"""SELECT TIPO_VENTA, COUNT(*) AS N FROM {TABLE} GROUP BY 1""")
fig_donut = go.Figure(go.Pie(labels=df_donut["TIPO_VENTA"], values=df_donut["N"], hole=0.5,
    marker=dict(colors=NAVY)))
fig_donut.update_layout(**CHART_LAYOUT, title="Tipo de Venta")
c3.plotly_chart(fig_donut, use_container_width=True)

df_canal = run_query(f"""SELECT CANAL_OPTIMO,
    ROUND(SUM(CASE WHEN CONVERSION_EXITOSA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS PCT_CONVERSION
FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
fig_canal = go.Figure(go.Bar(x=df_canal["CANAL_OPTIMO"], y=df_canal["PCT_CONVERSION"], marker_color=NAVY[3]))
fig_canal.update_layout(**CHART_LAYOUT, title="% Conversion por Canal", yaxis_title="% Conversion")
c4.plotly_chart(fig_canal, use_container_width=True)

# BOX PLOT - FULL WIDTH
df_box = run_query(f"""SELECT PRODUCTO_DESTINO, ROUND(REVENUE_ESTIMADO_COP/1e3, 1) AS REVENUE_K
FROM {TABLE} WHERE PRODUCTO_DESTINO IN (SELECT PRODUCTO_DESTINO FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 8)""")
fig_box = px.box(df_box, x="PRODUCTO_DESTINO", y="REVENUE_K", color="PRODUCTO_DESTINO",
    color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Revenue Estimado por Producto Destino (K COP, Top 8)", showlegend=False)
st.plotly_chart(fig_box, use_container_width=True)
