from app_pages.page_template import render_page

config = {
    "key": "ixo",
    "table": "MGG_FINTECH.MONETIZACION.INTERCHANGE_OPTIMIZATION",
    "icon": ":material/currency_exchange:",
    "title": "Interchange Optimization",
    "subtitle": "Optimizacion de interchange fees con analisis por red, tipo de tarjeta y MCC.",
    "cards": ["Optimiza ingresos por interchange en cada transaccion con tarjeta segun tipo y MCC", "Analiza red, tipo tarjeta, categoria comercio e interchange aplicado para detectar oportunidades", "Maximiza revenue por transaccion procesada sin afectar experiencia del usuario ni del comercio"],
    "date_col": "FECHA_HORA",
    "filter_cols": ["RED_TARJETA", "TIPO_TARJETA", "CANAL"],
    "kpi_query": """SELECT ROUND(AVG(INTERCHANGE_RATE_PCT),3), ROUND(SUM(OPORTUNIDAD_MEJORA_COP)/1e6,2), ROUND(SUM(REVENUE_NETO_COP)/1e6,2), ROUND(AVG(MDR_TOTAL_PCT),3), ROUND(AVG(MARGEN_CONTRIBUCION_PCT),3), ROUND(AVG(MONTO_TXN_COP)/1e3,1), ROUND(SUM(CASE WHEN CONTACTLESS THEN 1 ELSE 0 END)*100.0/COUNT(*),1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Interchange %", "Oportunidad M", "Revenue M", "MDR %", "Margen %", "Monto K", "Contactless %", "Total"],
    "kpi_formats": ["{:.3f}%", "${:.2f}M", "${:.2f}M", "{:.3f}%", "{:.3f}%", "${:.1f}K", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES, ROUND(AVG(INTERCHANGE_RATE_PCT),3) AS INTERCHANGE, ROUND(AVG(MARGEN_CONTRIBUCION_PCT),3) AS MARGEN FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["INTERCHANGE", "MARGEN"],
    "treemap_query": """SELECT RED_TARJETA AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Red Tarjeta"},
    "geo_query": """SELECT CIUDAD, AVG(MARGEN_CONTRIBUCION_PCT) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Margen Contribucion por Ciudad", "color_col": "VALOR"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Interchange")

df_heat = run_query(f"SELECT RED_TARJETA, TIPO_TARJETA, ROUND(AVG(MARGEN_CONTRIBUCION_PCT)*100,2) AS MARGEN FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="RED_TARJETA", columns="TIPO_TARJETA", values="MARGEN").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.2f}%"))
fig_heat.update_layout(**CHART_LAYOUT, title="Margen Contribucion: Red × Tipo Tarjeta")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT INTERCHANGE_RATE_PCT, INTERCHANGE_OPTIMO_PCT AS INTERCHANGE_OPTIMO, RED_TARJETA FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="INTERCHANGE_RATE_PCT", y="INTERCHANGE_OPTIMO", color="RED_TARJETA", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="Interchange Actual vs Optimo")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_bar = run_query(f"SELECT MCC_DESCRIPCION AS MCC, ROUND(SUM(OPORTUNIDAD_MEJORA_COP)/1e6,2) AS OPORTUNIDAD_M FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
    fig_bar = px.bar(df_bar, x="MCC", y="OPORTUNIDAD_M", color_discrete_sequence=SEMAFORO)
    fig_bar.update_layout(**CHART_LAYOUT, title="Oportunidad Mejora por MCC (Top 8)")
    st.plotly_chart(fig_bar, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    df_donut = run_query(f"SELECT CANAL, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["CANAL"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Canal")
    st.plotly_chart(fig_donut, use_container_width=True)
with col4:
    df_box = run_query(f"SELECT RED_TARJETA, MARGEN_CONTRIBUCION_PCT*100 AS MARGEN_PCT FROM {TABLE} LIMIT 3000")
    fig_box = px.box(df_box, x="RED_TARJETA", y="MARGEN_PCT", color_discrete_sequence=SEMAFORO)
    fig_box.update_layout(**CHART_LAYOUT, title="Margen Contribucion % por Red")
    st.plotly_chart(fig_box, use_container_width=True)
