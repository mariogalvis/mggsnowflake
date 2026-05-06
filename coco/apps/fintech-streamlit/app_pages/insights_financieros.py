from app_pages.page_template import render_page

config = {
    "key": "inf",
    "table": "MGG_FINTECH.SOPORTE_FINANCIERO_INTELIGENTE.INSIGHTS_FINANCIEROS",
    "icon": ":material/lightbulb:",
    "title": "Insights Financieros",
    "subtitle": "Generacion de insights personalizados de gasto y ahorro con engagement tracking.",
    "cards": ["Insights automaticos por AI sobre comportamiento financiero: gastos, ingresos, tendencias", "Genera mensajes personalizados como 'gastaste 20% mas en restaurantes este mes' con contexto", "Empodera al usuario con informacion accionable sobre su dinero sin analisis manual"],
    "date_col": "FECHA_GENERACION",
    "filter_cols": ["TIPO_INSIGHT", "CANAL_ENTREGA", "TONO_MENSAJE"],
    "kpi_query": """SELECT ROUND(SUM(CASE WHEN INSIGHT_VISTO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN INSIGHT_CLICKEADO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN ACCION_TOMADA THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(RATING_UTILIDAD),1), ROUND(AVG(RELEVANCIA_SCORE)*100,1), ROUND(AVG(AHORRO_SUGERIDO_COP)/1e3,1), ROUND(AVG(FATIGUE_SCORE)*100,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Visto %", "Clickeado %", "Accion %", "Rating", "Relevancia %", "Ahorro (K)", "Fatigue %", "Total insights"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.1f}", "{:.1f}%", "${:.1f}K", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_GENERACION) AS MES, ROUND(SUM(CASE WHEN INSIGHT_VISTO THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS VISTO_PCT, ROUND(SUM(CASE WHEN ACCION_TOMADA THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS ACCION_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["VISTO_PCT", "ACCION_PCT"],
    "treemap_query": """SELECT TIPO_INSIGHT AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo Insight"},
    "geo_query": """SELECT CIUDAD, AVG(RATING_UTILIDAD) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Rating Utilidad por Ciudad", "color_col": "VALOR"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Insights Financieros")

df_heatmap = run_query(f"SELECT TIPO_INSIGHT, CANAL_ENTREGA, ROUND(AVG(RATING_UTILIDAD),2) AS RATING FROM {TABLE} GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="TIPO_INSIGHT", columns="CANAL_ENTREGA", values="RATING", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.2f}"))
fig_hm.update_layout(**CHART_LAYOUT, title="Rating Utilidad por Tipo Insight × Canal", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT RELEVANCIA_SCORE, RATING_UTILIDAD, TONO_MENSAJE FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="RELEVANCIA_SCORE", y="RATING_UTILIDAD", color="TONO_MENSAJE", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Relevancia vs Rating Utilidad")
col1.plotly_chart(fig_sc, use_container_width=True)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN INSIGHT_VISTO THEN 1 ELSE 0 END) AS VISTO, SUM(CASE WHEN INSIGHT_CLICKEADO THEN 1 ELSE 0 END) AS CLICKEADO, SUM(CASE WHEN ACCION_TOMADA THEN 1 ELSE 0 END) AS ACCION FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total", "Visto", "Clickeado", "Accion tomada"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["VISTO"].iloc[0], df_funnel["CLICKEADO"].iloc[0], df_funnel["ACCION"].iloc[0]], marker=dict(color=SEMAFORO[:4])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel: Total → Visto → Click → Accion")
col2.plotly_chart(fig_fn, use_container_width=True)

col3, col4 = st.columns(2)
df_donut = run_query(f"SELECT TIPO_INSIGHT, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
fig_dn = px.pie(df_donut, names="TIPO_INSIGHT", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Tipo Insight")
col3.plotly_chart(fig_dn, use_container_width=True)

df_box = run_query(f"SELECT TIPO_INSIGHT, AHORRO_SUGERIDO_COP/1e3 AS AHORRO_K FROM {TABLE}")
fig_bx = px.box(df_box, x="TIPO_INSIGHT", y="AHORRO_K", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Ahorro Sugerido (K) por Tipo Insight")
col4.plotly_chart(fig_bx, use_container_width=True)
