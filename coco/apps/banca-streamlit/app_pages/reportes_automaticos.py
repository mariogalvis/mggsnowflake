from app_pages.page_template import render_page

config = {
    "key": "rau",
    "table": "MGG_BANCA.DATOS_Y_AI.REPORTES_AUTOMATICOS",
    "icon": ":material/summarize:",
    "title": "Reportes Automaticos",
    "subtitle": "Generacion automatica de reportes ejecutivos con narrativa AI y deteccion de anomalias.",
    "cards": ["TIPO_REPORTE", "FORMATO_SALIDA", "ESTADO"],
    "date_col": "FECHA_HORA_GENERACION",
    "filter_cols": ["TIPO_REPORTE", "FORMATO_SALIDA", "ESTADO"],
    "kpi_query": """SELECT
        AVG(RATING_UTILIDAD),
        AVG(PRECISION_INSIGHTS),
        AVG(CASE WHEN INCLUYE_NARRATIVA_AI THEN 1 ELSE 0 END)*100,
        AVG(ANOMALIAS_DETECTADAS),
        AVG(TIEMPO_GENERACION_SEG),
        AVG(CASE WHEN ACCION_TOMADA_POST_REPORTE IS NOT NULL THEN 1 ELSE 0 END)*100,
        AVG(PAGINAS_GENERADAS),
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Rating utilidad prom", "Precision insights", "% narrativa AI", "Anomalias prom", "Tiempo generacion (seg)", "% accion tomada", "Paginas prom", "Total reportes"],
    "kpi_formats": [",.2f", ",.2f", ",.1f", ",.2f", ",.1f", ",.1f", ",.1f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_GENERACION::DATE) AS MES, AVG(RATING_UTILIDAD) AS RATING_UTILIDAD, AVG(ANOMALIAS_DETECTADAS) AS ANOMALIAS FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["RATING_UTILIDAD", "ANOMALIAS"],
    "treemap_query": """SELECT TIPO_REPORTE AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Reporte"},
    "geo_query": None,
    "geo_config": None,
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
st.subheader(":material/bar_chart: Analisis Avanzado de Reportes Automaticos")

df_hm = run_query(f"SELECT TIPO_REPORTE, FORMATO_SALIDA, AVG(RATING_UTILIDAD) AS RATING FROM {TABLE} GROUP BY 1,2")
df_piv = df_hm.pivot(index="TIPO_REPORTE", columns="FORMATO_SALIDA", values="RATING").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=df_piv.values, x=df_piv.columns.tolist(), y=df_piv.index.tolist(), colorscale=[[0,"#FAFBFC"],[1,"#0F2B46"]]))
fig_hm.update_layout(**CHART_LAYOUT, title="Rating Utilidad por Tipo Reporte y Formato", xaxis_title="Formato Salida", yaxis_title="Tipo Reporte")
st.plotly_chart(fig_hm, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT PRECISION_INSIGHTS, RATING_UTILIDAD, STACK_GENERACION FROM {TABLE}")
    fig_sc = px.scatter(df_sc, x="PRECISION_INSIGHTS", y="RATING_UTILIDAD", color="STACK_GENERACION", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="Precision vs Rating Utilidad", xaxis_title="Precision Insights", yaxis_title="Rating Utilidad")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT FORMATO_SALIDA, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="FORMATO_SALIDA", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Formato de Salida")
    st.plotly_chart(fig_dn, use_container_width=True)

df_fn = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN INCLUYE_NARRATIVA_AI THEN 1 ELSE 0 END) AS NARRATIVA, SUM(CASE WHEN ACCION_TOMADA_POST_REPORTE IS NOT NULL THEN 1 ELSE 0 END) AS ACCION FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total Reportes", "Con Narrativa AI", "Accion Tomada"], x=[df_fn["TOTAL"].iloc[0], df_fn["NARRATIVA"].iloc[0], df_fn["ACCION"].iloc[0]], marker=dict(color=NAVY[:3])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel de Impacto")
st.plotly_chart(fig_fn, use_container_width=True)

df_box = run_query(f"SELECT TIPO_REPORTE, TIEMPO_GENERACION_SEG FROM {TABLE}")
fig_box = px.box(df_box, x="TIPO_REPORTE", y="TIEMPO_GENERACION_SEG", color="TIPO_REPORTE", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Tiempo Generacion por Tipo Reporte", showlegend=False, xaxis_title="Tipo Reporte", yaxis_title="Tiempo (seg)")
st.plotly_chart(fig_box, use_container_width=True)
