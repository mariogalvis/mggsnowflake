from app_pages.page_template import render_page

config = {
    "key": "grf",
    "table": "MGG_FINTECH.PRODUCTO_Y_CRECIMIENTO.GROWTH_REFERIDOS",
    "icon": ":material/share:",
    "title": "Growth Referidos",
    "subtitle": "Programa de referidos con tracking de viralidad, activacion y ROI por canal.",
    "cards": ["Sistema de adquisicion viral por referidos con recompensa y tracking de conversion", "Registra quien refirio a quien, recompensa otorgada, CAC y valor de vida generado", "Mide ROI del programa de referidos y optimiza incentivos para maximizar crecimiento organico"],
    "date_col": "FECHA_REFERIDO",
    "filter_cols": ["CANAL_REFERIDO", "ESTADO_REFERIDO", "TIPO_PROGRAMA"],
    "kpi_query": """SELECT ROUND(SUM(CASE WHEN REFERIDO_ACTIVO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(ROI_REFERIDO),2), ROUND(AVG(CAC_REFERIDO_COP)/1e3,1), ROUND(AVG(LTV_ESTIMADO_REFERIDO_COP)/1e6,2), ROUND(AVG(VIRAL_COEFFICIENT),3), ROUND(AVG(DIAS_HASTA_ACTIVACION),1), ROUND(SUM(CASE WHEN SOSPECHA_ABUSO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Activos %", "ROI", "CAC K", "LTV M", "Viral Coeff", "Dias Activ", "Abuso %", "Total"],
    "kpi_formats": ["{:.1f}%", "{:.2f}", "${:.1f}K", "${:.2f}M", "{:.3f}", "{:.1f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_REFERIDO) AS MES, ROUND(SUM(CASE WHEN REFERIDO_ACTIVO THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS ACTIVOS_PCT, ROUND(AVG(VIRAL_COEFFICIENT),3) AS VIRAL_COEFF FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["ACTIVOS_PCT", "VIRAL_COEFF"],
    "treemap_query": """SELECT CANAL_REFERIDO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Canal Referido"},
    "geo_query": """SELECT CIUDAD_REFERIDO AS CIUDAD, AVG(ROI_REFERIDO) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "ROI Referido por Ciudad", "color_col": "VALOR"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Referidos")

df_fun = run_query(f"SELECT COUNT(*) AS REFERIDOS, SUM(CASE WHEN REFERIDO_ACTIVO THEN 1 ELSE 0 END) AS ACTIVOS, SUM(CASE WHEN PRIMERA_TXN_REFERIDO THEN 1 ELSE 0 END) AS PRIMERA_TXN, SUM(CASE WHEN REFERIDO_TAMBIEN_REFIRIO THEN 1 ELSE 0 END) AS TAMBIEN_REFIRIO FROM {TABLE}")
fig_fun = go.Figure(go.Funnel(y=["Referidos","Activos","Primera TXN","Tambien Refirio"], x=[df_fun["REFERIDOS"].iloc[0], df_fun["ACTIVOS"].iloc[0], df_fun["PRIMERA_TXN"].iloc[0], df_fun["TAMBIEN_REFIRIO"].iloc[0]], marker=dict(color=SEMAFORO[:4])))
fig_fun.update_layout(**CHART_LAYOUT, title="Funnel de Viralidad")
st.plotly_chart(fig_fun, use_container_width=True)

df_heat = run_query(f"SELECT CANAL_REFERIDO, TIPO_PROGRAMA, ROUND(AVG(ROI_REFERIDO),2) AS ROI FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="CANAL_REFERIDO", columns="TIPO_PROGRAMA", values="ROI").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.2f}"))
fig_heat.update_layout(**CHART_LAYOUT, title="ROI: Canal × Tipo Programa")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT CAC_REFERIDO_COP/1e3 AS CAC_K, LTV_ESTIMADO_REFERIDO_COP/1e6 AS LTV_M, ESTADO_REFERIDO FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="CAC_K", y="LTV_M", color="ESTADO_REFERIDO", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="CAC vs LTV por Estado")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_donut = run_query(f"SELECT CANAL_REFERIDO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["CANAL_REFERIDO"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Canal")
    st.plotly_chart(fig_donut, use_container_width=True)

df_box = run_query(f"SELECT TIPO_PROGRAMA, DIAS_HASTA_ACTIVACION FROM {TABLE} LIMIT 3000")
fig_box = px.box(df_box, x="TIPO_PROGRAMA", y="DIAS_HASTA_ACTIVACION", color_discrete_sequence=SEMAFORO)
fig_box.update_layout(**CHART_LAYOUT, title="Dias Hasta Activacion por Tipo Programa")
st.plotly_chart(fig_box, use_container_width=True)
