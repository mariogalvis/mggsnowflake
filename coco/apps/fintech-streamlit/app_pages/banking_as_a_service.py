from app_pages.page_template import render_page

config = {
    "key": "bas",
    "table": "MGG_FINTECH.PARTNERS_BAAS.BANKING_AS_A_SERVICE",
    "icon": ":material/api:",
    "title": "Banking as a Service",
    "subtitle": "Monitoreo de APIs BaaS con latencia, errores, SLA y revenue por llamada.",
    "cards": ["APIs bancarias expuestas a terceros como servicio: cuentas, pagos y creditos via API", "Registra endpoint, partner, volumen de llamadas, latencia, errores, SLA y revenue generado", "Monetiza infraestructura financiera vendiendo capacidades bancarias a otras empresas (BaaS)"],
    "date_col": "FECHA_HORA",
    "filter_cols": ["PARTNER", "ENDPOINT", "STATUS_CODE"],
    "kpi_query": """SELECT ROUND(AVG(LATENCIA_MS),1), ROUND(SUM(CASE WHEN ERROR THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(SLA_CUMPLIMIENTO)*100,1), ROUND(AVG(REVENUE_POR_LLAMADA_COP),0), ROUND(AVG(COSTO_POR_LLAMADA_COP),0), ROUND(SUM(CASE WHEN RATE_LIMITED THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(MONTO_ASOCIADO_COP)/1e3,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Latencia (ms)", "Error %", "SLA %", "Revenue/call", "Costo/call", "Rate limited %", "Monto (K)", "Total API calls"],
    "kpi_formats": ["{:.0f}", "{:.1f}%", "{:.1f}%", "${:.0f}", "${:.0f}", "{:.1f}%", "${:.1f}K", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES, ROUND(SUM(CASE WHEN ERROR THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS ERROR_PCT, ROUND(AVG(SLA_CUMPLIMIENTO)*100,1) AS SLA FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["ERROR_PCT", "SLA"],
    "treemap_query": """SELECT PARTNER AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Partner"},
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
SEMAFORO = ["#2D9B2D", "#6ABF4B", "#F5D63D", "#F5A623", "#E63946", "#1B8C1B", "#8BC34A", "#D32F2F"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de APIs BaaS")

df_heatmap = run_query(f"SELECT PARTNER, ENDPOINT, ROUND(SUM(CASE WHEN ERROR THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS ERROR_PCT FROM {TABLE} GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="PARTNER", columns="ENDPOINT", values="ERROR_PCT", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.1f}%"))
fig_hm.update_layout(**CHART_LAYOUT, title="% Error por Partner × Endpoint", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT LATENCIA_MS, REVENUE_POR_LLAMADA_COP, STATUS_CODE FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="LATENCIA_MS", y="REVENUE_POR_LLAMADA_COP", color="STATUS_CODE", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Latencia vs Revenue por Llamada")
col1.plotly_chart(fig_sc, use_container_width=True)

df_donut = run_query(f"SELECT METODO_HTTP, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
fig_dn = px.pie(df_donut, names="METODO_HTTP", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Metodo HTTP")
col2.plotly_chart(fig_dn, use_container_width=True)

col3, col4 = st.columns(2)
df_sla = run_query(f"SELECT PARTNER, ROUND(AVG(SLA_CUMPLIMIENTO)*100,1) AS SLA FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig_bar = px.bar(df_sla, x="PARTNER", y="SLA", color_discrete_sequence=[SEMAFORO[3]])
fig_bar.update_layout(**CHART_LAYOUT, title="SLA Cumplimiento por Partner")
col3.plotly_chart(fig_bar, use_container_width=True)

df_box = run_query(f"SELECT ENDPOINT, LATENCIA_MS FROM {TABLE} WHERE ENDPOINT IN (SELECT ENDPOINT FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 6)")
fig_bx = px.box(df_box, x="ENDPOINT", y="LATENCIA_MS", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Latencia (ms) por Endpoint Top 6")
col4.plotly_chart(fig_bx, use_container_width=True)
