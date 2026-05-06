from app_pages.page_template import render_page

config = {
    "key": "pem",
    "table": "MGG_FINTECH.PAGOS_Y_ECOSISTEMA.PAGOS_EMBEBIDOS",
    "icon": ":material/integration_instructions:",
    "title": "Pagos Embebidos",
    "subtitle": "Pagos integrados en apps de partners con monitoreo de SLA, latencia y experiencia checkout.",
    "cards": ["Pagos integrados dentro de apps de terceros: marketplaces, delivery y ERPs", "Registra partner, tipo de pago, monto, comision y experiencia del usuario en cada integracion", "Monetiza capacidades de pago como API dentro de ecosistemas externos generando revenue adicional"],
    "date_col": "FECHA_HORA",
    "filter_cols": ["PARTNER_APP", "TIPO_INTEGRACION", "ESTADO_PAGO"],
    "kpi_query": """SELECT ROUND(AVG(COMISION_FINTECH_PCT),2), ROUND(AVG(LATENCIA_TOTAL_MS),1), ROUND(SUM(CASE WHEN ESTADO_PAGO='Aprobado' THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(SCORE_FRAUDE)*100,1), ROUND(AVG(SLA_UPTIME_PARTNER)*100,1), ROUND(AVG(EXPERIENCIA_CHECKOUT_SCORE),2), ROUND(AVG(MONTO_COP)/1e3,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Comision %", "Latencia ms", "Aprobacion %", "Fraude %", "SLA Uptime %", "Checkout Score", "Monto K", "Total"],
    "kpi_formats": ["{:.2f}%", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:.1f}%", "{:.2f}", "${:.1f}K", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES, ROUND(AVG(SLA_UPTIME_PARTNER)*100,1) AS SLA_UPTIME, ROUND(AVG(COMISION_FINTECH_PCT),2) AS COMISION FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["SLA_UPTIME", "COMISION"],
    "treemap_query": """SELECT PARTNER_APP AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Partner App"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Pagos Embebidos")

df_heat = run_query(f"SELECT PARTNER_APP, TIPO_INTEGRACION, ROUND(SUM(CASE WHEN ESTADO_PAGO='Exitoso' THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS APROBACION FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="PARTNER_APP", columns="TIPO_INTEGRACION", values="APROBACION").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.1f}%"))
fig_heat.update_layout(**CHART_LAYOUT, title="% Aprobacion: Partner × Tipo Integracion")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT LATENCIA_TOTAL_MS AS LATENCIA, SCORE_FRAUDE, ESTADO_PAGO FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="LATENCIA", y="SCORE_FRAUDE", color="ESTADO_PAGO", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="Latencia vs Score Fraude")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_donut = run_query(f"SELECT PLATAFORMA_USUARIO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["PLATAFORMA_USUARIO"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Plataforma Usuario")
    st.plotly_chart(fig_donut, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    df_bar = run_query(f"SELECT PARTNER_APP, ROUND(SUM(COMISION_FINTECH_PCT * MONTO_COP)/1e6,2) AS REVENUE_M FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
    fig_bar = px.bar(df_bar, x="PARTNER_APP", y="REVENUE_M", color_discrete_sequence=SEMAFORO)
    fig_bar.update_layout(**CHART_LAYOUT, title="Revenue por Partner (Top 8)")
    st.plotly_chart(fig_bar, use_container_width=True)
with col4:
    df_box = run_query(f"SELECT PARTNER_APP, LATENCIA_TOTAL_MS FROM {TABLE} LIMIT 3000")
    fig_box = px.box(df_box, x="PARTNER_APP", y="LATENCIA_TOTAL_MS", color_discrete_sequence=SEMAFORO)
    fig_box.update_layout(**CHART_LAYOUT, title="Latencia por Partner")
    st.plotly_chart(fig_box, use_container_width=True)
