from app_pages.page_template import render_page

config = {
    "key": "alp",
    "table": "MGG_FINTECH.SOPORTE_FINANCIERO_INTELIGENTE.ALERTAS_PROACTIVAS",
    "icon": ":material/notifications_active:",
    "title": "Alertas Proactivas",
    "subtitle": "Alertas predictivas para prevenir sobregiros, fraude y cargos innecesarios.",
    "cards": ["Alertas inteligentes antes de problemas: riesgo de sobregiro, pagos proximos, gastos inusuales", "Evalua contexto financiero del usuario y envia notificaciones preventivas por canal preferido", "Previene cargos no deseados, evita sobregiros y mejora salud financiera del usuario proactivamente"],
    "date_col": "FECHA_HORA_ALERTA",
    "filter_cols": ["TIPO_ALERTA", "URGENCIA", "CANAL_ENTREGA"],
    "kpi_query": """SELECT ROUND(SUM(CASE WHEN ALERTA_VISTA THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN ACCION_TOMADA THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(CASE WHEN EVENTO_PREVENIDO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(VALOR_PROTEGIDO_COP)/1e6,1), ROUND(AVG(TIEMPO_REACCION_MIN),1), ROUND(AVG(PROB_EVENTO)*100,1), ROUND(AVG(UTILIDAD_REPORTADA),1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Vista %", "Accion %", "Prevenido %", "Valor protegido (M)", "Reaccion (min)", "Prob evento %", "Utilidad", "Total alertas"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}M", "{:.1f}", "{:.1f}%", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_ALERTA) AS MES, ROUND(SUM(CASE WHEN EVENTO_PREVENIDO THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS PREVENIDO_PCT, ROUND(AVG(UTILIDAD_REPORTADA),1) AS UTILIDAD FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["PREVENIDO_PCT", "UTILIDAD"],
    "treemap_query": """SELECT TIPO_ALERTA AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo Alerta"},
    "geo_query": """SELECT CIUDAD, AVG(UTILIDAD_REPORTADA) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Utilidad Reportada por Ciudad", "color_col": "VALOR"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Efectividad de Alerta",
        "desc": "Estima la probabilidad de que el usuario actue ante una alerta proactiva segun contexto y fatigue.",
        "features": [
            {"name": "Urgencia (1-5)", "min": 1, "max": 5, "default": 3, "weight": 0.3, "step": 1},
            {"name": "Fatigue Score", "min": 0, "max": 100, "default": 30, "weight": -0.25, "step": 5},
            {"name": "Relevancia Contexto %", "min": 0, "max": 100, "default": 70, "weight": 0.2, "step": 5},
            {"name": "Alertas Ultimos 7D", "min": 0, "max": 20, "default": 3, "weight": -0.15, "step": 1},
            {"name": "Hora Envio (0-23)", "min": 0, "max": 23, "default": 10, "weight": -0.1, "step": 1},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Baja Efectividad", "Efectividad Media", "Alta Efectividad"],
    },
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
st.subheader(":material/bar_chart: Analisis Avanzado de Alertas Proactivas")

df_heatmap = run_query(f"SELECT TIPO_ALERTA, URGENCIA, ROUND(SUM(CASE WHEN EVENTO_PREVENIDO THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS PREVENIDO_PCT FROM {TABLE} GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="TIPO_ALERTA", columns="URGENCIA", values="PREVENIDO_PCT", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.1f}%"))
fig_hm.update_layout(**CHART_LAYOUT, title="% Prevenido por Tipo Alerta × Urgencia", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT PROB_EVENTO, VALOR_PROTEGIDO_COP/1e3 AS VALOR_K, URGENCIA FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="PROB_EVENTO", y="VALOR_K", color="URGENCIA", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Prob Evento vs Valor Protegido (K)")
col1.plotly_chart(fig_sc, use_container_width=True)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN ALERTA_VISTA THEN 1 ELSE 0 END) AS VISTA, SUM(CASE WHEN ACCION_TOMADA THEN 1 ELSE 0 END) AS ACCION, SUM(CASE WHEN EVENTO_PREVENIDO THEN 1 ELSE 0 END) AS PREVENIDO FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total", "Vista", "Accion", "Prevenido"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["VISTA"].iloc[0], df_funnel["ACCION"].iloc[0], df_funnel["PREVENIDO"].iloc[0]], marker=dict(color=SEMAFORO[:4])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel: Total → Vista → Accion → Prevenido")
col2.plotly_chart(fig_fn, use_container_width=True)

col3, col4 = st.columns(2)
df_donut = run_query(f"SELECT CANAL_ENTREGA, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
fig_dn = px.pie(df_donut, names="CANAL_ENTREGA", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Canal de Entrega")
col3.plotly_chart(fig_dn, use_container_width=True)

df_box = run_query(f"SELECT TIPO_ALERTA, TIEMPO_REACCION_MIN FROM {TABLE}")
fig_bx = px.box(df_box, x="TIPO_ALERTA", y="TIEMPO_REACCION_MIN", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Tiempo Reaccion (min) por Tipo Alerta")
col4.plotly_chart(fig_bx, use_container_width=True)
