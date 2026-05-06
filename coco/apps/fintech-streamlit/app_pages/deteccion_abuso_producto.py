from app_pages.page_template import render_page

config = {
    "key": "dap",
    "table": "MGG_FINTECH.RIESGO_AMPLIADO.DETECCION_ABUSO_PRODUCTO",
    "icon": ":material/block:",
    "title": "Deteccion Abuso",
    "subtitle": "Deteccion de abuso de promociones, referidos y cashback con correlacion de cuentas.",
    "cards": ["Detecta abuso de promociones, cashback hacking, multicuentas y explotacion de reglas", "Registra patron detectado, usuario, monto involucrado, evidencia y accion tomada en tiempo real", "Protege margenes de la fintech contra usuarios que explotan incentivos de forma fraudulenta"],
    "date_col": "FECHA_DETECCION",
    "filter_cols": ["TIPO_ABUSO", "SEVERIDAD", "ESTADO_CASO"],
    "kpi_query": """SELECT ROUND(AVG(SCORE_ABUSO)*100,1), ROUND(SUM(CASE WHEN CONFIRMADO_ABUSO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(SUM(PERDIDA_ESTIMADA_COP)/1e6,1), ROUND(SUM(MONTO_INVOLUCRADO_COP)/1e6,1), ROUND(AVG(CUENTAS_RELACIONADAS),1), ROUND(AVG(HORAS_INVESTIGACION),1), ROUND(AVG(TASA_FALSOS_POSITIVOS_REGLA)*100,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Score abuso %", "Confirmado %", "Perdida (M)", "Monto (M)", "Cuentas relac.", "Horas investig.", "Falsos pos. %", "Total casos"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:.1f}M", "${:.1f}M", "{:.1f}", "{:.1f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_DETECCION) AS MES, ROUND(AVG(SCORE_ABUSO)*100,1) AS SCORE_PCT, ROUND(SUM(CASE WHEN CONFIRMADO_ABUSO THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS CONFIRMADO_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["SCORE_PCT", "CONFIRMADO_PCT"],
    "treemap_query": """SELECT TIPO_ABUSO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo Abuso"},
    "geo_query": """SELECT CIUDAD, AVG(SCORE_ABUSO) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Score Abuso por Ciudad", "color_col": "VALOR"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Score de Abuso",
        "desc": "Estima la probabilidad de abuso de producto ajustando los patrones de comportamiento del usuario.",
        "features": [
            {"name": "Promos Redimidas 30D", "min": 0, "max": 50, "default": 5, "weight": 0.3, "step": 1},
            {"name": "Monto Acumulado Promos (K)", "min": 0, "max": 500, "default": 30, "weight": 0.25, "step": 10},
            {"name": "Cuentas Asociadas", "min": 1, "max": 10, "default": 1, "weight": 0.2, "step": 1},
            {"name": "Antiguedad (dias)", "min": 1, "max": 365, "default": 90, "weight": -0.15, "step": 10},
            {"name": "Velocidad Gasto (txn/hora)", "min": 0, "max": 20, "default": 2, "weight": 0.1, "step": 1},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Abuso Improbable", "Abuso Posible", "Abuso Probable"],
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
st.subheader(":material/bar_chart: Analisis Avanzado de Deteccion de Abuso")

df_heatmap = run_query(f"SELECT TIPO_ABUSO, SEVERIDAD, ROUND(AVG(SCORE_ABUSO)*100,1) AS SCORE FROM {TABLE} GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="TIPO_ABUSO", columns="SEVERIDAD", values="SCORE", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.1f}%"))
fig_hm.update_layout(**CHART_LAYOUT, title="Score Abuso por Tipo Abuso × Severidad", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT SCORE_ABUSO, PERDIDA_ESTIMADA_COP/1e3 AS PERDIDA_K, ESTADO_CASO FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="SCORE_ABUSO", y="PERDIDA_K", color="ESTADO_CASO", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Score vs Perdida Estimada (K)")
col1.plotly_chart(fig_sc, use_container_width=True)

df_funnel = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN CONFIRMADO_ABUSO THEN 1 ELSE 0 END) AS CONFIRMADO, SUM(CASE WHEN ACCION_TOMADA IS NOT NULL THEN 1 ELSE 0 END) AS ACCION_TOMADA FROM {TABLE}")
fig_fn = go.Figure(go.Funnel(y=["Total", "Confirmado abuso", "Accion tomada"], x=[df_funnel["TOTAL"].iloc[0], df_funnel["CONFIRMADO"].iloc[0], df_funnel["ACCION_TOMADA"].iloc[0]], marker=dict(color=SEMAFORO[:3])))
fig_fn.update_layout(**CHART_LAYOUT, title="Funnel: Total → Confirmado → Accion")
col2.plotly_chart(fig_fn, use_container_width=True)

col3, col4 = st.columns(2)
df_donut = run_query(f"SELECT METODO_DETECCION, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
fig_dn = px.pie(df_donut, names="METODO_DETECCION", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Metodo de Deteccion")
col3.plotly_chart(fig_dn, use_container_width=True)

df_box = run_query(f"SELECT TIPO_ABUSO, HORAS_INVESTIGACION FROM {TABLE}")
fig_bx = px.box(df_box, x="TIPO_ABUSO", y="HORAS_INVESTIGACION", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Horas Investigacion por Tipo Abuso")
col4.plotly_chart(fig_bx, use_container_width=True)
