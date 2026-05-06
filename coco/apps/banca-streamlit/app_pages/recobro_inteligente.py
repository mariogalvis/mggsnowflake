from app_pages.page_template import render_page

config = {
    "key": "rec",
    "table": "MGG_BANCA.RIESGO_Y_CREDITO.RECOBRO_INTELIGENTE",
    "icon": ":material/account_balance_wallet:",
    "title": "Recobro Inteligente",
    "subtitle": "Motor de cobranza basado en ML que optimiza canal, horario y estrategia para maximizar recuperacion.",
    "cards": [
        "Maximiza la tasa de recuperacion priorizando deudores con mayor probabilidad de pago.",
        "Usa modelos de contactabilidad y propension para asignar canal, horario y estrategia optima.",
        "Incrementa recuperacion en 30-45% y reduce costo por peso recuperado en 40%.",
    ],
    "date_col": "FECHA_GESTION",
    "filter_cols": ["CANAL_CONTACTO", "ESTRATEGIA_COBRO", "RESULTADO_GESTION", "CIUDAD_DEUDOR"],
    "kpi_query": """SELECT
        COUNT(*) AS TOTAL_GESTIONES,
        ROUND(AVG(TASA_RECUPERACION) * 100, 2) AS TASA_RECUP_PROM,
        ROUND(SUM(MONTO_RECUPERADO_COP) / 1e9, 2) AS RECUPERADO_TOTAL_B,
        ROUND(AVG(DIAS_MORA), 0) AS MORA_PROM_DIAS,
        ROUND(AVG(PROB_RECUPERACION) * 100, 1) AS PROB_RECUP_PROM,
        ROUND(AVG(SCORE_CONTACTABILIDAD), 1) AS CONTACTABILIDAD_PROM,
        ROUND(SUM(CASE WHEN PROMESA_PAGO_ACTIVA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS PCT_PROMESAS,
        ROUND(AVG(INTENTOS_CONTACTO_MES), 1) AS INTENTOS_PROM
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Total Gestiones", "Tasa Recuperacion %", "Recuperado (B)", "Mora Prom (dias)", "Prob Recuperacion %", "Contactabilidad", "% con Promesa", "Intentos Prom"],
    "kpi_formats": ["{:,.0f}", "{:.2f}%", "${:.2f}B", "{:.0f}", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:.1f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_GESTION) AS MES,
        ROUND(SUM(MONTO_RECUPERADO_COP) / 1e6, 1) AS RECUPERADO_M,
        ROUND(AVG(TASA_RECUPERACION) * 100, 2) AS TASA_RECUP
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Recuperado (M)", "Tasa Recuperacion %"],
    "treemap_query": """SELECT ESTRATEGIA_COBRO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Gestiones por Estrategia de Cobro"},
    "geo_query": """SELECT CIUDAD_DEUDOR AS CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(TASA_RECUPERACION) * 100, 2) AS TASA_REC FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Recuperacion por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "TASA_REC", "caption": "Tamano: volumen gestiones | Color: tasa de recuperacion"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Recobro Inteligente")

# HEATMAP - FULL WIDTH
st.markdown("**Tasa de Recuperacion por Estrategia y Canal**")
df_heat = run_query(f"SELECT ESTRATEGIA_COBRO, CANAL_CONTACTO, ROUND(AVG(TASA_RECUPERACION) * 100, 2) AS TASA_REC FROM {TABLE} GROUP BY 1, 2")
pivot = df_heat.pivot_table(index="ESTRATEGIA_COBRO", columns="CANAL_CONTACTO", values="TASA_REC", aggfunc="mean")
fig = go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.2f}%"))
fig.update_layout(**CHART_LAYOUT)
st.plotly_chart(fig, use_container_width=True)

# SCATTER + DONUT - SIDE BY SIDE
col_a, col_b = st.columns(2)
with col_a:
    st.markdown("**Probabilidad de Recuperacion vs Dias en Mora**")
    df_sc = run_query(f"SELECT PROB_RECUPERACION, DIAS_MORA, RESULTADO_GESTION, SALDO_EN_MORA_COP FROM {TABLE} WHERE PROB_RECUPERACION IS NOT NULL")
    fig = px.scatter(df_sc, x="DIAS_MORA", y="PROB_RECUPERACION", color="RESULTADO_GESTION", size="SALDO_EN_MORA_COP", color_discrete_sequence=NAVY, opacity=0.7)
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)
with col_b:
    st.markdown("**Distribucion por Canal de Contacto**")
    df_donut = run_query(f"SELECT CANAL_CONTACTO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig = px.pie(df_donut, names="CANAL_CONTACTO", values="N", hole=0.5, color_discrete_sequence=NAVY)
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

# FUNNEL - FULL WIDTH
st.markdown("**Gestiones por Resultado**")
df_funnel = run_query(f"SELECT RESULTADO_GESTION, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig = go.Figure(go.Funnel(y=df_funnel["RESULTADO_GESTION"].tolist(), x=df_funnel["N"].tolist(), marker=dict(color=NAVY[:len(df_funnel)])))
fig.update_layout(**CHART_LAYOUT)
st.plotly_chart(fig, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Saldo en Mora (miles) por Estrategia de Cobro**")
df_box = run_query(f"SELECT ESTRATEGIA_COBRO, SALDO_EN_MORA_COP / 1000 AS SALDO_MILES FROM {TABLE}")
fig = px.box(df_box, x="ESTRATEGIA_COBRO", y="SALDO_MILES", color="ESTRATEGIA_COBRO", color_discrete_sequence=NAVY)
fig.update_layout(**CHART_LAYOUT, showlegend=False, yaxis_title="Saldo en Mora (miles COP)")
st.plotly_chart(fig, use_container_width=True)

# BAR - FULL WIDTH
st.markdown("**Monto Recuperado por Agencia de Cobro**")
df_bar = run_query(f"SELECT AGENCIA_COBRO, ROUND(SUM(MONTO_RECUPERADO_COP) / 1e6, 1) AS RECUPERADO_M FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig = px.bar(df_bar, x="AGENCIA_COBRO", y="RECUPERADO_M", color_discrete_sequence=[NAVY[3]])
fig.update_layout(**CHART_LAYOUT, xaxis_title="Agencia", yaxis_title="Monto Recuperado (M COP)")
st.plotly_chart(fig, use_container_width=True)
