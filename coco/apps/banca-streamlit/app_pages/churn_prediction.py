from app_pages.page_template import render_page

config = {
    "key": "chp",
    "table": "MGG_BANCA.CLIENTE_Y_CRECIMIENTO.CHURN_PREDICTION",
    "icon": ":material/person_off:",
    "title": "Prediccion Churn",
    "subtitle": "Modelo predictivo de abandono bancario integrando transaccionalidad, engagement y satisfaccion.",
    "cards": [
        "Predice probabilidad de abandono integrando variaciones de saldo, transaccionalidad y engagement digital.",
        "Identifica motivo principal de riesgo y sugiere accion de retencion personalizada por segmento.",
        "Monitorea efectividad de gestiones de retencion midiendo churn real vs predicho por cohorte.",
    ],
    "date_col": "FECHA_PREDICCION",
    "filter_cols": ["SEGMENTO", "MOTIVO_RIESGO_PRINCIPAL", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(PROBABILIDAD_CHURN), 3) AS PROB_CHURN_PROM,
        ROUND(SUM(CASE WHEN CHURN_REAL = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_CHURN_REAL,
        ROUND(AVG(NPS_ULTIMO), 1) AS NPS_PROM,
        ROUND(AVG(DIAS_SIN_TRANSACCION), 1) AS DIAS_SIN_TXN_PROM,
        ROUND(AVG(ENGAGEMENT_SCORE), 2) AS ENGAGEMENT_PROM,
        ROUND(AVG(VALOR_CLIENTE_ANUAL_COP) / 1e6, 1) AS VALOR_CLIENTE_M,
        ROUND(AVG(QUEJAS_ULTIMO_6M), 1) AS QUEJAS_PROM,
        COUNT(*) AS TOTAL_PREDICCIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Prob Churn Prom", "% Churn Real", "NPS Prom", "Dias Sin TXN Prom", "Engagement Prom", "Valor Cliente (M)", "Quejas Prom", "Total Predicciones"],
    "kpi_formats": ["{:.3f}", "{:.2f}%", "{:.1f}", "{:.1f}", "{:.2f}", "${:.1f}M", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_PREDICCION) AS MES,
        ROUND(AVG(PROBABILIDAD_CHURN), 3) AS PROB_CHURN,
        ROUND(SUM(CASE WHEN CHURN_REAL = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_CHURN_REAL
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Prob Churn", "% Churn Real"],
    "treemap_query": """SELECT MOTIVO_RIESGO_PRINCIPAL, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Motivo de Riesgo"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(PROBABILIDAD_CHURN), 3) AS PROB_CHURN FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Churn por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "PROB_CHURN", "caption": "Tamano: volumen predicciones | Color: probabilidad churn promedio"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Probabilidad de Churn",
        "desc": "Ajuste las variables del cliente para estimar la probabilidad de abandono.",
        "features": [
            {"name": "Quejas Ultimo 6M", "min": 0, "max": 10, "default": 1, "step": 1, "weight": 0.25, "desc": "Numero de quejas en los ultimos 6 meses"},
            {"name": "Variacion Saldo 3M (%)", "min": -100, "max": 100, "default": 0, "step": 5, "weight": -0.2, "desc": "Variacion porcentual del saldo en 3 meses"},
            {"name": "Dias Sin Transaccion", "min": 0, "max": 180, "default": 15, "step": 5, "weight": 0.25, "desc": "Dias desde la ultima transaccion"},
            {"name": "Logins App Mes", "min": 0, "max": 60, "default": 10, "step": 1, "weight": -0.15, "desc": "Numero de logins en la app movil por mes"},
            {"name": "Engagement Score", "min": 0.0, "max": 1.0, "default": 0.5, "step": 0.05, "weight": -0.15, "desc": "Score de engagement del cliente"},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Bajo Riesgo", "Riesgo Medio", "Alto Riesgo"],
    },
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
st.subheader(":material/bar_chart: Analisis Avanzado de Prediccion Churn")

# HEATMAP - FULL WIDTH
df_heat = run_query(f"""SELECT SEGMENTO, MOTIVO_RIESGO_PRINCIPAL,
    ROUND(AVG(PROBABILIDAD_CHURN), 3) AS PROB_CHURN
FROM {TABLE} GROUP BY 1, 2""")
pivot = df_heat.pivot(index="SEGMENTO", columns="MOTIVO_RIESGO_PRINCIPAL", values="PROB_CHURN").fillna(0)
fig_heat = go.Figure(go.Heatmap(
    z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
    colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.3f}"
))
fig_heat.update_layout(**CHART_LAYOUT, title="Prob Churn por Segmento × Motivo de Riesgo")
st.plotly_chart(fig_heat, use_container_width=True)

c1, c2 = st.columns(2)

df_scatter = run_query(f"""SELECT PROBABILIDAD_CHURN, NPS_ULTIMO, SEGMENTO, VALOR_CLIENTE_ANUAL_COP FROM {TABLE}""")
fig_scatter = px.scatter(df_scatter, x="PROBABILIDAD_CHURN", y="NPS_ULTIMO",
    color="SEGMENTO", size="VALOR_CLIENTE_ANUAL_COP", color_discrete_sequence=NAVY)
fig_scatter.update_layout(**CHART_LAYOUT, title="Prob Churn vs NPS")
c1.plotly_chart(fig_scatter, use_container_width=True)

df_radar = run_query(f"""SELECT SEGMENTO,
    ROUND(AVG(QUEJAS_ULTIMO_6M)/10, 2) AS QUEJAS,
    ROUND(AVG(DIAS_SIN_TRANSACCION)/180, 2) AS DIAS_SIN_TXN,
    ROUND(1 - AVG(ENGAGEMENT_SCORE), 2) AS ENGAGEMENT_INV,
    ROUND(AVG(PROBABILIDAD_CHURN), 2) AS PROB_CHURN,
    ROUND(AVG(VALOR_CLIENTE_ANUAL_COP)/1e7, 2) AS VALOR
FROM {TABLE} GROUP BY 1""")
categories = ["Quejas", "Dias Sin TXN", "Engagement Inv", "Prob Churn", "Valor"]
fig_radar = go.Figure()
for i, row in df_radar.iterrows():
    vals = [row["QUEJAS"], row["DIAS_SIN_TXN"], row["ENGAGEMENT_INV"], row["PROB_CHURN"], row["VALOR"]]
    fig_radar.add_trace(go.Scatterpolar(r=vals + [vals[0]], theta=categories + [categories[0]],
        name=row["SEGMENTO"], line=dict(color=NAVY[i % len(NAVY)])))
fig_radar.update_layout(**CHART_LAYOUT, title="Factores de Riesgo por Segmento", polar=dict(bgcolor="#FAFBFC"))
c2.plotly_chart(fig_radar, use_container_width=True)

c3, c4 = st.columns(2)

df_donut = run_query(f"""SELECT ACCION_RETENCION, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
fig_donut = go.Figure(go.Pie(labels=df_donut["ACCION_RETENCION"], values=df_donut["N"], hole=0.5,
    marker=dict(colors=NAVY)))
fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Accion de Retencion")
c3.plotly_chart(fig_donut, use_container_width=True)

df_wf = run_query(f"""SELECT SEGMENTO, ROUND(SUM(VALOR_CLIENTE_ANUAL_COP * PROBABILIDAD_CHURN)/1e6, 1) AS REVENUE_RIESGO_M
FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
fig_wf = go.Figure(go.Waterfall(
    x=df_wf["SEGMENTO"], y=df_wf["REVENUE_RIESGO_M"],
    connector=dict(line=dict(color=NAVY[1])),
    increasing=dict(marker=dict(color=NAVY[0])),
    decreasing=dict(marker=dict(color=NAVY[3]))
))
fig_wf.update_layout(**CHART_LAYOUT, title="Revenue en Riesgo por Segmento (M COP)", yaxis_title="Millones COP")
c4.plotly_chart(fig_wf, use_container_width=True)

# BOX PLOT - FULL WIDTH
df_box = run_query(f"""SELECT SEGMENTO, DIAS_SIN_TRANSACCION FROM {TABLE}""")
fig_box = px.box(df_box, x="SEGMENTO", y="DIAS_SIN_TRANSACCION", color="SEGMENTO",
    color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Dias Sin Transaccion por Segmento", showlegend=False)
st.plotly_chart(fig_box, use_container_width=True)
