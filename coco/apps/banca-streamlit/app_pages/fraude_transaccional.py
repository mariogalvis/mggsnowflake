from app_pages.page_template import render_page

config = {
    "key": "ftx",
    "table": "MGG_BANCA.FRAUDE_Y_SEGURIDAD.FRAUDE_TRANSACCIONAL",
    "icon": ":material/gpp_bad:",
    "title": "Fraude Transaccional",
    "subtitle": "Deteccion en tiempo real de fraude transaccional con ML y reglas, decisiones en milisegundos.",
    "cards": [
        "Detecta fraude en tiempo real combinando modelos ML y reglas de negocio con latencia sub-segundo.",
        "Analiza patrones de velocidad, geolocalizacion y comportamiento para generar alertas inmediatas.",
        "Reduce perdidas por fraude en 40-60% manteniendo friccion minima para clientes legitimos.",
    ],
    "date_col": "FECHA_HORA_TXN",
    "filter_cols": ["TIPO_TRANSACCION", "DECISION_TIEMPO_REAL", "DISPOSITIVO"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_FRAUDE), 3) AS SCORE_FRAUDE_PROM,
        ROUND(SUM(CASE WHEN ES_FRAUDE = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_FRAUDE,
        ROUND(SUM(CASE WHEN ALERTA_GENERADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_ALERTAS,
        ROUND(SUM(CASE WHEN ES_FRAUDE = TRUE THEN MONTO_COP ELSE 0 END) / 1e6, 1) AS MONTO_FRAUDE_M,
        ROUND(AVG(LATENCIA_DECISION_MS), 1) AS LATENCIA_PROM,
        ROUND(SUM(CASE WHEN TARJETA_PRESENTE = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS PCT_TARJETA_PRESENTE,
        ROUND(SUM(CASE WHEN ALERTA_GENERADA = TRUE AND ES_FRAUDE = FALSE THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN ALERTA_GENERADA = TRUE THEN 1 ELSE 0 END), 0), 1) AS FALSOS_POSITIVOS,
        COUNT(*) AS TOTAL_TXN
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score Fraude Prom", "% Fraude Confirmado", "% Alertas", "Monto Fraude (M)", "Latencia Decision (ms)", "% Tarjeta Presente", "Falsos Positivos %", "Total TXN"],
    "kpi_formats": ["{:.3f}", "{:.2f}%", "{:.2f}%", "${:.1f}M", "{:.1f} ms", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_TXN) AS MES,
        ROUND(AVG(SCORE_FRAUDE), 3) AS SCORE_PROM,
        ROUND(SUM(CASE WHEN ES_FRAUDE = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_FRAUDE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Score Prom", "% Fraude"],
    "treemap_query": """SELECT TIPO_TRANSACCION, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Transaccion"},
    "geo_query": """SELECT CIUDAD_TXN AS CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_FRAUDE), 3) AS SCORE_PROM FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Fraude por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "SCORE_PROM", "caption": "Tamano: volumen TXN | Color: score fraude promedio"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Score de Fraude",
        "desc": "Ajuste las variables de la transaccion para estimar el score de fraude.",
        "features": [
            {"name": "Monto COP (M)", "min": 0.01, "max": 50.0, "default": 1.0, "step": 0.5, "weight": 0.2, "desc": "Monto de la transaccion en millones"},
            {"name": "Distancia Ultima TXN (km)", "min": 0, "max": 1000, "default": 5, "step": 5, "weight": 0.25, "desc": "Distancia desde la ultima transaccion"},
            {"name": "Segundos Desde Ultima TXN", "min": 0, "max": 86400, "default": 3600, "step": 60, "weight": -0.2, "desc": "Tiempo desde la ultima transaccion"},
            {"name": "TXN Ultimas 24H", "min": 0, "max": 100, "default": 5, "step": 1, "weight": 0.2, "desc": "Numero de transacciones en 24 horas"},
            {"name": "Sin PIN (1=Si, 0=No)", "min": 0, "max": 1, "default": 0, "step": 1, "weight": 0.15, "desc": "Transaccion sin validacion de PIN"},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Bajo Riesgo", "Riesgo Medio", "Alto Riesgo"],
    },
}

render_page(config)

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from app_pages.conn_helper import run_query

TABLE = config["table"]
CHART_LAYOUT = dict(paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC", font=dict(family="Inter, sans-serif", color="#334155"))
NAVY = ["#0F2B46", "#1B3A5C", "#2E7D8C", "#29B5E8", "#0F4C75", "#3282B8", "#11567F", "#1A5276"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de Fraude Transaccional")

df_heat = run_query(f"""
    SELECT TIPO_TRANSACCION, DISPOSITIVO,
        ROUND(SUM(CASE WHEN ES_FRAUDE = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_FRAUDE
    FROM {TABLE} GROUP BY 1, 2
""")
pivot = df_heat.pivot(index="TIPO_TRANSACCION", columns="DISPOSITIVO", values="PCT_FRAUDE").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, "#FAFBFC"], [1, NAVY[0]]], texttemplate="%{z:.1f}%", hovertemplate="Tipo: %{y}<br>Dispositivo: %{x}<br>% Fraude: %{z:.2f}%<extra></extra>"))
fig_heat.update_layout(**CHART_LAYOUT, title="% Fraude por Tipo Transaccion x Dispositivo", height=400)
st.plotly_chart(fig_heat, use_container_width=True)

df_scatter = run_query(f"SELECT SCORE_FRAUDE, MONTO_COP / 1000.0 AS MONTO_K, DECISION_TIEMPO_REAL, TXN_ULTIMAS_24H FROM {TABLE}")
df_donut = run_query(f"SELECT DECISION_TIEMPO_REAL, COUNT(*) AS N FROM {TABLE} GROUP BY 1")

c1, c2 = st.columns(2)
with c1:
    fig_sc = px.scatter(df_scatter, x="SCORE_FRAUDE", y="MONTO_K", color="DECISION_TIEMPO_REAL", size="TXN_ULTIMAS_24H", color_discrete_sequence=NAVY, labels={"MONTO_K": "Monto (miles COP)", "SCORE_FRAUDE": "Score Fraude"})
    fig_sc.update_layout(**CHART_LAYOUT, title="Score Fraude vs Monto", height=420)
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    fig_donut = go.Figure(go.Pie(labels=df_donut["DECISION_TIEMPO_REAL"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Decision Tiempo Real", height=420)
    st.plotly_chart(fig_donut, use_container_width=True)

df_funnel = run_query(f"""
    SELECT 'Total TXN' AS ETAPA, COUNT(*) AS N FROM {TABLE}
    UNION ALL SELECT 'Alerta Generada', COUNT(*) FROM {TABLE} WHERE ALERTA_GENERADA = TRUE
    UNION ALL SELECT 'Fraude Confirmado', COUNT(*) FROM {TABLE} WHERE ES_FRAUDE = TRUE
""")
df_bar = run_query(f"""
    SELECT MOTIVO_ALERTA, ROUND(SUM(CASE WHEN ES_FRAUDE = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_FRAUDE
    FROM {TABLE} WHERE MOTIVO_ALERTA IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 6
""")

c3, c4 = st.columns(2)
with c3:
    order = ["Total TXN", "Alerta Generada", "Fraude Confirmado"]
    df_funnel["ETAPA"] = pd.Categorical(df_funnel["ETAPA"], categories=order, ordered=True)
    df_funnel = df_funnel.sort_values("ETAPA")
    fig_fun = go.Figure(go.Funnel(y=df_funnel["ETAPA"], x=df_funnel["N"], marker=dict(color=NAVY[:3])))
    fig_fun.update_layout(**CHART_LAYOUT, title="Funnel: Total → Alerta → Fraude", height=400)
    st.plotly_chart(fig_fun, use_container_width=True)
with c4:
    fig_bar = go.Figure(go.Bar(x=df_bar["MOTIVO_ALERTA"], y=df_bar["PCT_FRAUDE"], marker_color=NAVY[0]))
    fig_bar.update_layout(**CHART_LAYOUT, title="% Fraude por Motivo Alerta (Top 6)", yaxis_title="% Fraude", height=400)
    st.plotly_chart(fig_bar, use_container_width=True)

df_box = run_query(f"SELECT TIPO_TRANSACCION, LATENCIA_DECISION_MS FROM {TABLE}")
fig_box = px.box(df_box, x="TIPO_TRANSACCION", y="LATENCIA_DECISION_MS", color="TIPO_TRANSACCION", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Latencia Decision (ms) por Tipo Transaccion", showlegend=False, height=420)
st.plotly_chart(fig_box, use_container_width=True)
