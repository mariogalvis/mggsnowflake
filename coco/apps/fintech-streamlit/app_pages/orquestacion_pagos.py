from app_pages.page_template import render_page

config = {
    "key": "opc",
    "table": "MGG_FINTECH.PAGOS_Y_ECOSISTEMA.ORQUESTACION_PAGOS",
    "icon": ":material/payments:",
    "title": "Orquestacion Pagos",
    "subtitle": "Orquestacion inteligente de rieles de pago para maximizar aprobacion y minimizar costo.",
    "cards": ["Decide en tiempo real por cual riel enviar cada transaccion (ACH, tarjeta, PSE, QR) para maximizar aprobacion", "Evalua costo, latencia y tasa de aprobacion de cada riel disponible y selecciona el optimo con fallback automatico", "Optimiza costos de procesamiento y maximiza la tasa de exito de cada pago hasta un 30% mas"],
    "date_col": "FECHA_HORA",
    "filter_cols": ["TIPO_PAGO", "RIEL_SELECCIONADO", "RESULTADO"],
    "kpi_query": """SELECT ROUND(AVG(TASA_APROBACION_RIEL)*100,1), ROUND(AVG(LATENCIA_MS),1), ROUND(SUM(CASE WHEN USO_FALLBACK THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(MONTO_COP)/1e3,1), ROUND(AVG(COSTO_RIEL_PCT),2), ROUND(AVG(COMISION_COP),0), ROUND(AVG(SCORE_RIESGO_TXN)*100,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Aprobacion %", "Latencia ms", "Fallback %", "Monto K", "Costo %", "Comision", "Riesgo %", "Total"],
    "kpi_formats": ["{:.1f}%", "{:.1f}", "{:.1f}%", "${:.1f}K", "{:.2f}%", "${:.0f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES, ROUND(AVG(TASA_APROBACION_RIEL)*100,1) AS APROBACION, ROUND(AVG(LATENCIA_MS),1) AS LATENCIA FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["APROBACION", "LATENCIA"],
    "treemap_query": """SELECT RIEL_SELECCIONADO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Riel Seleccionado"},
    "geo_query": """SELECT CIUDAD, AVG(TASA_APROBACION_RIEL) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Tasa Aprobacion por Ciudad", "color_col": "VALOR"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Riesgo por Transaccion",
        "desc": "Estima el score de riesgo de una transaccion ajustando las variables del contexto de pago.",
        "features": [
            {"name": "Monto (K COP)", "min": 1, "max": 500, "default": 50, "weight": 0.25, "step": 10},
            {"name": "Latencia (ms)", "min": 50, "max": 5000, "default": 500, "weight": 0.2, "step": 50},
            {"name": "Costo Riel %", "min": 0, "max": 5, "default": 1.5, "weight": 0.15, "step": 0.1},
            {"name": "Tasa Aprobacion Historica %", "min": 50, "max": 100, "default": 90, "weight": -0.25, "step": 1},
            {"name": "Intentos Fallback", "min": 0, "max": 3, "default": 0, "weight": 0.15, "step": 1},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Riesgo Bajo", "Riesgo Medio", "Riesgo Alto"],
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
st.subheader(":material/bar_chart: Analisis Avanzado de Orquestacion")

df_heat = run_query(f"SELECT TIPO_PAGO, RIEL_SELECCIONADO, ROUND(AVG(TASA_APROBACION_RIEL)*100,1) AS TASA FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="TIPO_PAGO", columns="RIEL_SELECCIONADO", values="TASA").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.1f}%"))
fig_heat.update_layout(**CHART_LAYOUT, title="Tasa Aprobacion: Tipo Pago × Riel")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT LATENCIA_MS, COSTO_RIEL_PCT, RESULTADO FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="LATENCIA_MS", y="COSTO_RIEL_PCT", color="RESULTADO", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="Latencia vs Costo por Resultado")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_donut = run_query(f"SELECT INSTRUMENTO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["INSTRUMENTO"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Instrumento")
    st.plotly_chart(fig_donut, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    df_fun = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN RESULTADO='Aprobado' THEN 1 ELSE 0 END) AS APROBADO, SUM(CASE WHEN RESULTADO='Aprobado' AND NOT USO_FALLBACK THEN 1 ELSE 0 END) AS SIN_FALLBACK FROM {TABLE}")
    fig_fun = go.Figure(go.Funnel(y=["Total","Aprobado","Sin Fallback"], x=[df_fun["TOTAL"].iloc[0], df_fun["APROBADO"].iloc[0], df_fun["SIN_FALLBACK"].iloc[0]], marker=dict(color=SEMAFORO[:3])))
    fig_fun.update_layout(**CHART_LAYOUT, title="Funnel de Aprobacion")
    st.plotly_chart(fig_fun, use_container_width=True)
with col4:
    df_box = run_query(f"SELECT RIEL_SELECCIONADO, LATENCIA_MS FROM {TABLE} LIMIT 3000")
    fig_box = px.box(df_box, x="RIEL_SELECCIONADO", y="LATENCIA_MS", color_discrete_sequence=SEMAFORO)
    fig_box.update_layout(**CHART_LAYOUT, title="Latencia por Riel")
    st.plotly_chart(fig_box, use_container_width=True)
