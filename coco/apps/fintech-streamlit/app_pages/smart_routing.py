from app_pages.page_template import render_page

config = {
    "key": "srt",
    "table": "MGG_FINTECH.PAGOS_Y_ECOSISTEMA.SMART_ROUTING",
    "icon": ":material/alt_route:",
    "title": "Smart Routing",
    "subtitle": "Motor de decisiones para seleccion optima de proveedor minimizando costo y maximizando aprobacion.",
    "cards": ["Enruta transacciones entre multiples proveedores de pago para optimizar costo y aprobacion", "Compara proveedores en tiempo real, selecciona el mejor segun contexto y registra el resultado", "Reduce tasas de declinacion y costos de procesamiento hasta un 30% con decisiones automaticas"],
    "date_col": "FECHA_HORA",
    "filter_cols": ["PROVEEDOR_SELECCIONADO", "RESULTADO_FINAL", "MOTOR_DECISION"],
    "kpi_query": """SELECT ROUND(AVG(TASA_APROBACION_ESTIMADA)*100,1), ROUND(SUM(AHORRO_VS_DEFAULT_COP)/1e6,2), ROUND(AVG(LATENCIA_DECISION_MS),1), ROUND(AVG(COSTO_SELECCIONADO_PCT),2), ROUND(AVG(CONFIANZA_MODELO)*100,1), ROUND(SUM(CASE WHEN REINTENTOS_EJECUTADOS THEN 1 ELSE 0 END)*100.0/COUNT(*),1), COUNT(*), ROUND(AVG(AHORRO_VS_DEFAULT_COP),0) FROM {table} WHERE {where}""",
    "kpi_labels": ["Aprobacion %", "Ahorro M", "Latencia ms", "Costo %", "Confianza %", "Reintentos %", "Total", "Ahorro Prom"],
    "kpi_formats": ["{:.1f}%", "${:.2f}M", "{:.1f}", "{:.2f}%", "{:.1f}%", "{:.1f}%", "{:,.0f}", "${:.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES, ROUND(AVG(TASA_APROBACION_ESTIMADA)*100,1) AS APROBACION, ROUND(AVG(CONFIANZA_MODELO)*100,1) AS CONFIANZA FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["APROBACION", "CONFIANZA"],
    "treemap_query": """SELECT RAZON_SELECCION AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Razon de Seleccion"},
    "geo_query": None,
    "geo_config": None,
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Tasa de Aprobacion",
        "desc": "Estima la tasa de aprobacion esperada segun el contexto de la transaccion y proveedores disponibles.",
        "features": [
            {"name": "Monto (K COP)", "min": 1, "max": 500, "default": 80, "weight": -0.2, "step": 10},
            {"name": "Reintentos Disponibles", "min": 0, "max": 3, "default": 2, "weight": 0.2, "step": 1},
            {"name": "Score Cliente", "min": 0, "max": 100, "default": 70, "weight": 0.3, "step": 5},
            {"name": "Hora del Dia", "min": 0, "max": 23, "default": 14, "weight": -0.1, "step": 1},
            {"name": "Proveedores Disponibles", "min": 1, "max": 5, "default": 3, "weight": 0.2, "step": 1},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Aprobacion Baja", "Aprobacion Media", "Aprobacion Alta"],
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
st.subheader(":material/bar_chart: Analisis Avanzado de Smart Routing")

df_heat = run_query(f"SELECT CATEGORIA_COMERCIO, PROVEEDOR_SELECCIONADO, ROUND(AVG(AHORRO_VS_DEFAULT_COP),0) AS AHORRO FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="CATEGORIA_COMERCIO", columns="PROVEEDOR_SELECCIONADO", values="AHORRO").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:,.0f}"))
fig_heat.update_layout(**CHART_LAYOUT, title="Ahorro Promedio: Categoria × Proveedor")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT CONFIANZA_MODELO, TASA_APROBACION_ESTIMADA, RESULTADO_FINAL FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="CONFIANZA_MODELO", y="TASA_APROBACION_ESTIMADA", color="RESULTADO_FINAL", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="Confianza vs Aprobacion Estimada")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_donut = run_query(f"SELECT RAZON_SELECCION, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["RAZON_SELECCION"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Razon Seleccion")
    st.plotly_chart(fig_donut, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    df_bar = run_query(f"SELECT PROVEEDOR_SELECCIONADO, ROUND(SUM(AHORRO_VS_DEFAULT_COP)/1e6,2) AS AHORRO_M FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 6")
    fig_bar = px.bar(df_bar, x="PROVEEDOR_SELECCIONADO", y="AHORRO_M", color_discrete_sequence=SEMAFORO)
    fig_bar.update_layout(**CHART_LAYOUT, title="Ahorro Total por Proveedor (Top 6)")
    st.plotly_chart(fig_bar, use_container_width=True)
with col4:
    df_box = run_query(f"SELECT MOTOR_DECISION, LATENCIA_DECISION_MS FROM {TABLE} LIMIT 3000")
    fig_box = px.box(df_box, x="MOTOR_DECISION", y="LATENCIA_DECISION_MS", color_discrete_sequence=SEMAFORO)
    fig_box.update_layout(**CHART_LAYOUT, title="Latencia Decision por Motor")
    st.plotly_chart(fig_box, use_container_width=True)
