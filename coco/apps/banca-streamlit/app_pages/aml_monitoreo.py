from app_pages.page_template import render_page

config = {
    "key": "aml",
    "table": "MGG_BANCA.FRAUDE_Y_SEGURIDAD.AML_MONITOREO",
    "icon": ":material/policy:",
    "title": "AML Monitoreo",
    "subtitle": "Monitoreo anti-lavado con deteccion de patrones transaccionales, fragmentacion y PEP.",
    "cards": [
        "Detecta patrones de lavado de activos mediante analisis de fragmentacion y velocidad transaccional.",
        "Monitorea PEP y vinculaciones con investigados para priorizar casos de mayor riesgo.",
        "Automatiza generacion de ROS y reduce tiempo de investigacion en 50% con scoring inteligente.",
    ],
    "date_col": "FECHA_ALERTA",
    "filter_cols": ["TIPO_ALERTA", "DISPOSICION_CASO", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_AML), 3) AS SCORE_AML_PROM,
        ROUND(SUM(CASE WHEN ES_PEP = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_PEP,
        ROUND(SUM(CASE WHEN ROS_GENERADO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_ROS,
        ROUND(AVG(DIAS_INVESTIGACION), 1) AS DIAS_INV_PROM,
        ROUND(SUM(CASE WHEN FRAGMENTACION_DETECTADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS PCT_FRAGMENTACION,
        ROUND(AVG(VOLUMEN_TRANSACCIONAL_30D_COP) / 1e6, 1) AS VOL_30D_PROM_M,
        ROUND(AVG(NIVEL_RIESGO_AML), 2) AS NIVEL_RIESGO_PROM,
        COUNT(*) AS TOTAL_CASOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score AML Prom", "% PEP", "% ROS Generado", "Dias Investigacion Prom", "% Fragmentacion", "Volumen 30d Prom (M)", "Nivel Riesgo Prom", "Total Casos"],
    "kpi_formats": ["{:.3f}", "{:.2f}%", "{:.2f}%", "{:.1f} dias", "{:.1f}%", "${:.1f}M", "{:.2f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_ALERTA) AS MES,
        ROUND(AVG(SCORE_AML), 3) AS SCORE_AML,
        ROUND(SUM(CASE WHEN ROS_GENERADO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_ROS
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Score AML", "% ROS"],
    "treemap_query": """SELECT TIPO_ALERTA, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Alerta"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_AML), 3) AS SCORE_AML FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa AML por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "SCORE_AML", "caption": "Tamano: volumen casos | Color: score AML promedio"},
    "diagnostics": None,
    "simulator": None,
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
st.subheader(":material/bar_chart: Analisis Avanzado de AML Monitoreo")

df_heat = run_query(f"""
    SELECT TIPO_ALERTA, DISPOSICION_CASO, ROUND(AVG(SCORE_AML), 3) AS SCORE_AML
    FROM {TABLE} GROUP BY 1, 2
""")
pivot = df_heat.pivot(index="TIPO_ALERTA", columns="DISPOSICION_CASO", values="SCORE_AML").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, "#FAFBFC"], [1, NAVY[0]]], texttemplate="%{z:.3f}", hovertemplate="Alerta: %{y}<br>Disposicion: %{x}<br>Score: %{z:.3f}<extra></extra>"))
fig_heat.update_layout(**CHART_LAYOUT, title="Score AML por Tipo Alerta x Disposicion", height=400)
st.plotly_chart(fig_heat, use_container_width=True)

df_scatter = run_query(f"SELECT VOLUMEN_TRANSACCIONAL_30D_COP / 1000000.0 AS VOL_M, SCORE_AML, ESTADO_CASO, NUM_CONTRAPARTES FROM {TABLE}")
df_donut = run_query(f"SELECT ESTADO_CASO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")

c1, c2 = st.columns(2)
with c1:
    fig_sc = px.scatter(df_scatter, x="VOL_M", y="SCORE_AML", color="ESTADO_CASO", size="NUM_CONTRAPARTES", color_discrete_sequence=NAVY, labels={"VOL_M": "Volumen 30d (Millones COP)", "SCORE_AML": "Score AML"})
    fig_sc.update_layout(**CHART_LAYOUT, title="Volumen 30d vs Score AML", height=420)
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    fig_donut = go.Figure(go.Pie(labels=df_donut["ESTADO_CASO"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Estado de Casos", height=420)
    st.plotly_chart(fig_donut, use_container_width=True)

df_funnel = run_query(f"""
    SELECT 'Alertas' AS ETAPA, COUNT(*) AS N FROM {TABLE}
    UNION ALL SELECT 'En Investigacion', COUNT(*) FROM {TABLE} WHERE ESTADO_CASO = 'En Investigacion'
    UNION ALL SELECT 'ROS Generado', COUNT(*) FROM {TABLE} WHERE ROS_GENERADO = TRUE
""")
df_bar = run_query(f"""
    SELECT SECTOR_ECONOMICO, ROUND(SUM(CASE WHEN FRAGMENTACION_DETECTADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_FRAG
    FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 8
""")

c3, c4 = st.columns(2)
with c3:
    order = ["Alertas", "En Investigacion", "ROS Generado"]
    df_funnel["ETAPA"] = pd.Categorical(df_funnel["ETAPA"], categories=order, ordered=True)
    df_funnel = df_funnel.sort_values("ETAPA")
    fig_fun = go.Figure(go.Funnel(y=df_funnel["ETAPA"], x=df_funnel["N"], marker=dict(color=NAVY[:3])))
    fig_fun.update_layout(**CHART_LAYOUT, title="Funnel: Alertas → Investigacion → ROS", height=400)
    st.plotly_chart(fig_fun, use_container_width=True)
with c4:
    fig_bar = go.Figure(go.Bar(x=df_bar["SECTOR_ECONOMICO"], y=df_bar["PCT_FRAG"], marker_color=NAVY[0]))
    fig_bar.update_layout(**CHART_LAYOUT, title="% Fragmentacion por Sector (Top 8)", yaxis_title="% Fragmentacion", height=400)
    st.plotly_chart(fig_bar, use_container_width=True)

df_box = run_query(f"SELECT TIPO_ALERTA, DIAS_INVESTIGACION FROM {TABLE}")
fig_box = px.box(df_box, x="TIPO_ALERTA", y="DIAS_INVESTIGACION", color="TIPO_ALERTA", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Dias Investigacion por Tipo Alerta", showlegend=False, height=420)
st.plotly_chart(fig_box, use_container_width=True)
