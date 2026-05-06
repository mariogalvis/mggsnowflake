from app_pages.page_template import render_page

config = {
    "key": "ori",
    "table": "MGG_BANCA.RIESGO_Y_CREDITO.ORIGINACION_DIGITAL",
    "icon": ":material/phone_android:",
    "title": "Originacion Digital",
    "subtitle": "Proceso end-to-end de solicitud de credito digital con validacion automatica y decision en tiempo real.",
    "cards": [
        "Reduce friccion en la solicitud de credito digital, acelerando tiempos de respuesta a minutos.",
        "Valida identidad, ingresos y documentos de forma automatica con scoring interno y deteccion de fraude.",
        "Incrementa conversion de solicitudes en 35% y reduce costos operativos de originacion en 50%.",
    ],
    "date_col": "FECHA_SOLICITUD",
    "filter_cols": ["CANAL_ORIGEN", "TIPO_CREDITO", "ESTADO_SOLICITUD", "CIUDAD_SOLICITANTE"],
    "kpi_query": """SELECT
        COUNT(*) AS TOTAL_SOLICITUDES,
        ROUND(SUM(CASE WHEN ESTADO_SOLICITUD = 'APROBADO' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS TASA_APROBACION,
        ROUND(AVG(TIEMPO_RESPUESTA_MINUTOS), 1) AS TIEMPO_RESP_PROM,
        ROUND(SUM(MONTO_SOLICITADO_COP) / 1e9, 2) AS MONTO_TOTAL_B,
        ROUND(AVG(SCORE_INTERNO), 1) AS SCORE_INTERNO_PROM,
        ROUND(SUM(CASE WHEN DESEMBOLSO_REALIZADO = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN ESTADO_SOLICITUD = 'APROBADO' THEN 1 ELSE 0 END), 0), 1) AS TASA_DESEMBOLSO,
        ROUND(AVG(RATIO_CUOTA_INGRESO) * 100, 1) AS CUOTA_INGRESO_PROM,
        ROUND(AVG(SCORE_FRAUDE_SOLICITUD), 2) AS SCORE_FRAUDE_PROM
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Total Solicitudes", "Tasa Aprobacion %", "Tiempo Resp (min)", "Monto Total (B)", "Score Interno", "Tasa Desembolso %", "Cuota/Ingreso %", "Score Fraude"],
    "kpi_formats": ["{:,.0f}", "{:.1f}%", "{:.1f}", "${:.2f}B", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:.2f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_SOLICITUD) AS MES,
        COUNT(*) AS SOLICITUDES,
        ROUND(SUM(CASE WHEN ESTADO_SOLICITUD = 'APROBADO' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS TASA_APROBACION
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Solicitudes", "Tasa Aprobacion %"],
    "treemap_query": """SELECT TIPO_CREDITO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Solicitudes por Tipo de Credito"},
    "geo_query": """SELECT CIUDAD_SOLICITANTE AS CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(TIEMPO_RESPUESTA_MINUTOS), 1) AS TIEMPO_PROM FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Originacion por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "TIEMPO_PROM", "caption": "Tamano: volumen solicitudes | Color: tiempo respuesta promedio"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Originacion Digital")

# FUNNEL - FULL WIDTH
st.markdown("**Embudo de Originacion Digital**")
df_funnel = run_query(f"""SELECT 'Solicitudes' AS ETAPA, COUNT(*) AS N FROM {TABLE}
UNION ALL SELECT 'Identidad Validada', COUNT(*) FROM {TABLE} WHERE IDENTIDAD_VALIDADA = TRUE
UNION ALL SELECT 'Ingresos Verificados', COUNT(*) FROM {TABLE} WHERE INGRESOS_VERIFICADOS = TRUE
UNION ALL SELECT 'Docs Completos', COUNT(*) FROM {TABLE} WHERE DOCUMENTOS_COMPLETOS = TRUE
UNION ALL SELECT 'Desembolsado', COUNT(*) FROM {TABLE} WHERE DESEMBOLSO_REALIZADO = TRUE""")
order = ['Solicitudes', 'Identidad Validada', 'Ingresos Verificados', 'Docs Completos', 'Desembolsado']
df_funnel["ETAPA"] = pd.Categorical(df_funnel["ETAPA"], categories=order, ordered=True) if "pandas" not in dir() else df_funnel["ETAPA"]
fig = go.Figure(go.Funnel(y=order, x=[df_funnel[df_funnel["ETAPA"] == e]["N"].values[0] for e in order], marker=dict(color=NAVY[:5])))
fig.update_layout(**CHART_LAYOUT)
st.plotly_chart(fig, use_container_width=True)

# HEATMAP - FULL WIDTH
st.markdown("**Tasa de Aprobacion por Canal y Tipo de Credito**")
df_heat = run_query(f"SELECT CANAL_ORIGEN, TIPO_CREDITO, ROUND(SUM(CASE WHEN ESTADO_SOLICITUD = 'APROBADO' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS TASA_APROBACION FROM {TABLE} GROUP BY 1, 2")
pivot = df_heat.pivot_table(index="CANAL_ORIGEN", columns="TIPO_CREDITO", values="TASA_APROBACION", aggfunc="mean")
fig = go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.1f}%"))
fig.update_layout(**CHART_LAYOUT)
st.plotly_chart(fig, use_container_width=True)

# SCATTER + DONUT - SIDE BY SIDE
col_a, col_b = st.columns(2)
with col_a:
    st.markdown("**Score Interno vs Ratio Cuota/Ingreso**")
    df_sc = run_query(f"SELECT SCORE_INTERNO, RATIO_CUOTA_INGRESO, ESTADO_SOLICITUD FROM {TABLE} WHERE SCORE_INTERNO IS NOT NULL")
    fig = px.scatter(df_sc, x="SCORE_INTERNO", y="RATIO_CUOTA_INGRESO", color="ESTADO_SOLICITUD", color_discrete_sequence=NAVY, opacity=0.7)
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)
with col_b:
    st.markdown("**Motivos de Rechazo**")
    df_donut = run_query(f"SELECT MOTIVO_RECHAZO, COUNT(*) AS N FROM {TABLE} WHERE MOTIVO_RECHAZO IS NOT NULL GROUP BY 1")
    fig = px.pie(df_donut, names="MOTIVO_RECHAZO", values="N", hole=0.5, color_discrete_sequence=NAVY)
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Monto Solicitado por Tipo de Credito**")
df_box = run_query(f"SELECT TIPO_CREDITO, MONTO_SOLICITADO_COP FROM {TABLE}")
fig = px.box(df_box, x="TIPO_CREDITO", y="MONTO_SOLICITADO_COP", color="TIPO_CREDITO", color_discrete_sequence=NAVY)
fig.update_layout(**CHART_LAYOUT, showlegend=False)
st.plotly_chart(fig, use_container_width=True)

# BAR - FULL WIDTH
st.markdown("**Tiempo de Respuesta Promedio por Canal**")
df_bar = run_query(f"SELECT CANAL_ORIGEN, ROUND(AVG(TIEMPO_RESPUESTA_MINUTOS), 1) AS TIEMPO_PROM FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig = px.bar(df_bar, x="CANAL_ORIGEN", y="TIEMPO_PROM", color_discrete_sequence=[NAVY[3]])
fig.update_layout(**CHART_LAYOUT, xaxis_title="Canal", yaxis_title="Tiempo Promedio (min)")
st.plotly_chart(fig, use_container_width=True)
