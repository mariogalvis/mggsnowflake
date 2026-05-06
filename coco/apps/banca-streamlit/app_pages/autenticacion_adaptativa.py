from app_pages.page_template import render_page

config = {
    "key": "aut",
    "table": "MGG_BANCA.FRAUDE_Y_SEGURIDAD.AUTENTICACION_ADAPTATIVA",
    "icon": ":material/fingerprint:",
    "title": "Autenticacion Adaptativa",
    "subtitle": "Autenticacion basada en riesgo con evaluacion contextual de dispositivo, ubicacion y comportamiento.",
    "cards": [
        "Evalua riesgo de sesion en tiempo real combinando dispositivo, ubicacion y comportamiento.",
        "Aplica desafios adicionales solo cuando el contexto indica riesgo elevado, reduciendo friccion.",
        "Detecta sesiones sospechosas y VPN para prevenir accesos no autorizados con autenticacion escalonada.",
    ],
    "date_col": "FECHA_HORA_EVENTO",
    "filter_cols": ["METODO_AUTENTICACION", "RESULTADO_AUTENTICACION", "DISPOSITIVO"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_RIESGO_SESION), 3) AS SCORE_RIESGO_PROM,
        ROUND(SUM(CASE WHEN DESAFIO_ADICIONAL_REQUERIDO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_DESAFIO,
        ROUND(SUM(CASE WHEN DISPOSITIVO_CONOCIDO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS PCT_DISP_CONOCIDO,
        ROUND(SUM(CASE WHEN UBICACION_HABITUAL = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS PCT_UBIC_HABITUAL,
        ROUND(AVG(LATENCIA_RESPUESTA_MS), 1) AS LATENCIA_PROM,
        ROUND(SUM(CASE WHEN SESION_SOSPECHOSA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_SOSPECHOSA,
        ROUND(SUM(CASE WHEN VPN_DETECTADA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_VPN,
        COUNT(*) AS TOTAL_EVENTOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score Riesgo Prom", "% Desafio Adicional", "% Dispositivo Conocido", "% Ubicacion Habitual", "Latencia (ms)", "% Sesion Sospechosa", "% VPN Detectada", "Total Eventos"],
    "kpi_formats": ["{:.3f}", "{:.2f}%", "{:.1f}%", "{:.1f}%", "{:.1f} ms", "{:.2f}%", "{:.2f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA_EVENTO) AS MES,
        ROUND(AVG(SCORE_RIESGO_SESION), 3) AS SCORE_RIESGO,
        ROUND(SUM(CASE WHEN SESION_SOSPECHOSA = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_SOSPECHOSA
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Score Riesgo", "% Sospechosa"],
    "treemap_query": """SELECT METODO_AUTENTICACION, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Metodo de Autenticacion"},
    "geo_query": """SELECT CIUDAD_CONEXION AS CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(SCORE_RIESGO_SESION), 3) AS SCORE_RIESGO FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Autenticacion por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "SCORE_RIESGO", "caption": "Tamano: volumen eventos | Color: score riesgo sesion"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Autenticacion Adaptativa")

df_heat = run_query(f"""
    SELECT METODO_AUTENTICACION, ACCION_SOLICITADA, ROUND(AVG(SCORE_RIESGO_SESION), 3) AS SCORE_RIESGO
    FROM {TABLE} GROUP BY 1, 2
""")
pivot = df_heat.pivot(index="METODO_AUTENTICACION", columns="ACCION_SOLICITADA", values="SCORE_RIESGO").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, "#FAFBFC"], [1, NAVY[0]]], texttemplate="%{z:.3f}", hovertemplate="Metodo: %{y}<br>Accion: %{x}<br>Score: %{z:.3f}<extra></extra>"))
fig_heat.update_layout(**CHART_LAYOUT, title="Score Riesgo por Metodo x Accion Solicitada", height=400)
st.plotly_chart(fig_heat, use_container_width=True)

df_scatter = run_query(f"SELECT SCORE_RIESGO_SESION, CONFIANZA_DISPOSITIVO, RESULTADO_AUTENTICACION FROM {TABLE}")
df_donut = run_query(f"SELECT RESULTADO_AUTENTICACION, COUNT(*) AS N FROM {TABLE} GROUP BY 1")

c1, c2 = st.columns(2)
with c1:
    fig_sc = px.scatter(df_scatter, x="SCORE_RIESGO_SESION", y="CONFIANZA_DISPOSITIVO", color="RESULTADO_AUTENTICACION", color_discrete_sequence=NAVY, labels={"SCORE_RIESGO_SESION": "Score Riesgo Sesion", "CONFIANZA_DISPOSITIVO": "Confianza Dispositivo"})
    fig_sc.update_layout(**CHART_LAYOUT, title="Score Riesgo vs Confianza Dispositivo", height=420)
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    fig_donut = go.Figure(go.Pie(labels=df_donut["RESULTADO_AUTENTICACION"], values=df_donut["N"], hole=0.5, marker=dict(colors=NAVY)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Resultado Autenticacion", height=420)
    st.plotly_chart(fig_donut, use_container_width=True)

df_funnel = run_query(f"""
    SELECT 'Total Eventos' AS ETAPA, COUNT(*) AS N FROM {TABLE}
    UNION ALL SELECT 'Desafio Requerido', COUNT(*) FROM {TABLE} WHERE DESAFIO_ADICIONAL_REQUERIDO = TRUE
    UNION ALL SELECT 'Sesion Sospechosa', COUNT(*) FROM {TABLE} WHERE SESION_SOSPECHOSA = TRUE
    UNION ALL SELECT 'VPN Detectada', COUNT(*) FROM {TABLE} WHERE VPN_DETECTADA = TRUE
""")
df_bar = run_query(f"""
    SELECT METODO_AUTENTICACION,
        ROUND(SUM(CASE WHEN DISPOSITIVO_CONOCIDO = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_DISP_CONOCIDO,
        ROUND(SUM(CASE WHEN UBICACION_HABITUAL = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS PCT_UBIC_HABITUAL
    FROM {TABLE} GROUP BY 1
""")

c3, c4 = st.columns(2)
with c3:
    order = ["Total Eventos", "Desafio Requerido", "Sesion Sospechosa", "VPN Detectada"]
    df_funnel["ETAPA"] = pd.Categorical(df_funnel["ETAPA"], categories=order, ordered=True)
    df_funnel = df_funnel.sort_values("ETAPA")
    fig_fun = go.Figure(go.Funnel(y=df_funnel["ETAPA"], x=df_funnel["N"], marker=dict(color=NAVY[:4])))
    fig_fun.update_layout(**CHART_LAYOUT, title="Funnel: Total → Desafio → Sospechosa → VPN", height=400)
    st.plotly_chart(fig_fun, use_container_width=True)
with c4:
    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(x=df_bar["METODO_AUTENTICACION"], y=df_bar["PCT_DISP_CONOCIDO"], name="% Disp. Conocido", marker_color=NAVY[0]))
    fig_bar.add_trace(go.Bar(x=df_bar["METODO_AUTENTICACION"], y=df_bar["PCT_UBIC_HABITUAL"], name="% Ubic. Habitual", marker_color=NAVY[3]))
    fig_bar.update_layout(**CHART_LAYOUT, barmode="group", title="% Dispositivo Conocido & Ubicacion Habitual", yaxis_title="%", height=400)
    st.plotly_chart(fig_bar, use_container_width=True)

df_box = run_query(f"SELECT DISPOSITIVO, LATENCIA_RESPUESTA_MS FROM {TABLE}")
fig_box = px.box(df_box, x="DISPOSITIVO", y="LATENCIA_RESPUESTA_MS", color="DISPOSITIVO", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Latencia Respuesta (ms) por Dispositivo", showlegend=False, height=420)
st.plotly_chart(fig_box, use_container_width=True)
