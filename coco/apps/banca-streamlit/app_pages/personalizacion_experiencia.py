from app_pages.page_template import render_page

config = {
    "key": "pex",
    "table": "MGG_BANCA.CLIENTE_Y_CRECIMIENTO.PERSONALIZACION_EXPERIENCIA",
    "icon": ":material/tune:",
    "title": "Personalizacion CX",
    "subtitle": "Perfilamiento de preferencias digitales para personalizar experiencia en cada canal.",
    "cards": [
        "Perfila preferencias digitales de cada cliente incluyendo canal, horario y categoria de gasto principal.",
        "Mide engagement por canal con tasas de apertura, click y tiempo de sesion por segmento.",
        "Optimiza comunicaciones personalizadas basadas en preferencias reales de interaccion del cliente.",
    ],
    "date_col": "ULTIMA_ACTUALIZACION_PERFIL",
    "filter_cols": ["CANAL_FAVORITO", "SISTEMA_OPERATIVO", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(INDICE_DIGITALIZACION), 2) AS IDX_DIGITALIZACION,
        ROUND(AVG(FRECUENCIA_USO_APP_MES), 1) AS FRECUENCIA_APP,
        ROUND(AVG(FRECUENCIA_USO_WEB_MES), 1) AS FRECUENCIA_WEB,
        ROUND(AVG(TASA_APERTURA_EMAILS), 3) AS TASA_APERTURA_EMAIL,
        ROUND(AVG(TASA_CLICK_NOTIFICACIONES), 3) AS TASA_CLICK_NOTIF,
        ROUND(AVG(TIEMPO_PROMEDIO_SESION_SEG), 1) AS TIEMPO_SESION_SEG,
        ROUND(AVG(RATING_APP), 2) AS RATING_APP,
        COUNT(*) AS TOTAL_PERFILES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Idx Digitalizacion", "Frecuencia App", "Frecuencia Web", "Tasa Apertura Email", "Tasa Click Notif", "Tiempo Sesion (seg)", "Rating App", "Total Perfiles"],
    "kpi_formats": ["{:.2f}", "{:.1f}", "{:.1f}", "{:.3f}", "{:.3f}", "{:.1f} seg", "{:.2f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', ULTIMA_ACTUALIZACION_PERFIL) AS MES,
        ROUND(AVG(INDICE_DIGITALIZACION), 2) AS IDX_DIGITALIZACION,
        ROUND(AVG(RATING_APP), 2) AS RATING_APP
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Idx Digitalizacion", "Rating App"],
    "treemap_query": """SELECT CANAL_FAVORITO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Canal Favorito"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(INDICE_DIGITALIZACION), 2) AS INDICE_DIGITALIZACION FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de Digitalizacion por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "INDICE_DIGITALIZACION", "caption": "Tamano: volumen perfiles | Color: indice digitalizacion promedio"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Personalizacion CX")

# HEATMAP - FULL WIDTH
df_heat = run_query(f"""SELECT CANAL_FAVORITO, HORARIO_USO_PRINCIPAL,
    ROUND(AVG(INDICE_DIGITALIZACION), 2) AS IDX_DIGITAL
FROM {TABLE} GROUP BY 1, 2""")
pivot = df_heat.pivot(index="CANAL_FAVORITO", columns="HORARIO_USO_PRINCIPAL", values="IDX_DIGITAL").fillna(0)
fig_heat = go.Figure(go.Heatmap(
    z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
    colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.2f}"
))
fig_heat.update_layout(**CHART_LAYOUT, title="Indice Digitalizacion por Canal × Horario")
st.plotly_chart(fig_heat, use_container_width=True)

c1, c2 = st.columns(2)

df_scatter = run_query(f"""SELECT TASA_APERTURA_EMAILS, TASA_CLICK_NOTIFICACIONES, CANAL_FAVORITO FROM {TABLE}""")
fig_scatter = px.scatter(df_scatter, x="TASA_APERTURA_EMAILS", y="TASA_CLICK_NOTIFICACIONES",
    color="CANAL_FAVORITO", color_discrete_sequence=NAVY)
fig_scatter.update_layout(**CHART_LAYOUT, title="Tasa Apertura vs Click")
c1.plotly_chart(fig_scatter, use_container_width=True)

df_donut = run_query(f"""SELECT SISTEMA_OPERATIVO, COUNT(*) AS N FROM {TABLE} GROUP BY 1""")
fig_donut = go.Figure(go.Pie(labels=df_donut["SISTEMA_OPERATIVO"], values=df_donut["N"], hole=0.5,
    marker=dict(colors=NAVY)))
fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Sistema Operativo")
c2.plotly_chart(fig_donut, use_container_width=True)

df_freq = run_query(f"""SELECT CANAL_FAVORITO,
    ROUND(AVG(FRECUENCIA_USO_APP_MES), 1) AS FREQ_APP,
    ROUND(AVG(FRECUENCIA_USO_WEB_MES), 1) AS FREQ_WEB
FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
fig_grp = go.Figure()
fig_grp.add_trace(go.Bar(name="App", x=df_freq["CANAL_FAVORITO"], y=df_freq["FREQ_APP"], marker_color=NAVY[0]))
fig_grp.add_trace(go.Bar(name="Web", x=df_freq["CANAL_FAVORITO"], y=df_freq["FREQ_WEB"], marker_color=NAVY[3]))
fig_grp.update_layout(**CHART_LAYOUT, barmode="group", title="Frecuencia App y Web por Canal")
st.plotly_chart(fig_grp, use_container_width=True)

# BOX PLOT - FULL WIDTH
df_box = run_query(f"""SELECT CANAL_FAVORITO, RATING_APP FROM {TABLE}""")
fig_box = px.box(df_box, x="CANAL_FAVORITO", y="RATING_APP", color="CANAL_FAVORITO",
    color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Rating App por Canal", showlegend=False)
st.plotly_chart(fig_box, use_container_width=True)

# HISTOGRAM - FULL WIDTH
df_hist = run_query(f"""SELECT TIEMPO_PROMEDIO_SESION_SEG FROM {TABLE}""")
fig_hist = px.histogram(df_hist, x="TIEMPO_PROMEDIO_SESION_SEG", nbins=30,
    color_discrete_sequence=[NAVY[3]])
fig_hist.update_layout(**CHART_LAYOUT, title="Distribucion Tiempo Promedio de Sesion (seg)",
    xaxis_title="Segundos", yaxis_title="Frecuencia")
st.plotly_chart(fig_hist, use_container_width=True)
