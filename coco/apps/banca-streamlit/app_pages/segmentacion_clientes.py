from app_pages.page_template import render_page

config = {
    "key": "seg",
    "table": "MGG_BANCA.CLIENTE_Y_CRECIMIENTO.SEGMENTACION_CLIENTES",
    "icon": ":material/groups:",
    "title": "Segmentacion Clientes",
    "subtitle": "Segmentacion 360 por valor, comportamiento digital y potencial de crecimiento.",
    "cards": [
        "Clasifica clientes por comportamiento, perfil financiero y ciclo de vida para estrategias diferenciadas.",
        "Mide indice de digitalizacion, share of wallet y potencial de crecimiento por segmento.",
        "Identifica riesgo de churn y oportunidades de profundizacion en cada cluster de clientes.",
    ],
    "date_col": None,
    "filter_cols": ["SEGMENTO_COMPORTAMENTAL", "PERFIL_FINANCIERO", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(INGRESO_ESTIMADO_COP) / 1e6, 1) AS INGRESO_PROM_M,
        ROUND(AVG(NUM_PRODUCTOS), 1) AS PRODUCTOS_PROM,
        ROUND(AVG(SALDO_TOTAL_COP) / 1e6, 1) AS SALDO_PROM_M,
        ROUND(AVG(INDICE_DIGITALIZACION), 2) AS DIGITALIZACION,
        ROUND(AVG(NPS_SCORE), 1) AS NPS,
        ROUND(AVG(VALOR_CLIENTE_ANUAL_COP) / 1e6, 1) AS VALOR_CLIENTE_M,
        ROUND(AVG(PROBABILIDAD_CHURN) * 100, 2) AS CHURN_PCT,
        COUNT(*) AS TOTAL_CLIENTES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Ingreso Prom (M)", "Productos Prom", "Saldo Prom (M)", "Digitalizacion", "NPS", "Valor Cliente (M)", "Churn %", "Total Clientes"],
    "kpi_formats": ["${:.1f}M", "{:.1f}", "${:.1f}M", "{:.2f}", "{:.1f}", "${:.1f}M", "{:.2f}%", "{:,.0f}"],
    "trend_query": None,
    "trend_cols": None,
    "treemap_query": """SELECT SEGMENTO_COMPORTAMENTAL, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Segmento Comportamental"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(NPS_SCORE), 1) AS NPS_SCORE FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Mapa de NPS por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "NPS_SCORE", "caption": "Tamano: volumen clientes | Color: NPS score promedio"},
    "diagnostics": None,
    "simulator": None,
}

render_page(config)

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from app_pages.conn_helper import run_query

TABLE = config["table"]
CHART_LAYOUT = dict(paper_bgcolor="#FAFBFC", plot_bgcolor="#FAFBFC", font=dict(family="Inter, sans-serif", color="#334155"))
NAVY = ["#0F2B46", "#1B3A5C", "#2E7D8C", "#29B5E8", "#0F4C75", "#3282B8", "#11567F", "#1A5276"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de Segmentacion")

# SCATTER - FULL WIDTH
df_scatter = run_query(f"""SELECT ROUND(VALOR_CLIENTE_ANUAL_COP/1e6, 2) AS VALOR_M, PROBABILIDAD_CHURN,
    SEGMENTO_COMPORTAMENTAL, NUM_PRODUCTOS FROM {TABLE}""")
fig_scatter = px.scatter(df_scatter, x="VALOR_M", y="PROBABILIDAD_CHURN",
    color="SEGMENTO_COMPORTAMENTAL", size="NUM_PRODUCTOS",
    color_discrete_sequence=NAVY)
fig_scatter.update_layout(**CHART_LAYOUT, title="Valor Cliente vs Probabilidad Churn",
    xaxis_title="Valor Cliente (M COP)", yaxis_title="Probabilidad Churn")
st.plotly_chart(fig_scatter, use_container_width=True)

c1, c2 = st.columns(2)

df_radar = run_query(f"""SELECT SEGMENTO_COMPORTAMENTAL,
    ROUND(AVG(INDICE_DIGITALIZACION), 2) AS DIGITALIZACION,
    ROUND(AVG(NPS_SCORE)/100, 2) AS NPS,
    ROUND(AVG(SHARE_OF_WALLET), 2) AS SHARE_WALLET,
    ROUND(AVG(POTENCIAL_CRECIMIENTO), 2) AS POTENCIAL,
    ROUND(AVG(NUM_PRODUCTOS)/10, 2) AS PRODUCTOS
FROM {TABLE} GROUP BY 1""")
categories = ["Digitalizacion", "NPS", "Share Wallet", "Potencial", "Productos"]
fig_radar = go.Figure()
for i, row in df_radar.iterrows():
    vals = [row["DIGITALIZACION"], row["NPS"], row["SHARE_WALLET"], row["POTENCIAL"], row["PRODUCTOS"]]
    fig_radar.add_trace(go.Scatterpolar(r=vals + [vals[0]], theta=categories + [categories[0]],
        name=row["SEGMENTO_COMPORTAMENTAL"], line=dict(color=NAVY[i % len(NAVY)])))
fig_radar.update_layout(**CHART_LAYOUT, title="Radar por Segmento", polar=dict(bgcolor="#FAFBFC"))
c1.plotly_chart(fig_radar, use_container_width=True)

df_ciclo = run_query(f"""SELECT CICLO_VIDA, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
fig_donut = go.Figure(go.Pie(labels=df_ciclo["CICLO_VIDA"], values=df_ciclo["N"], hole=0.5,
    marker=dict(colors=NAVY)))
fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Ciclo de Vida")
c2.plotly_chart(fig_donut, use_container_width=True)

df_sun = run_query(f"""SELECT SEGMENTO_COMPORTAMENTAL, PERFIL_FINANCIERO, COUNT(*) AS N
FROM {TABLE} GROUP BY 1, 2 ORDER BY 3 DESC""")
fig_sun = px.sunburst(df_sun, path=["SEGMENTO_COMPORTAMENTAL", "PERFIL_FINANCIERO"], values="N",
    color_discrete_sequence=NAVY)
fig_sun.update_layout(**CHART_LAYOUT, title="Segmento → Perfil Financiero")
st.plotly_chart(fig_sun, use_container_width=True)

# BOX PLOT - FULL WIDTH
df_box = run_query(f"""SELECT SEGMENTO_COMPORTAMENTAL, ROUND(INGRESO_ESTIMADO_COP/1e6, 2) AS INGRESO_M FROM {TABLE}""")
fig_box = px.box(df_box, x="SEGMENTO_COMPORTAMENTAL", y="INGRESO_M", color="SEGMENTO_COMPORTAMENTAL",
    color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Ingreso Estimado por Segmento (M COP)", showlegend=False)
st.plotly_chart(fig_box, use_container_width=True)

df_grouped = run_query(f"""SELECT PERFIL_FINANCIERO,
    ROUND(AVG(VALOR_CLIENTE_ANUAL_COP)/1e6, 2) AS ARPU_M,
    ROUND(AVG(INDICE_DIGITALIZACION), 2) AS DIGITALIZACION
FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC""")
fig_grp = go.Figure()
fig_grp.add_trace(go.Bar(name="ARPU (M)", x=df_grouped["PERFIL_FINANCIERO"], y=df_grouped["ARPU_M"], marker_color=NAVY[0]))
fig_grp.add_trace(go.Bar(name="Digitalizacion", x=df_grouped["PERFIL_FINANCIERO"], y=df_grouped["DIGITALIZACION"], marker_color=NAVY[3]))
fig_grp.update_layout(**CHART_LAYOUT, barmode="group", title="ARPU y Digitalizacion por Perfil Financiero")
st.plotly_chart(fig_grp, use_container_width=True)
