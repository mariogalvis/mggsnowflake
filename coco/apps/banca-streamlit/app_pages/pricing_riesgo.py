from app_pages.page_template import render_page

config = {
    "key": "pri",
    "table": "MGG_BANCA.RIESGO_Y_CREDITO.PRICING_RIESGO",
    "icon": ":material/price_change:",
    "title": "Pricing de Riesgo",
    "subtitle": "Motor de pricing diferenciado que ajusta tasa, spread y condiciones segun perfil de riesgo del cliente.",
    "cards": [
        "Optimiza la rentabilidad ajustada a riesgo asignando tasas coherentes con la perdida esperada.",
        "Calcula spread por segmento combinando PD, LGD, EAD y costo de fondeo con modelos de pricing.",
        "Incrementa NIM en 15-25 bps y mejora la competitividad en segmentos de bajo riesgo.",
    ],
    "date_col": "FECHA_DESEMBOLSO",
    "filter_cols": ["TIPO_CREDITO", "SEGMENTO_RIESGO", "TIPO_GARANTIA", "CIUDAD"],
    "kpi_query": """SELECT
        COUNT(*) AS TOTAL_CREDITOS,
        ROUND(AVG(TASA_EA_PCT), 2) AS TASA_EA_PROM,
        ROUND(AVG(SPREAD_RIESGO_PCT), 2) AS SPREAD_PROM,
        ROUND(SUM(MONTO_DESEMBOLSADO_COP) / 1e9, 2) AS CARTERA_TOTAL_B,
        ROUND(AVG(PERDIDA_ESPERADA_PCT), 2) AS PE_PROM,
        ROUND(AVG(LTV) * 100, 1) AS LTV_PROM,
        ROUND(AVG(DTI) * 100, 1) AS DTI_PROM,
        ROUND(AVG(RENTABILIDAD_AJUSTADA_RIESGO_PCT), 2) AS RAROC_PROM
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Total Creditos", "Tasa EA Prom %", "Spread Prom %", "Cartera Total (B)", "Perdida Esperada %", "LTV Prom %", "DTI Prom %", "RAROC Prom %"],
    "kpi_formats": ["{:,.0f}", "{:.2f}%", "{:.2f}%", "${:.2f}B", "{:.2f}%", "{:.1f}%", "{:.1f}%", "{:.2f}%"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_DESEMBOLSO) AS MES,
        ROUND(AVG(TASA_EA_PCT), 2) AS TASA_EA,
        ROUND(AVG(SPREAD_RIESGO_PCT), 2) AS SPREAD
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Tasa EA %", "Spread %"],
    "treemap_query": """SELECT SEGMENTO_RIESGO, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Creditos por Segmento de Riesgo"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(RENTABILIDAD_AJUSTADA_RIESGO_PCT), 2) AS RAROC FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Rentabilidad Ajustada por Ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "RAROC", "caption": "Tamano: volumen creditos | Color: RAROC promedio"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Pricing de Riesgo")

# SCATTER - FULL WIDTH
st.markdown("**Tasa EA vs Probabilidad de Default por Segmento**")
df_sc = run_query(f"SELECT TASA_EA_PCT, PROBABILIDAD_DEFAULT, SEGMENTO_RIESGO, MONTO_DESEMBOLSADO_COP FROM {TABLE} WHERE TASA_EA_PCT IS NOT NULL")
fig = px.scatter(df_sc, x="TASA_EA_PCT", y="PROBABILIDAD_DEFAULT", color="SEGMENTO_RIESGO", size="MONTO_DESEMBOLSADO_COP", color_discrete_sequence=NAVY, opacity=0.7)
fig.update_layout(**CHART_LAYOUT, xaxis_title="Tasa EA %", yaxis_title="Probabilidad Default")
st.plotly_chart(fig, use_container_width=True)

# HEATMAP + DONUT - SIDE BY SIDE
col_a, col_b = st.columns(2)
with col_a:
    st.markdown("**Rentabilidad Ajustada por Segmento y Tipo de Credito**")
    df_heat = run_query(f"SELECT SEGMENTO_RIESGO, TIPO_CREDITO, ROUND(AVG(RENTABILIDAD_AJUSTADA_RIESGO_PCT), 2) AS RAROC FROM {TABLE} GROUP BY 1, 2")
    pivot = df_heat.pivot_table(index="SEGMENTO_RIESGO", columns="TIPO_CREDITO", values="RAROC", aggfunc="mean")
    fig = go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, NAVY[0]], [1, NAVY[3]]], texttemplate="%{z:.2f}%"))
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)
with col_b:
    st.markdown("**Distribucion por Tipo de Garantia**")
    df_donut = run_query(f"SELECT TIPO_GARANTIA, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig = px.pie(df_donut, names="TIPO_GARANTIA", values="N", hole=0.5, color_discrete_sequence=NAVY)
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

# RADAR - FULL WIDTH
st.markdown("**Metricas de Riesgo por Segmento**")
df_radar = run_query(f"""SELECT SEGMENTO_RIESGO,
    ROUND(AVG(PROBABILIDAD_DEFAULT) * 100, 1) AS PD,
    ROUND(AVG(SEVERIDAD_PERDIDA) * 100, 1) AS LGD,
    ROUND(AVG(LTV) * 100, 1) AS LTV,
    ROUND(AVG(DTI) * 100, 1) AS DTI,
    ROUND(AVG(SPREAD_RIESGO_PCT), 1) AS SPREAD
FROM {TABLE} GROUP BY 1""")
categories = ["PD", "LGD", "LTV", "DTI", "SPREAD"]
fig = go.Figure()
for i, row in df_radar.iterrows():
    fig.add_trace(go.Scatterpolar(r=[row[c] for c in categories], theta=categories, fill="toself", name=row["SEGMENTO_RIESGO"], line=dict(color=NAVY[i % len(NAVY)])))
fig.update_layout(**CHART_LAYOUT, polar=dict(bgcolor="#FAFBFC"))
st.plotly_chart(fig, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Spread de Riesgo por Segmento**")
df_box = run_query(f"SELECT SEGMENTO_RIESGO, SPREAD_RIESGO_PCT FROM {TABLE}")
fig = px.box(df_box, x="SEGMENTO_RIESGO", y="SPREAD_RIESGO_PCT", color="SEGMENTO_RIESGO", color_discrete_sequence=NAVY)
fig.update_layout(**CHART_LAYOUT, showlegend=False)
st.plotly_chart(fig, use_container_width=True)

# WATERFALL - FULL WIDTH
st.markdown("**Composicion de Tasa por Segmento de Riesgo**")
df_wf = run_query(f"""SELECT SEGMENTO_RIESGO, ROUND(AVG(TASA_EA_PCT) - AVG(SPREAD_RIESGO_PCT), 2) AS TASA_BASE, ROUND(AVG(SPREAD_RIESGO_PCT), 2) AS SPREAD
FROM {TABLE} GROUP BY 1 ORDER BY 2""")
base_prom = df_wf["TASA_BASE"].mean()
fig = go.Figure(go.Waterfall(
    x=["Tasa Base"] + df_wf["SEGMENTO_RIESGO"].tolist(),
    y=[base_prom] + df_wf["SPREAD"].tolist(),
    measure=["absolute"] + ["relative"] * len(df_wf),
    connector=dict(line=dict(color=NAVY[1])),
    increasing=dict(marker=dict(color=NAVY[0])),
    decreasing=dict(marker=dict(color=NAVY[3])),
    totals=dict(marker=dict(color=NAVY[4]))
))
fig.update_layout(**CHART_LAYOUT, yaxis_title="Tasa %")
st.plotly_chart(fig, use_container_width=True)
