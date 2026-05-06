from app_pages.page_template import render_page

config = {
    "key": "rvs",
    "table": "MGG_FINTECH.PARTNERS_BAAS.REVENUE_SHARING",
    "icon": ":material/pie_chart:",
    "title": "Revenue Sharing",
    "subtitle": "Liquidacion y reparto de revenue entre fintech, partners y red de pagos.",
    "cards": ["Reparto automatico de ingresos entre fintech, partner, red de pagos y sponsor", "Registra regla de reparto, monto por actor, frecuencia de liquidacion y conciliacion", "Automatiza settlement entre partes garantizando transparencia en distribucion de ingresos"],
    "date_col": "FECHA_PERIODO",
    "filter_cols": ["PARTNER", "PRODUCTO", "ESTADO_LIQUIDACION"],
    "kpi_query": """SELECT ROUND(SUM(REVENUE_BRUTO_COP)/1e9,1), ROUND(AVG(MARGEN_NETO_PCT),1), ROUND(SUM(CASE WHEN CONCILIADO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(HORAS_HASTA_LIQUIDACION),0), ROUND(SUM(DISPUTAS_COP)/1e6,1), ROUND(SUM(MONTO_FINTECH_COP)/1e9,1), ROUND(SUM(MONTO_PARTNER_COP)/1e9,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Revenue bruto (B)", "Margen neto %", "Conciliado %", "Horas liquidacion", "Disputas (M)", "Monto fintech (B)", "Monto partner (B)", "Liquidaciones"],
    "kpi_formats": ["${:.1f}B", "{:.1f}%", "{:.1f}%", "{:.0f}", "${:.1f}M", "${:.1f}B", "${:.1f}B", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_PERIODO) AS MES, ROUND(SUM(REVENUE_BRUTO_COP)/1e9,2) AS REVENUE_BRUTO, ROUND(AVG(MARGEN_NETO_PCT),1) AS MARGEN_NETO_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["REVENUE_BRUTO", "MARGEN_NETO_PCT"],
    "treemap_query": """SELECT PARTNER AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Partner"},
    "geo_query": None,
    "geo_config": None,
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
SEMAFORO = ["#2D9B2D", "#6ABF4B", "#F5D63D", "#F5A623", "#E63946", "#1B8C1B", "#8BC34A", "#D32F2F"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de Revenue Sharing")

df_heatmap = run_query(f"SELECT PARTNER, PRODUCTO, ROUND(AVG(MARGEN_NETO_PCT),1) AS MARGEN FROM {TABLE} GROUP BY 1,2")
pivot = df_heatmap.pivot_table(index="PARTNER", columns="PRODUCTO", values="MARGEN", aggfunc="mean").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0, SEMAFORO[3]], [1, SEMAFORO[0]]], texttemplate="%{z:.1f}%"))
fig_hm.update_layout(**CHART_LAYOUT, title="Margen Neto (%) por Partner × Producto", height=420)
st.plotly_chart(fig_hm, use_container_width=True)

col1, col2 = st.columns(2)
df_scatter = run_query(f"SELECT VOLUMEN_BRUTO_COP/1e9 AS VOL_B, MARGEN_NETO_PCT, ESTADO_LIQUIDACION FROM {TABLE} SAMPLE (2000 ROWS)")
fig_sc = px.scatter(df_scatter, x="VOL_B", y="MARGEN_NETO_PCT", color="ESTADO_LIQUIDACION", color_discrete_sequence=SEMAFORO, opacity=0.7)
fig_sc.update_layout(**CHART_LAYOUT, title="Volumen Bruto (B) vs Margen Neto %")
col1.plotly_chart(fig_sc, use_container_width=True)

df_donut = run_query(f"SELECT FRECUENCIA_LIQUIDACION, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
fig_dn = px.pie(df_donut, names="FRECUENCIA_LIQUIDACION", values="N", hole=0.5, color_discrete_sequence=SEMAFORO)
fig_dn.update_layout(**CHART_LAYOUT, title="Frecuencia de Liquidacion")
col2.plotly_chart(fig_dn, use_container_width=True)

col3, col4 = st.columns(2)
df_wf = run_query(f"SELECT PARTNER, ROUND(SUM(REVENUE_BRUTO_COP)/1e9,2) AS REV FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
fig_wf = go.Figure(go.Waterfall(x=df_wf["PARTNER"].tolist(), y=df_wf["REV"].tolist(), connector=dict(line=dict(color=SEMAFORO[3]))))
fig_wf.update_layout(**CHART_LAYOUT, title="Revenue Bruto (B) por Partner")
col3.plotly_chart(fig_wf, use_container_width=True)

df_box = run_query(f"SELECT PARTNER, HORAS_HASTA_LIQUIDACION FROM {TABLE} WHERE PARTNER IN (SELECT PARTNER FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 6)")
fig_bx = px.box(df_box, x="PARTNER", y="HORAS_HASTA_LIQUIDACION", color_discrete_sequence=[SEMAFORO[2]])
fig_bx.update_layout(**CHART_LAYOUT, title="Horas hasta Liquidacion por Partner")
col4.plotly_chart(fig_bx, use_container_width=True)
