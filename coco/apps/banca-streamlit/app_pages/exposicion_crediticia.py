from app_pages.page_template import render_page

config = {
    "key": "exc",
    "table": "MGG_BANCA.DATOS_Y_AI.EXPOSICION_CREDITICIA",
    "icon": ":material/analytics:",
    "title": "Exposicion Crediticia",
    "subtitle": "Vista consolidada de exposicion crediticia con PD, LGD, EAD y perdida esperada por portafolio.",
    "cards": ["TIPO_CREDITO", "CALIFICACION_RIESGO", "SECTOR_ECONOMICO"],
    "date_col": "FECHA_DESEMBOLSO",
    "filter_cols": ["TIPO_CREDITO", "CALIFICACION_RIESGO", "SECTOR_ECONOMICO"],
    "kpi_query": """SELECT
        AVG(PROBABILIDAD_DEFAULT),
        AVG(SEVERIDAD_PERDIDA),
        SUM(SALDO_VIGENTE_COP)/1e9,
        SUM(SALDO_MORA_COP)/1e9,
        SUM(PROVISION_COP)/1e9,
        SUM(PERDIDA_ESPERADA_COP)/1e9,
        AVG(TASA_EA_PCT),
        COUNT(*)
    FROM {table} WHERE {where}""",
    "kpi_labels": ["PD prom", "LGD prom", "Saldo vigente (B)", "Saldo mora (B)", "Provision (B)", "Perdida esperada (B)", "Tasa prom", "Total creditos"],
    "kpi_formats": [",.4f", ",.4f", ",.1f", ",.1f", ",.1f", ",.1f", ",.2f", ",.0f"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_DESEMBOLSO) AS MES, SUM(SALDO_VIGENTE_COP)/1e9 AS SALDO_VIGENTE_B, SUM(PERDIDA_ESPERADA_COP)/1e9 AS PERDIDA_ESPERADA_B FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["SALDO_VIGENTE_B", "PERDIDA_ESPERADA_B"],
    "treemap_query": """SELECT TIPO_CREDITO AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Tipo de Credito"},
    "geo_query": """SELECT CIUDAD_CLIENTE AS CITY, AVG(PROBABILIDAD_DEFAULT) AS VALUE FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "PD por Ciudad", "metric": "PROBABILIDAD_DEFAULT"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Exposicion Crediticia")

df_hm = run_query(f"SELECT TIPO_CREDITO, CALIFICACION_RIESGO, AVG(PROBABILIDAD_DEFAULT) AS PD FROM {TABLE} GROUP BY 1,2")
df_piv = df_hm.pivot(index="TIPO_CREDITO", columns="CALIFICACION_RIESGO", values="PD").fillna(0)
fig_hm = go.Figure(go.Heatmap(z=df_piv.values, x=df_piv.columns.tolist(), y=df_piv.index.tolist(), colorscale=[[0,"#FAFBFC"],[1,"#0F2B46"]]))
fig_hm.update_layout(**CHART_LAYOUT, title="PD Promedio por Tipo Credito y Calificacion", xaxis_title="Calificacion Riesgo", yaxis_title="Tipo Credito")
st.plotly_chart(fig_hm, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    df_sc = run_query(f"SELECT PROBABILIDAD_DEFAULT, SEVERIDAD_PERDIDA, TIPO_CREDITO, SALDO_VIGENTE_COP/1e9 AS SALDO_B FROM {TABLE}")
    fig_sc = px.scatter(df_sc, x="PROBABILIDAD_DEFAULT", y="SEVERIDAD_PERDIDA", color="TIPO_CREDITO", size="SALDO_B", color_discrete_sequence=NAVY)
    fig_sc.update_layout(**CHART_LAYOUT, title="PD vs Severidad Perdida", xaxis_title="Probabilidad Default", yaxis_title="Severidad Perdida")
    st.plotly_chart(fig_sc, use_container_width=True)
with c2:
    df_dn = run_query(f"SELECT CALIFICACION_RIESGO, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_dn = px.pie(df_dn, names="CALIFICACION_RIESGO", values="N", hole=0.45, color_discrete_sequence=NAVY)
    fig_dn.update_layout(**CHART_LAYOUT, title="Distribucion por Calificacion")
    st.plotly_chart(fig_dn, use_container_width=True)

df_wf = run_query(f"SELECT TIPO_CREDITO, SUM(PERDIDA_ESPERADA_COP)/1e6 AS PE_M FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC")
fig_wf = go.Figure(go.Waterfall(x=df_wf["TIPO_CREDITO"], y=df_wf["PE_M"], connector=dict(line=dict(color="#29B5E8")), increasing=dict(marker=dict(color="#0F2B46")), decreasing=dict(marker=dict(color="#2E7D8C"))))
fig_wf.update_layout(**CHART_LAYOUT, title="Perdida Esperada por Tipo de Credito", xaxis_title="Tipo Credito", yaxis_title="Perdida Esperada (Millones COP)")
st.plotly_chart(fig_wf, use_container_width=True)

df_box = run_query(f"SELECT CALIFICACION_RIESGO, DIAS_MORA FROM {TABLE}")
fig_box = px.box(df_box, x="CALIFICACION_RIESGO", y="DIAS_MORA", color="CALIFICACION_RIESGO", color_discrete_sequence=NAVY)
fig_box.update_layout(**CHART_LAYOUT, title="Dias Mora por Calificacion de Riesgo", showlegend=False, xaxis_title="Calificacion Riesgo", yaxis_title="Dias Mora")
st.plotly_chart(fig_box, use_container_width=True)
