from app_pages.page_template import render_page

config = {
    "key": "cbk",
    "table": "MGG_FINTECH.MONETIZACION.CASHBACK_REWARDS",
    "icon": ":material/redeem:",
    "title": "Cashback & Rewards",
    "subtitle": "Gestion de programas de cashback con ROI, redencion y engagement post-reward.",
    "cards": ["Motor de cashback y recompensas dinamicas personalizadas en tiempo real por usuario", "Registra oferta, comercio, porcentaje, monto devuelto, costo y conversion de cada incentivo", "Incentiva comportamientos deseados (activacion, frecuencia, gasto) midiendo retorno de cada incentivo"],
    "date_col": "FECHA_HORA",
    "filter_cols": ["CATEGORIA", "TIPO_REWARD", "TIPO_CAMPANA"],
    "kpi_query": """SELECT ROUND(SUM(CASE WHEN OFERTA_REDIMIDA THEN 1 ELSE 0 END)*100.0/COUNT(*),1), ROUND(AVG(ROI_OFERTA),2), ROUND(AVG(MONTO_CASHBACK_COP)/1e3,1), ROUND(SUM(REVENUE_INCREMENTAL_COP)/1e6,2), ROUND(SUM(COSTO_NETO_COP)/1e6,2), ROUND(AVG(ENGAGEMENT_POST_REWARD)*100,1), ROUND(SUM(CASE WHEN PRIMERA_VEZ_COMERCIO THEN 1 ELSE 0 END)*100.0/COUNT(*),1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Redencion %", "ROI", "Cashback K", "Rev Incr M", "Costo M", "Engagement %", "Primera Vez %", "Total"],
    "kpi_formats": ["{:.1f}%", "{:.2f}", "${:.1f}K", "${:.2f}M", "${:.2f}M", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES, ROUND(SUM(CASE WHEN OFERTA_REDIMIDA THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS REDENCION_PCT, ROUND(AVG(ROI_OFERTA),2) AS ROI FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["REDENCION_PCT", "ROI"],
    "treemap_query": """SELECT CATEGORIA AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Categoria"},
    "geo_query": None,
    "geo_config": None,
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de ROI de Cashback",
        "desc": "Estima el retorno de inversion de una oferta de cashback ajustando los parametros de la campana.",
        "features": [
            {"name": "Porcentaje Cashback %", "min": 0.5, "max": 10, "default": 3, "weight": -0.3, "step": 0.5},
            {"name": "Monto Minimo Compra (K)", "min": 0, "max": 200, "default": 30, "weight": 0.2, "step": 10},
            {"name": "Frecuencia Target (txn/mes)", "min": 1, "max": 30, "default": 5, "weight": 0.25, "step": 1},
            {"name": "Tasa Redencion Historica %", "min": 10, "max": 90, "default": 50, "weight": 0.15, "step": 5},
            {"name": "Duracion Campana (dias)", "min": 7, "max": 90, "default": 30, "weight": 0.1, "step": 7},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["ROI Negativo", "ROI Neutro", "ROI Positivo"],
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
st.subheader(":material/bar_chart: Analisis Avanzado de Cashback & Rewards")

df_heat = run_query(f"SELECT CATEGORIA, TIPO_REWARD, ROUND(SUM(CASE WHEN OFERTA_REDIMIDA THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS REDENCION FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="CATEGORIA", columns="TIPO_REWARD", values="REDENCION").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.1f}%"))
fig_heat.update_layout(**CHART_LAYOUT, title="% Redencion: Categoria × Tipo Reward")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT ROI_OFERTA AS ROI, ENGAGEMENT_POST_REWARD AS ENGAGEMENT_POST, TIPO_CAMPANA FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="ROI", y="ENGAGEMENT_POST", color="TIPO_CAMPANA", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="ROI vs Engagement Post")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_fun = run_query(f"SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN OFERTA_NOTIFICADA THEN 1 ELSE 0 END) AS NOTIFICADA, SUM(CASE WHEN OFERTA_REDIMIDA THEN 1 ELSE 0 END) AS REDIMIDA FROM {TABLE}")
    fig_fun = go.Figure(go.Funnel(y=["Total","Notificada","Redimida"], x=[df_fun["TOTAL"].iloc[0], df_fun["NOTIFICADA"].iloc[0], df_fun["REDIMIDA"].iloc[0]], marker=dict(color=SEMAFORO[:3])))
    fig_fun.update_layout(**CHART_LAYOUT, title="Funnel de Redencion")
    st.plotly_chart(fig_fun, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    df_donut = run_query(f"SELECT TIPO_CAMPANA, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["TIPO_CAMPANA"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Tipo Campana")
    st.plotly_chart(fig_donut, use_container_width=True)
with col4:
    df_box = run_query(f"SELECT CATEGORIA, MONTO_CASHBACK_COP/1e3 AS CASHBACK_K FROM {TABLE} LIMIT 3000")
    fig_box = px.box(df_box, x="CATEGORIA", y="CASHBACK_K", color_discrete_sequence=SEMAFORO)
    fig_box.update_layout(**CHART_LAYOUT, title="Monto Cashback (K) por Categoria")
    st.plotly_chart(fig_box, use_container_width=True)
