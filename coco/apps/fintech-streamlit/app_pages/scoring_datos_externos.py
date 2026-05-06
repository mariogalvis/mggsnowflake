from app_pages.page_template import render_page

config = {
    "key": "sde",
    "table": "MGG_FINTECH.OPEN_BANKING.SCORING_DATOS_EXTERNOS",
    "icon": ":material/score:",
    "title": "Scoring Alternativo",
    "subtitle": "Scoring crediticio con datos alternativos para poblacion sin historial bancario tradicional.",
    "cards": ["Scoring crediticio alternativo usando datos en tiempo real: operador celular, servicios, ecommerce", "Evalua clientes sin historial crediticio via APIs externas y fuentes de datos no tradicionales", "Amplia inclusion financiera a poblacion no bancarizada en Colombia con decisiones data-driven"],
    "date_col": "FECHA_EVALUACION",
    "filter_cols": ["SEGMENTO_CLIENTE", "RESULTADO_EVALUACION", "FUENTE_PRINCIPAL"],
    "kpi_query": """SELECT ROUND(AVG(SCORE_ALTERNATIVO),1), ROUND(AVG(SCORE_BUREAU_TRADICIONAL),1), ROUND(AVG(PROBABILIDAD_DEFAULT_ALT)*100,1), ROUND(AVG(CONFIANZA_INGRESO)*100,1), ROUND(AVG(CUPO_APROBADO_COP)/1e6,2), ROUND(AVG(LATENCIA_EVALUACION_MS),1), ROUND(AVG(SCORE_ESTABILIDAD_DIGITAL)*100,1), COUNT(*) FROM {table} WHERE {where}""",
    "kpi_labels": ["Score Alt", "Score Bureau", "Default %", "Confianza Ingreso %", "Cupo M", "Latencia ms", "Estabilidad %", "Total"],
    "kpi_formats": ["{:.1f}", "{:.1f}", "{:.1f}%", "{:.1f}%", "${:.2f}M", "{:.1f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_EVALUACION) AS MES, ROUND(AVG(SCORE_ALTERNATIVO),1) AS SCORE_ALT, ROUND(AVG(PROBABILIDAD_DEFAULT_ALT)*100,1) AS DEFAULT_PCT FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["SCORE_ALT", "DEFAULT_PCT"],
    "treemap_query": """SELECT SEGMENTO_CLIENTE AS CAT, COUNT(*) AS N FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por Segmento Cliente"},
    "geo_query": """SELECT CIUDAD, AVG(SCORE_ALTERNATIVO) AS VALOR FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Score Alternativo por Ciudad", "color_col": "VALOR"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Scoring Alternativo",
        "desc": "Ajusta las variables para estimar la probabilidad de default usando datos no tradicionales.",
        "features": [
            {"name": "Score Alternativo", "min": 0, "max": 100, "default": 60, "weight": -0.3, "step": 1},
            {"name": "Score Bureau Tradicional", "min": 0, "max": 100, "default": 55, "weight": -0.2, "step": 1},
            {"name": "Prob Default Alternativa", "min": 0, "max": 1, "default": 0.15, "weight": 0.25, "step": 0.01},
            {"name": "Confianza Ingreso", "min": 0, "max": 1, "default": 0.7, "weight": -0.15, "step": 0.05},
            {"name": "Estabilidad Digital", "min": 0, "max": 100, "default": 65, "weight": -0.1, "step": 1},
        ],
        "thresholds": [0.33, 0.66],
        "labels": ["Riesgo Bajo", "Riesgo Medio", "Riesgo Alto"],
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
st.subheader(":material/bar_chart: Analisis Avanzado de Scoring Alternativo")

df_heat = run_query(f"SELECT SEGMENTO_CLIENTE, RESULTADO_EVALUACION, ROUND(AVG(PROBABILIDAD_DEFAULT_ALT)*100,1) AS PD FROM {TABLE} GROUP BY 1,2")
pivot = df_heat.pivot(index="SEGMENTO_CLIENTE", columns="RESULTADO_EVALUACION", values="PD").fillna(0)
fig_heat = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(), colorscale=[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]], texttemplate="%{z:.1f}%"))
fig_heat.update_layout(**CHART_LAYOUT, title="Probabilidad Default: Segmento × Resultado")
st.plotly_chart(fig_heat, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    df_sc = run_query(f"SELECT SCORE_ALTERNATIVO, SCORE_BUREAU_TRADICIONAL AS SCORE_BUREAU, RESULTADO_EVALUACION FROM {TABLE} LIMIT 2000")
    fig_sc = px.scatter(df_sc, x="SCORE_ALTERNATIVO", y="SCORE_BUREAU", color="RESULTADO_EVALUACION", color_discrete_sequence=SEMAFORO)
    fig_sc.update_layout(**CHART_LAYOUT, title="Score Alternativo vs Bureau")
    st.plotly_chart(fig_sc, use_container_width=True)
with col2:
    df_donut = run_query(f"SELECT FUENTE_PRINCIPAL, COUNT(*) AS N FROM {TABLE} GROUP BY 1")
    fig_donut = go.Figure(go.Pie(labels=df_donut["FUENTE_PRINCIPAL"], values=df_donut["N"], hole=0.5, marker=dict(colors=SEMAFORO)))
    fig_donut.update_layout(**CHART_LAYOUT, title="Distribucion por Fuente Principal")
    st.plotly_chart(fig_donut, use_container_width=True)

df_hist = run_query(f"SELECT SCORE_ALTERNATIVO FROM {TABLE} LIMIT 5000")
fig_hist = px.histogram(df_hist, x="SCORE_ALTERNATIVO", nbins=40, color_discrete_sequence=SEMAFORO)
fig_hist.update_layout(**CHART_LAYOUT, title="Distribucion de Score Alternativo")
st.plotly_chart(fig_hist, use_container_width=True)

df_box = run_query(f"SELECT SEGMENTO_CLIENTE AS SEGMENTO, CUPO_APROBADO_COP/1e6 AS CUPO_M FROM {TABLE} LIMIT 3000")
fig_box = px.box(df_box, x="SEGMENTO", y="CUPO_M", color_discrete_sequence=SEMAFORO)
fig_box.update_layout(**CHART_LAYOUT, title="Cupo Aprobado (M) por Segmento")
st.plotly_chart(fig_box, use_container_width=True)
