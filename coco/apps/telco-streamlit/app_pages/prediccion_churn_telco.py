from app_pages.page_template import render_page

config = {
    "key": "pct",
    "table": "MGG_TELCO.CLIENTE_Y_RETENCION.PREDICCION_CHURN_TELCO",
    "icon": ":material/person_off:",
    "title": "Prediccion Churn",
    "subtitle": "Modelo predictivo de fuga de clientes integrando consumo, calidad de red, quejas y competencia.",
    "cards": [
        "Identifica clientes con alta probabilidad de fuga antes de que soliciten portabilidad, permitiendo acciones preventivas de retencion.",
        "Integra variaciones de consumo, calidad percibida (QoE), historial de quejas, llamadas a retencion y competencia para calcular score de churn.",
        "Reduce la tasa de churn en 15-25%. Cada punto de churn evitado equivale a miles de millones en revenue recurrente protegido.",
    ],
    "date_col": "FECHA_PREDICCION",
    "filter_cols": ["SEGMENTO_CHURN", "PLAN_ACTUAL", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(CHURN_SCORE)*100, 1) AS SCORE_PROM,
        ROUND(AVG(CONFIANZA_PREDICCION)*100, 1) AS CONFIANZA,
        ROUND(AVG(QOE_SCORE), 1) AS QOE_PROM,
        ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU_K,
        ROUND(AVG(DATOS_GB_MES), 1) AS DATOS_GB,
        ROUND(AVG(QUEJAS_6M), 1) AS QUEJAS_PROM,
        ROUND(SUM(CASE WHEN NOT PERMANENCIA_VIGENTE THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS SIN_PERM_PCT,
        COUNT(*) AS PREDICCIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score churn", "Confianza", "QoE prom.", "ARPU (K)", "Datos GB/mes", "Quejas 6M", "Sin permanencia", "Predicciones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}", "${:.1f}K", "{:.1f}", "{:.1f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_PREDICCION) AS MES,
        ROUND(AVG(CHURN_SCORE)*100, 1) AS CHURN_SCORE,
        ROUND(AVG(QOE_SCORE), 1) AS QOE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Churn score (%)", "QoE"],
    "treemap_query": """SELECT SEGMENTO_CHURN, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Distribucion por segmento de churn"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(CHURN_SCORE)*100, 1) AS CHURN
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Riesgo de churn por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "CHURN", "caption": "Tamano: clientes | Color: score churn"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Riesgo de Churn",
        "desc": "Estima la probabilidad de fuga de un cliente basado en sus indicadores.",
        "features": [
            {"name": "Quejas ultimos 6M", "min": 0, "max": 10, "default": 2, "weight": 0.25, "desc": "Mayor quejas = mayor riesgo"},
            {"name": "Cambio consumo MoM (%)", "min": -50, "max": 50, "default": -5, "weight": -0.20, "desc": "Caida de consumo indica desinteres"},
            {"name": "QoE Score (0-10)", "min": 0, "max": 10, "default": 7, "step": 0.5, "weight": -0.20, "desc": "Menor calidad percibida = mayor riesgo"},
            {"name": "Fallas red experimentadas", "min": 0, "max": 20, "default": 3, "weight": 0.20, "desc": "Eventos de falla impactan satisfaccion"},
            {"name": "Meses antiguedad", "min": 1, "max": 120, "default": 24, "weight": -0.15, "desc": "Mayor antiguedad = menor riesgo"},
        ],
        "thresholds": [0.35, 0.65],
        "labels": ["Bajo riesgo", "Riesgo moderado", "Alto riesgo"],
    },
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
st.subheader(":material/bar_chart: Analisis Avanzado de Churn")

# SCATTER + RADAR side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Scatter: Churn Score vs QoE (color = Segmento)**")
    sc_df = run_query(f"""
        SELECT SEGMENTO_CHURN,
            ROUND(AVG(CHURN_SCORE)*100, 1) AS CHURN,
            ROUND(AVG(QOE_SCORE), 2) AS QOE,
            ROUND(AVG(ARPU_COP)/1e3, 1) AS ARPU_K,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="QOE", y="CHURN", size="ARPU_K", color="SEGMENTO_CHURN",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"N": ":,.0f", "ARPU_K": ":,.1f"},
                           labels={"QOE": "QoE Score", "CHURN": "Churn Score (%)"})
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_b:
    st.markdown("**Radar: Factores de Riesgo por Segmento**")
    radar_df = run_query(f"""
        SELECT SEGMENTO_CHURN,
            ROUND(AVG(QUEJAS_6M)*10, 1) AS QUEJAS_N,
            ROUND(AVG(FALLAS_RED_EXPERIMENTADAS)*5, 1) AS FALLAS_N,
            ROUND((1-AVG(QOE_SCORE)/10)*100, 1) AS INSATISFACCION,
            ROUND(ABS(LEAST(AVG(CAMBIO_CONSUMO_MOM_PCT), 0))*2, 1) AS CAIDA_CONSUMO,
            ROUND(AVG(LLAMADAS_RETENCION)*20, 1) AS LLAMADAS_N
        FROM {TABLE} GROUP BY 1
    """)
    if not radar_df.empty:
        categories = ["Quejas", "Fallas red", "Insatisfaccion", "Caida consumo", "Llamadas retencion"]
        fig_radar = go.Figure()
        for i, row in radar_df.iterrows():
            vals = [row["QUEJAS_N"], row["FALLAS_N"], row["INSATISFACCION"], row["CAIDA_CONSUMO"], row["LLAMADAS_N"]]
            fig_radar.add_trace(go.Scatterpolar(
                r=vals + [vals[0]], theta=categories + [categories[0]],
                fill="toself", name=row["SEGMENTO_CHURN"],
                line=dict(color=NAVY[i % len(NAVY)]),
                fillcolor=f"rgba({int(NAVY[i % len(NAVY)][1:3], 16)},{int(NAVY[i % len(NAVY)][3:5], 16)},{int(NAVY[i % len(NAVY)][5:7], 16)},0.12)",
            ))
        fig_radar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=60, r=60, t=30, b=30),
                               polar=dict(radialaxis=dict(visible=True, range=[0, 100]), bgcolor="#FAFBFC"),
                               legend=dict(orientation="h", yanchor="bottom", y=-0.2, x=0.5, xanchor="center"),
                               showlegend=True)
        st.plotly_chart(fig_radar, use_container_width=True)

# DONUT + WATERFALL side by side
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Donut: Operador Destino Probable**")
    donut_df = run_query(f"""
        SELECT OPERADOR_DESTINO_PROBABLE, COUNT(*) AS N
        FROM {TABLE}
        WHERE CHURN_SCORE >= 0.5
        GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["OPERADOR_DESTINO_PROBABLE"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f} clientes en riesgo<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

with col_d:
    st.markdown("**Waterfall: Revenue en Riesgo por Segmento**")
    wf_df = run_query(f"""
        SELECT SEGMENTO_CHURN,
            ROUND(SUM(ARPU_COP * CHURN_SCORE)/1e9, 2) AS REV_RIESGO_B
        FROM {TABLE} GROUP BY 1 ORDER BY REV_RIESGO_B DESC
    """)
    if not wf_df.empty:
        labels = wf_df["SEGMENTO_CHURN"].tolist() + ["Total riesgo"]
        values = wf_df["REV_RIESGO_B"].tolist() + [0]
        measures = ["relative"] * len(wf_df) + ["total"]
        fig_wf = go.Figure(go.Waterfall(
            x=labels, y=values, measure=measures,
            connector=dict(line=dict(color="#1B3A5C", width=1.5)),
            increasing=dict(marker=dict(color="#C0392B")),
            totals=dict(marker=dict(color="#0F2B46")),
            textposition="outside",
            text=[f"${v:.2f}B" for v in wf_df["REV_RIESGO_B"]] + [f"${sum(wf_df['REV_RIESGO_B']):.2f}B"],
        ))
        fig_wf.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                           yaxis=dict(title="Revenue en riesgo (B COP)", gridcolor="#E2E8F0"), showlegend=False)
        st.plotly_chart(fig_wf, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Box Plot: Antiguedad (meses) por Segmento de Churn**")
box_df = run_query(f"SELECT SEGMENTO_CHURN, MESES_ANTIGUEDAD FROM {TABLE}")
if not box_df.empty:
    fig_box = px.box(box_df, x="SEGMENTO_CHURN", y="MESES_ANTIGUEDAD", color="SEGMENTO_CHURN",
                    color_discrete_sequence=NAVY,
                    labels={"MESES_ANTIGUEDAD": "Meses de antiguedad", "SEGMENTO_CHURN": ""})
    fig_box.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                        showlegend=False, yaxis=dict(gridcolor="#E2E8F0"))
    st.plotly_chart(fig_box, use_container_width=True)

# HEATMAP - FULL WIDTH
st.markdown("**Heatmap: Score Churn por Plan y Motivo de Riesgo**")
hm_df = run_query(f"""
    SELECT PLAN_ACTUAL, MOTIVO_RIESGO_PRINCIPAL,
        ROUND(AVG(CHURN_SCORE)*100, 1) AS SCORE
    FROM {TABLE}
    WHERE PLAN_ACTUAL IN (SELECT PLAN_ACTUAL FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 6)
    GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="PLAN_ACTUAL", columns="MOTIVO_RIESGO_PRINCIPAL", values="SCORE", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#E8F4F8"], [0.3, "#29B5E8"], [0.6, "#E8963A"], [1, "#C0392B"]],
        text=[[f"{v:.1f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=10, color="white"),
        hovertemplate="Plan: %{y}<br>Motivo: %{x}<br>Score: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Churn %"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=140, r=20, t=30, b=80),
                        xaxis=dict(title="Motivo riesgo principal", tickangle=-25), yaxis=dict(title=""))
    st.plotly_chart(fig_hm, use_container_width=True)
