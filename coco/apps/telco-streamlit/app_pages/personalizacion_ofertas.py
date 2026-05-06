from app_pages.page_template import render_page

config = {
    "key": "pof",
    "table": "MGG_TELCO.CLIENTE_Y_RETENCION.PERSONALIZACION_OFERTAS",
    "icon": ":material/redeem:",
    "title": "Personalizacion Ofertas",
    "subtitle": "Motor de ofertas personalizadas por canal con control de fatiga y medicion de impacto en ARPU.",
    "cards": [
        "Entrega la oferta correcta al cliente correcto en el momento correcto, maximizando aceptacion sin generar fatigue.",
        "Modelo que integra propension de aceptacion, relevancia, churn risk, fatigue score y LTV para recomendar oferta y canal optimos.",
        "Incrementa ARPU via ofertas personalizadas con alta aceptacion. Reduce opt-out y mejora percepcion de relevancia comercial.",
    ],
    "date_col": "FECHA_OFERTA",
    "filter_cols": ["TIPO_OFERTA", "CANAL", "TRIGGER_OFERTA"],
    "kpi_query": """SELECT
        ROUND(SUM(CASE WHEN OFERTA_ACEPTADA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS ACEPTACION_PCT,
        ROUND(AVG(PROPENSION_ACEPTACION)*100, 1) AS PROPENSION_PCT,
        ROUND(AVG(RELEVANCIA_SCORE)*100, 1) AS RELEVANCIA_PCT,
        ROUND(AVG(ARPU_INCREMENTAL_COP)/1e3, 1) AS INC_ARPU_K,
        ROUND(AVG(ROI_OFERTA), 1) AS ROI,
        ROUND(AVG(FATIGUE_SCORE)*100, 1) AS FATIGUE_PCT,
        ROUND(SUM(CASE WHEN OPT_OUT THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS OPT_OUT_PCT,
        COUNT(*) AS OFERTAS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Aceptacion", "Propension", "Relevancia", "ARPU increm. (K)", "ROI", "Fatigue", "Opt-out", "Ofertas"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}K", "{:.1f}x", "{:.1f}%", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_OFERTA) AS MES,
        ROUND(SUM(CASE WHEN OFERTA_ACEPTADA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS ACEPTACION,
        ROUND(AVG(FATIGUE_SCORE)*100, 1) AS FATIGUE
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Aceptacion (%)", "Fatigue (%)"],
    "treemap_query": """SELECT TIPO_OFERTA, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Ofertas por tipo"},
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
NAVY = ["#0F2B46", "#1B3A5C", "#2E7D8C", "#29B5E8", "#0F4C75", "#3282B8", "#11567F", "#1A5276"]

st.divider()
st.subheader(":material/bar_chart: Analisis Avanzado de Ofertas")

# FUNNEL - FULL WIDTH
st.markdown("**Funnel: Conversion de Ofertas (Enviadas → Vistas → Aceptadas)**")
fn_data = run_query(f"""
    SELECT
        COUNT(*) AS ENVIADAS,
        SUM(CASE WHEN OFERTA_VISTA THEN 1 ELSE 0 END) AS VISTAS,
        SUM(CASE WHEN OFERTA_ACEPTADA THEN 1 ELSE 0 END) AS ACEPTADAS
    FROM {TABLE}
""")
if not fn_data.empty:
    r = fn_data.iloc[0]
    fig_fn = go.Figure(go.Funnel(
        y=["Enviadas", "Vistas", "Aceptadas"],
        x=[int(r["ENVIADAS"]), int(r["VISTAS"]), int(r["ACEPTADAS"])],
        textposition="inside",
        textinfo="value+percent initial",
        marker=dict(color=["#1B3A5C", "#2E7D8C", "#29B5E8"]),
        connector=dict(line=dict(color="#E2E8F0", width=2)),
    ))
    fig_fn.update_layout(**CHART_LAYOUT, height=320, margin=dict(l=20, r=20, t=30, b=20))
    st.plotly_chart(fig_fn, use_container_width=True)

# HEATMAP - FULL WIDTH
st.markdown("**Heatmap: Tasa Aceptacion por Canal y Trigger**")
hm_df = run_query(f"""
    SELECT CANAL, TRIGGER_OFERTA,
        ROUND(SUM(CASE WHEN OFERTA_ACEPTADA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS ACEPT_PCT
    FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="CANAL", columns="TRIGGER_OFERTA", values="ACEPT_PCT", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#E8F4F8"], [0.3, "#29B5E8"], [0.7, "#2E7D8C"], [1, "#0F2B46"]],
        text=[[f"{v:.1f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=10, color="white"),
        hovertemplate="Canal: %{y}<br>Trigger: %{x}<br>Aceptacion: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Aceptacion %"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=100, r=20, t=30, b=80),
                        xaxis=dict(title="Trigger", tickangle=-30), yaxis=dict(title=""))
    st.plotly_chart(fig_hm, use_container_width=True)

# SCATTER + DONUT side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Scatter: Propension vs Relevancia (color = Tipo Oferta)**")
    sc_df = run_query(f"""
        SELECT TIPO_OFERTA,
            ROUND(AVG(PROPENSION_ACEPTACION)*100, 1) AS PROPENSION,
            ROUND(AVG(RELEVANCIA_SCORE)*100, 1) AS RELEVANCIA,
            ROUND(AVG(VALOR_OFERTA_COP)/1e3, 1) AS VALOR_K,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="PROPENSION", y="RELEVANCIA", size="VALOR_K", color="TIPO_OFERTA",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"N": ":,.0f"},
                           labels={"PROPENSION": "Propension (%)", "RELEVANCIA": "Relevancia (%)"})
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_b:
    st.markdown("**Donut: Distribucion por Tipo de Oferta**")
    donut_df = run_query(f"""
        SELECT TIPO_OFERTA, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["TIPO_OFERTA"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f} ofertas<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Box Plot: Valor de Oferta por Canal**")
box_df = run_query(f"SELECT CANAL, VALOR_OFERTA_COP/1e3 AS VALOR_K FROM {TABLE}")
if not box_df.empty:
    fig_box = px.box(box_df, x="CANAL", y="VALOR_K", color="CANAL",
                    color_discrete_sequence=NAVY,
                    labels={"VALOR_K": "Valor oferta (K COP)", "CANAL": ""})
    fig_box.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=40, r=20, t=30, b=60),
                        showlegend=False, yaxis=dict(gridcolor="#E2E8F0"))
    st.plotly_chart(fig_box, use_container_width=True)
