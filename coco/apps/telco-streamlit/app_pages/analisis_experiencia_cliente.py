from app_pages.page_template import render_page

config = {
    "key": "aec",
    "table": "MGG_TELCO.EXPERIENCIA_Y_ATENCION.ANALISIS_EXPERIENCIA_CLIENTE",
    "icon": ":material/sentiment_satisfied:",
    "title": "Experiencia Cliente",
    "subtitle": "Analisis integral de NPS, satisfaccion y sentimiento por touchpoint para mejorar la experiencia.",
    "cards": [
        "Mide y analiza la experiencia del cliente en cada punto de contacto para identificar momentos de friccion y oportunidades de mejora.",
        "Integra NPS, satisfaccion, facilidad de proceso, calidad de atencion y sentimiento. Correlaciona con churn para priorizar por impacto.",
        "Mejora NPS y reduce churn asociado a mala experiencia. Cada punto de NPS ganado incrementa recomendacion y reduce costo de adquisicion.",
    ],
    "date_col": "FECHA_ENCUESTA",
    "filter_cols": ["TOUCHPOINT", "CATEGORIA_NPS", "CIUDAD"],
    "kpi_query": """SELECT
        ROUND(AVG(NPS_SCORE), 1) AS NPS_PROM,
        ROUND(AVG(SATISFACCION_GENERAL), 1) AS SATISF_PROM,
        ROUND(AVG(FACILIDAD_PROCESO), 1) AS FACILIDAD,
        ROUND(AVG(CALIDAD_ATENCION), 1) AS CAL_ATENCION,
        ROUND(SUM(CASE WHEN RESOLUCION_PRIMER_CONTACTO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS FCR_PCT,
        ROUND(AVG(TIEMPO_RESPUESTA_HORAS), 1) AS T_RESP_H,
        ROUND(SUM(CASE WHEN RECOMENDARIA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS RECOMENDARIA_PCT,
        COUNT(*) AS ENCUESTAS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["NPS", "Satisfaccion", "Facilidad", "Cal. Atencion", "FCR", "T. Respuesta (h)", "Recomendaria", "Encuestas"],
    "kpi_formats": ["{:.1f}", "{:.1f}", "{:.1f}", "{:.1f}", "{:.1f}%", "{:.1f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_ENCUESTA) AS MES,
        ROUND(AVG(NPS_SCORE), 1) AS NPS,
        ROUND(AVG(SATISFACCION_GENERAL), 1) AS SATISFACCION
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["NPS", "Satisfaccion"],
    "treemap_query": """SELECT TOUCHPOINT, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Encuestas por touchpoint"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(NPS_SCORE), 1) AS NPS
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "NPS por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "NPS", "caption": "Tamano: encuestas | Color: NPS"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Experiencia")

# HEATMAP - FULL WIDTH
st.markdown("**Heatmap: Satisfaccion por Touchpoint y Sentimiento**")
hm_df = run_query(f"""
    SELECT TOUCHPOINT, SENTIMIENTO,
        ROUND(AVG(SATISFACCION_GENERAL), 2) AS SATISF
    FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="TOUCHPOINT", columns="SENTIMIENTO", values="SATISF", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#C0392B"], [0.3, "#E8963A"], [0.6, "#29B5E8"], [1, "#0F2B46"]],
        text=[[f"{v:.2f}" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=12, color="white"),
        hovertemplate="Touchpoint: %{y}<br>Sentimiento: %{x}<br>Satisfaccion: %{z:.2f}<extra></extra>",
        colorbar=dict(title="Satisfaccion"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=140, r=20, t=30, b=60),
                        xaxis=dict(title="Sentimiento"), yaxis=dict(title=""))
    st.plotly_chart(fig_hm, use_container_width=True)

# RADAR + DONUT NPS side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Radar: Dimensiones CX por Touchpoint (Top 5)**")
    radar_df = run_query(f"""
        SELECT TOUCHPOINT,
            ROUND(AVG(SATISFACCION_GENERAL)*20, 1) AS SATISF,
            ROUND(AVG(FACILIDAD_PROCESO)*20, 1) AS FACILIDAD,
            ROUND(AVG(CALIDAD_ATENCION)*20, 1) AS CALIDAD,
            ROUND(AVG(VELOCIDAD_RESPUESTA)*20, 1) AS VELOCIDAD,
            ROUND(AVG(CALIDAD_RED)*20, 1) AS RED
        FROM {TABLE}
        WHERE TOUCHPOINT IN (SELECT TOUCHPOINT FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5)
        GROUP BY 1
    """)
    if not radar_df.empty:
        categories = ["Satisfaccion", "Facilidad", "Calidad atencion", "Velocidad", "Calidad red"]
        fig_radar = go.Figure()
        for i, row in radar_df.iterrows():
            vals = [row["SATISF"], row["FACILIDAD"], row["CALIDAD"], row["VELOCIDAD"], row["RED"]]
            fig_radar.add_trace(go.Scatterpolar(
                r=vals + [vals[0]], theta=categories + [categories[0]],
                fill="toself", name=row["TOUCHPOINT"],
                line=dict(color=NAVY[i % len(NAVY)]),
                fillcolor=f"rgba({int(NAVY[i % len(NAVY)][1:3], 16)},{int(NAVY[i % len(NAVY)][3:5], 16)},{int(NAVY[i % len(NAVY)][5:7], 16)},0.12)",
            ))
        fig_radar.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=60, r=60, t=30, b=30),
                               polar=dict(radialaxis=dict(visible=True, range=[0, 100]), bgcolor="#FAFBFC"),
                               legend=dict(orientation="h", yanchor="bottom", y=-0.2, x=0.5, xanchor="center"),
                               showlegend=True)
        st.plotly_chart(fig_radar, use_container_width=True)

with col_b:
    st.markdown("**Donut: Distribucion NPS (Promotores / Neutros / Detractores)**")
    nps_df = run_query(f"""
        SELECT CATEGORIA_NPS, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not nps_df.empty:
        color_map = {"Promotor": "#2E7D8C", "Neutro": "#29B5E8", "Detractor": "#C0392B"}
        colors = [color_map.get(c, "#1B3A5C") for c in nps_df["CATEGORIA_NPS"]]
        fig_donut = go.Figure(go.Pie(
            labels=nps_df["CATEGORIA_NPS"].tolist(),
            values=nps_df["N"].tolist(),
            hole=0.55,
            marker=dict(colors=colors),
            textinfo="label+percent",
            textfont=dict(size=12),
            hovertemplate="%{label}<br>%{value:,.0f} encuestas<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

# SCATTER + FUNNEL side by side
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Scatter: NPS vs Prob. Churn por Servicio**")
    sc_df = run_query(f"""
        SELECT SERVICIO,
            ROUND(AVG(NPS_SCORE), 1) AS NPS,
            ROUND(AVG(PROB_CHURN_POST)*100, 1) AS CHURN,
            COUNT(*) AS ENCUESTAS,
            ROUND(AVG(SATISFACCION_GENERAL), 2) AS SATISF
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="NPS", y="CHURN", size="ENCUESTAS", color="SERVICIO",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"SATISF": ":.2f", "ENCUESTAS": ":,.0f"},
                           labels={"NPS": "NPS promedio", "CHURN": "Prob. churn post (%)"})
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_d:
    st.markdown("**Funnel: Contactos Necesarios para Resolucion**")
    fn_df = run_query(f"""
        SELECT
            CASE
                WHEN CONTACTOS_NECESARIOS = 1 THEN '1 contacto (FCR)'
                WHEN CONTACTOS_NECESARIOS = 2 THEN '2 contactos'
                WHEN CONTACTOS_NECESARIOS = 3 THEN '3 contactos'
                ELSE '4+ contactos'
            END AS NIVEL,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 1
    """)
    if not fn_df.empty:
        fig_fn = go.Figure(go.Funnel(
            y=fn_df["NIVEL"].tolist(),
            x=fn_df["N"].tolist(),
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=["#2E7D8C", "#29B5E8", "#E8963A", "#C0392B"][:len(fn_df)]),
            connector=dict(line=dict(color="#E2E8F0", width=1)),
        ))
        fig_fn.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_fn, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Box Plot: Distribucion de NPS por Touchpoint**")
box_df = run_query(f"""
    SELECT TOUCHPOINT, NPS_SCORE
    FROM {TABLE}
    WHERE TOUCHPOINT IN (SELECT TOUCHPOINT FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 8)
""")
if not box_df.empty:
    fig_box = px.box(box_df, x="TOUCHPOINT", y="NPS_SCORE", color="TOUCHPOINT",
                    color_discrete_sequence=NAVY,
                    labels={"NPS_SCORE": "NPS Score", "TOUCHPOINT": ""})
    fig_box.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=80),
                        showlegend=False, yaxis=dict(gridcolor="#E2E8F0"),
                        xaxis=dict(tickangle=-20))
    st.plotly_chart(fig_box, use_container_width=True)

# WATERFALL - FULL WIDTH
st.markdown("**Waterfall: Contribucion al NPS por Dimension**")
wf_df = run_query(f"""
    SELECT
        ROUND(AVG(CALIDAD_ATENCION)-3, 2) AS CALIDAD_ATENCION,
        ROUND(AVG(FACILIDAD_PROCESO)-3, 2) AS FACILIDAD,
        ROUND(AVG(VELOCIDAD_RESPUESTA)-3, 2) AS VELOCIDAD,
        ROUND(AVG(CALIDAD_RED)-3, 2) AS CALIDAD_RED,
        ROUND(AVG(SATISFACCION_GENERAL)-3, 2) AS SATISFACCION_NETA
    FROM {TABLE}
""")
if not wf_df.empty:
    r = wf_df.iloc[0]
    fig_wf = go.Figure(go.Waterfall(
        x=["Calidad atencion", "Facilidad proceso", "Velocidad respuesta", "Calidad red", "Satisfaccion neta"],
        y=[r["CALIDAD_ATENCION"], r["FACILIDAD"], r["VELOCIDAD"], r["CALIDAD_RED"], 0],
        measure=["relative", "relative", "relative", "relative", "total"],
        connector=dict(line=dict(color="#1B3A5C", width=1.5)),
        increasing=dict(marker=dict(color="#2E7D8C")),
        decreasing=dict(marker=dict(color="#C0392B")),
        totals=dict(marker=dict(color="#0F2B46")),
        textposition="outside",
        text=[f"{r['CALIDAD_ATENCION']:+.2f}", f"{r['FACILIDAD']:+.2f}", f"{r['VELOCIDAD']:+.2f}", f"{r['CALIDAD_RED']:+.2f}", f"{r['SATISFACCION_NETA']:+.2f}"],
    ))
    fig_wf.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=40, r=20, t=30, b=40),
                       yaxis=dict(title="Delta vs baseline (3.0)", gridcolor="#E2E8F0"), showlegend=False)
    st.plotly_chart(fig_wf, use_container_width=True)
