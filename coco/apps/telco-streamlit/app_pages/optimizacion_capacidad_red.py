from app_pages.page_template import render_page

config = {
    "key": "ocr",
    "table": "MGG_TELCO.GESTION_DE_RED.OPTIMIZACION_CAPACIDAD_RED",
    "icon": ":material/speed:",
    "title": "Capacidad de Red",
    "subtitle": "Monitoreo de utilizacion de capacidad por celda, tecnologia y franja horaria para prevenir congestion.",
    "cards": [
        "Identifica celdas con congestion o cercanas a saturacion para planificar intervenciones antes de degradar la experiencia del usuario.",
        "Analiza utilizacion, trafico, throughput, tasa de congestion y llamadas caidas por celda y franja horaria. Prioriza por impacto en revenue.",
        "Previene degradacion de servicio por saturacion. Optimiza CAPEX al dirigir expansion solo donde hay necesidad real con ROI positivo.",
    ],
    "date_col": "FECHA_MEDICION",
    "filter_cols": ["TECNOLOGIA", "CIUDAD", "FRANJA_HORARIA"],
    "kpi_query": """SELECT
        ROUND(AVG(UTILIZACION_CAPACIDAD)*100, 1) AS UTIL_PCT,
        ROUND(AVG(TRAFICO_GB), 1) AS TRAFICO_PROM,
        ROUND(AVG(THROUGHPUT_DL_MBPS), 1) AS THROUGHPUT,
        ROUND(AVG(LATENCIA_MS), 1) AS LATENCIA,
        ROUND(AVG(TASA_CONGESTION)*100, 1) AS CONGESTION_PCT,
        ROUND(SUM(CASE WHEN CONGESTION_CRITICA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS CRITICAS_PCT,
        ROUND(AVG(SCORE_CALIDAD), 1) AS SCORE_CAL,
        COUNT(*) AS REGISTROS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Utilizacion", "Trafico (GB)", "Throughput DL", "Latencia (ms)", "Congestion", "Criticas", "Score calidad", "Registros"],
    "kpi_formats": ["{:.1f}%", "{:.1f}", "{:.1f} Mbps", "{:.1f}", "{:.1f}%", "{:.1f}%", "{:.1f}", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_MEDICION) AS MES,
        ROUND(AVG(UTILIZACION_CAPACIDAD)*100, 1) AS UTILIZACION,
        ROUND(AVG(SCORE_CALIDAD), 1) AS CALIDAD
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Utilizacion (%)", "Score calidad"],
    "treemap_query": """SELECT TECNOLOGIA, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Registros por tecnologia"},
    "geo_query": """SELECT CIUDAD, COUNT(*) AS VOLUMEN, ROUND(AVG(UTILIZACION_CAPACIDAD)*100, 1) AS UTIL
    FROM {table} WHERE {where} GROUP BY 1""",
    "geo_config": {"title": "Utilizacion por ciudad", "city_col": "CIUDAD", "size_col": "VOLUMEN", "color_col": "UTIL", "caption": "Tamano: celdas | Color: % utilizacion"},
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Congestion",
        "desc": "Estima probabilidad de congestion critica basado en parametros de red.",
        "features": [
            {"name": "Utilizacion capacidad (%)", "min": 0, "max": 100, "default": 60, "weight": 0.30, "desc": "Porcentaje de capacidad usada"},
            {"name": "Usuarios conectados", "min": 0, "max": 5000, "default": 800, "weight": 0.25, "desc": "Usuarios simultaneos en celda"},
            {"name": "Trafico (GB)", "min": 0, "max": 500, "default": 50, "weight": 0.20, "desc": "Trafico total en la celda"},
            {"name": "Crecimiento MoM (%)", "min": -10, "max": 50, "default": 8, "weight": 0.15, "desc": "Crecimiento mensual del trafico"},
            {"name": "Tasa llamadas caidas (%)", "min": 0, "max": 10, "default": 1, "step": 0.1, "weight": 0.10, "desc": "Porcentaje de llamadas caidas"},
        ],
        "thresholds": [0.35, 0.65],
        "labels": ["Sin congestion", "Riesgo moderado", "Congestion critica"],
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
st.subheader(":material/bar_chart: Analisis Avanzado de Capacidad")

st.markdown("**Heatmap: Congestion por Franja Horaria y Tecnologia**")
hm_df = run_query(f"""
    SELECT FRANJA_HORARIA, TECNOLOGIA,
        ROUND(AVG(TASA_CONGESTION)*100, 1) AS CONGESTION
    FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="FRANJA_HORARIA", columns="TECNOLOGIA", values="CONGESTION", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#E8F4F8"], [0.3, "#29B5E8"], [0.6, "#E8963A"], [1, "#C0392B"]],
        text=[[f"{v:.1f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=12, color="white"),
        hovertemplate="Franja: %{y}<br>Tecnologia: %{x}<br>Congestion: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Congestion %"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=520, margin=dict(l=60, r=20, t=30, b=60),
                        xaxis=dict(title="Tecnologia"), yaxis=dict(title="Franja horaria"))
    st.plotly_chart(fig_hm, use_container_width=True)

col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Scatter: Utilizacion vs Throughput (Tamano = Usuarios)**")
    sc_df = run_query(f"""
        SELECT TECNOLOGIA, ROUND(AVG(UTILIZACION_CAPACIDAD)*100, 1) AS UTIL,
            ROUND(AVG(THROUGHPUT_DL_MBPS), 1) AS THROUGHPUT,
            ROUND(AVG(USUARIOS_CONECTADOS), 0) AS USUARIOS,
            ROUND(AVG(LATENCIA_MS), 1) AS LATENCIA
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="UTIL", y="THROUGHPUT", size="USUARIOS", color="TECNOLOGIA",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"LATENCIA": ":.1f", "USUARIOS": ":,.0f"},
                           labels={"UTIL": "Utilizacion (%)", "THROUGHPUT": "Throughput DL (Mbps)"})
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_b:
    st.markdown("**Radar: Metricas por Tecnologia**")
    radar_df = run_query(f"""
        SELECT TECNOLOGIA,
            ROUND(AVG(UTILIZACION_CAPACIDAD)*100, 1) AS UTILIZACION,
            ROUND(AVG(THROUGHPUT_DL_MBPS)/10, 1) AS THROUGHPUT_NORM,
            ROUND(AVG(SCORE_CALIDAD)*10, 1) AS CALIDAD,
            ROUND((1 - AVG(TASA_CONGESTION))*100, 1) AS DISPONIBILIDAD,
            ROUND((1 - AVG(TASA_LLAMADAS_CAIDAS))*100, 1) AS ESTABILIDAD
        FROM {TABLE} GROUP BY 1
    """)
    if not radar_df.empty:
        categories = ["Utilizacion", "Throughput", "Calidad", "Disponibilidad", "Estabilidad"]
        fig_radar = go.Figure()
        for i, row in radar_df.iterrows():
            vals = [row["UTILIZACION"], row["THROUGHPUT_NORM"], row["CALIDAD"], row["DISPONIBILIDAD"], row["ESTABILIDAD"]]
            fig_radar.add_trace(go.Scatterpolar(
                r=vals + [vals[0]], theta=categories + [categories[0]],
                fill="toself", name=row["TECNOLOGIA"],
                line=dict(color=NAVY[i % len(NAVY)]),
                fillcolor=f"rgba({int(NAVY[i % len(NAVY)][1:3], 16)},{int(NAVY[i % len(NAVY)][3:5], 16)},{int(NAVY[i % len(NAVY)][5:7], 16)},0.15)",
            ))
        fig_radar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=60, r=60, t=30, b=30),
                               polar=dict(radialaxis=dict(visible=True, range=[0, 100]),
                                          bgcolor="#FAFBFC"),
                               legend=dict(orientation="h", yanchor="bottom", y=-0.15, x=0.5, xanchor="center"),
                               showlegend=True)
        st.plotly_chart(fig_radar, use_container_width=True)

col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Sunburst: Celdas por Tecnologia y Congestion**")
    sun_df = run_query(f"""
        SELECT TECNOLOGIA,
            CASE WHEN CONGESTION_CRITICA THEN 'Critica' ELSE 'Normal' END AS ESTADO,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1, 2
    """)
    if not sun_df.empty:
        fig_sun = px.sunburst(sun_df, path=["TECNOLOGIA", "ESTADO"], values="N",
                             color="N", color_continuous_scale=["#E8F4F8", "#2E7D8C", "#0F2B46"])
        fig_sun.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10))
        fig_sun.update_traces(textinfo="label+percent parent", insidetextorientation="radial")
        st.plotly_chart(fig_sun, use_container_width=True)

with col_d:
    st.markdown("**Waterfall: Componentes de Degradacion de Calidad**")
    wf_df = run_query(f"""
        SELECT
            ROUND(AVG(TASA_CONGESTION)*100, 1) AS CONGESTION,
            ROUND(AVG(TASA_LLAMADAS_CAIDAS)*100, 1) AS LLAMADAS_CAIDAS,
            ROUND(AVG(TASA_PAQUETES_PERDIDOS)*100, 1) AS PAQUETES_PERDIDOS,
            ROUND(AVG(LATENCIA_MS)/10, 1) AS LATENCIA_IMPACTO,
            ROUND((1-AVG(SCORE_CALIDAD))*100, 1) AS DEGRADACION_TOTAL
        FROM {TABLE}
    """)
    if not wf_df.empty:
        r = wf_df.iloc[0]
        fig_wf = go.Figure(go.Waterfall(
            x=["Congestion", "Llamadas caidas", "Paquetes perdidos", "Latencia", "Degradacion total"],
            y=[r["CONGESTION"], r["LLAMADAS_CAIDAS"], r["PAQUETES_PERDIDOS"], r["LATENCIA_IMPACTO"], 0],
            measure=["relative", "relative", "relative", "relative", "total"],
            connector=dict(line=dict(color="#1B3A5C", width=1.5)),
            increasing=dict(marker=dict(color="#E85D29")),
            totals=dict(marker=dict(color="#0F2B46")),
            textposition="outside",
            text=[f"{r['CONGESTION']:.1f}%", f"{r['LLAMADAS_CAIDAS']:.1f}%", f"{r['PAQUETES_PERDIDOS']:.1f}%", f"{r['LATENCIA_IMPACTO']:.1f}", f"{r['DEGRADACION_TOTAL']:.1f}%"],
        ))
        fig_wf.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           yaxis=dict(title="Impacto (%)", gridcolor="#E2E8F0"),
                           showlegend=False)
        st.plotly_chart(fig_wf, use_container_width=True)

st.markdown("**Box Plot: Distribucion de Throughput por Ciudad (Top 8)**")
box_df = run_query(f"""
    SELECT CIUDAD, THROUGHPUT_DL_MBPS
    FROM {TABLE}
    WHERE CIUDAD IN (SELECT CIUDAD FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 8)
""")
if not box_df.empty:
    fig_box = px.box(box_df, x="CIUDAD", y="THROUGHPUT_DL_MBPS", color="CIUDAD",
                    color_discrete_sequence=NAVY,
                    labels={"THROUGHPUT_DL_MBPS": "Throughput DL (Mbps)", "CIUDAD": ""})
    fig_box.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=40, r=20, t=30, b=60),
                        showlegend=False, yaxis=dict(gridcolor="#E2E8F0"))
    st.plotly_chart(fig_box, use_container_width=True)

st.markdown("**Histograma: Distribucion de Utilizacion de Capacidad**")
hist_df = run_query(f"SELECT UTILIZACION_CAPACIDAD FROM {TABLE}")
if not hist_df.empty:
    fig_hist = px.histogram(hist_df, x="UTILIZACION_CAPACIDAD", nbins=30,
                           color_discrete_sequence=["#1B3A5C"],
                           labels={"UTILIZACION_CAPACIDAD": "Utilizacion de capacidad"})
    fig_hist.add_vline(x=0.8, line_dash="dash", line_color="#E85D29", annotation_text="Umbral critico (80%)",
                      annotation_position="top right")
    fig_hist.update_layout(**CHART_LAYOUT, height=350, margin=dict(l=40, r=20, t=30, b=40),
                          yaxis=dict(title="Frecuencia", gridcolor="#E2E8F0"),
                          xaxis=dict(title="Utilizacion de capacidad", tickformat=".0%"),
                          bargap=0.05, showlegend=False)
    st.plotly_chart(fig_hist, use_container_width=True)

col_g, col_h = st.columns(2)

with col_g:
    st.markdown("**Funnel: Celdas por Nivel de Prioridad de Intervencion**")
    fn_df = run_query(f"""
        SELECT
            CASE
                WHEN PRIORIDAD_INTERVENCION >= 0.8 THEN '1. Critica (>=0.8)'
                WHEN PRIORIDAD_INTERVENCION >= 0.6 THEN '2. Alta (0.6-0.8)'
                WHEN PRIORIDAD_INTERVENCION >= 0.4 THEN '3. Media (0.4-0.6)'
                WHEN PRIORIDAD_INTERVENCION >= 0.2 THEN '4. Baja (0.2-0.4)'
                ELSE '5. Minima (<0.2)'
            END AS NIVEL, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 1
    """)
    if not fn_df.empty:
        fig_fn = go.Figure(go.Funnel(
            y=fn_df["NIVEL"].tolist(),
            x=fn_df["N"].tolist(),
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=["#C0392B", "#E85D29", "#E8963A", "#29B5E8", "#2E7D8C"]),
            connector=dict(line=dict(color="#E2E8F0", width=1)),
        ))
        fig_fn.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_fn, use_container_width=True)

with col_h:
    st.markdown("**Donut: Distribucion de Celdas por Estado**")
    donut_df = run_query(f"""
        SELECT
            CASE
                WHEN CONGESTION_CRITICA AND REQUIERE_EXPANSION THEN 'Critica + Expansion'
                WHEN CONGESTION_CRITICA THEN 'Congestion critica'
                WHEN REQUIERE_EXPANSION THEN 'Requiere expansion'
                ELSE 'Operacion normal'
            END AS ESTADO, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["ESTADO"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=["#C0392B", "#E85D29", "#E8963A", "#29B5E8"]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f} celdas<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)
