from app_pages.page_template import render_page

config = {
    "key": "dft",
    "table": "MGG_TELCO.FRAUDE_Y_SEGURIDAD.DETECCION_FRAUDE_TELCO",
    "icon": ":material/shield:",
    "title": "Deteccion Fraude",
    "subtitle": "Deteccion en tiempo real de fraudes telecom (SIM swap, bypass, subscription fraud) con correlacion de alertas.",
    "cards": [
        "Detecta y previene fraudes que generan perdidas millonarias: SIM swap, bypass de interconexion, fraude de suscripcion y roaming.",
        "Combina reglas, ML y correlacion de alertas para detectar patrones de fraude. Mide precision, falsos positivos y tiempo de respuesta.",
        "Evita perdidas por fraude de $2-5B anuales. Protege a clientes de robo de identidad y mejora confianza en el operador.",
    ],
    "date_col": "FECHA_DETECCION",
    "filter_cols": ["TIPO_FRAUDE", "ESTADO", "METODO_DETECCION"],
    "kpi_query": """SELECT
        ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE_PROM,
        ROUND(SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS CONF_PCT,
        ROUND(SUM(CASE WHEN FALSO_POSITIVO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS FP_PCT,
        ROUND(SUM(PERDIDA_ESTIMADA_COP)/1e9, 1) AS PERDIDA_B,
        ROUND(AVG(TIEMPO_DETECCION_MIN), 0) AS T_DETECCION,
        ROUND(AVG(TIEMPO_RESPUESTA_MIN), 0) AS T_RESPUESTA,
        ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS SLA_PCT,
        COUNT(*) AS CASOS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Score fraude", "Confirmados", "Falsos positivos", "Perdida (B)", "T. Deteccion (min)", "T. Respuesta (min)", "SLA", "Casos"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "{:.1f}%", "${:.1f}B", "{:.0f}", "{:.0f}", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_DETECCION) AS MES,
        COUNT(*) AS CASOS,
        ROUND(SUM(PERDIDA_ESTIMADA_COP)/1e6, 1) AS PERDIDA_M
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Casos", "Perdida ($M)"],
    "treemap_query": """SELECT TIPO_FRAUDE, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Casos por tipo de fraude"},
    "geo_query": None,
    "geo_config": None,
    "diagnostics": None,
    "simulator": {
        "title": "Simulador de Score de Fraude",
        "desc": "Estima la probabilidad de que un caso sea fraude confirmado.",
        "features": [
            {"name": "Score fraude (0-100)", "min": 0, "max": 100, "default": 50, "weight": 0.30, "desc": "Score del modelo de deteccion"},
            {"name": "Lineas involucradas", "min": 1, "max": 20, "default": 2, "weight": 0.20, "desc": "Cantidad de lineas en el caso"},
            {"name": "Alertas correlacionadas", "min": 0, "max": 15, "default": 3, "weight": 0.20, "desc": "Alertas asociadas"},
            {"name": "Perdida estimada (M COP)", "min": 0, "max": 100, "default": 5, "weight": 0.15, "desc": "Impacto economico estimado"},
            {"name": "Reincidencia (0=No, 1=Si)", "min": 0, "max": 1, "default": 0, "weight": 0.15, "desc": "Caso reincidente"},
        ],
        "thresholds": [0.35, 0.65],
        "labels": ["Bajo riesgo", "Riesgo moderado", "Fraude probable"],
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
st.subheader(":material/bar_chart: Analisis Avanzado de Fraude")

# HEATMAP - FULL WIDTH
st.markdown("**Heatmap: Score Fraude Promedio por Tipo y Estado**")
hm_df = run_query(f"""
    SELECT TIPO_FRAUDE, ESTADO,
        ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE
    FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="TIPO_FRAUDE", columns="ESTADO", values="SCORE", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#E8F4F8"], [0.3, "#29B5E8"], [0.6, "#E8963A"], [1, "#C0392B"]],
        text=[[f"{v:.1f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=11, color="white"),
        hovertemplate="Tipo: %{y}<br>Estado: %{x}<br>Score: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Score %"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=120, r=20, t=30, b=60),
                        xaxis=dict(title="Estado"), yaxis=dict(title=""))
    st.plotly_chart(fig_hm, use_container_width=True)

# SCATTER + DONUT side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Scatter: Score vs Perdida Estimada (Tamano = Lineas)**")
    sc_df = run_query(f"""
        SELECT TIPO_FRAUDE, ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE,
            ROUND(SUM(PERDIDA_ESTIMADA_COP)/1e6, 1) AS PERDIDA_M,
            SUM(LINEAS_INVOLUCRADAS) AS LINEAS,
            COUNT(*) AS CASOS
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="SCORE", y="PERDIDA_M", size="LINEAS", color="TIPO_FRAUDE",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"CASOS": ":,.0f", "LINEAS": ":,.0f"},
                           labels={"SCORE": "Score fraude (%)", "PERDIDA_M": "Perdida ($M)"})
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_b:
    st.markdown("**Donut: Casos por Metodo de Deteccion**")
    donut_df = run_query(f"""
        SELECT METODO_DETECCION, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["METODO_DETECCION"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f} casos<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

# FUNNEL + WATERFALL side by side
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Funnel: Casos por Estado del Proceso**")
    fn_df = run_query(f"""
        SELECT ESTADO, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY N DESC
    """)
    if not fn_df.empty:
        fig_fn = go.Figure(go.Funnel(
            y=fn_df["ESTADO"].tolist(),
            x=fn_df["N"].tolist(),
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=NAVY[:len(fn_df)]),
            connector=dict(line=dict(color="#E2E8F0", width=1)),
        ))
        fig_fn.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_fn, use_container_width=True)

with col_d:
    st.markdown("**Waterfall: Perdida vs Recuperacion por Tipo de Fraude**")
    wf_df = run_query(f"""
        SELECT TIPO_FRAUDE,
            ROUND(SUM(PERDIDA_ESTIMADA_COP)/1e6, 1) AS PERDIDA_M,
            ROUND(SUM(MONTO_RECUPERADO_COP)/1e6, 1) AS RECUPERADO_M
        FROM {TABLE} GROUP BY 1 ORDER BY PERDIDA_M DESC LIMIT 5
    """)
    if not wf_df.empty:
        labels = []
        values = []
        measures = []
        for _, row in wf_df.iterrows():
            labels.append(f"{row['TIPO_FRAUDE']} (perdida)")
            values.append(row["PERDIDA_M"])
            measures.append("relative")
            labels.append(f"{row['TIPO_FRAUDE']} (recup.)")
            values.append(-row["RECUPERADO_M"])
            measures.append("relative")
        labels.append("Perdida neta")
        values.append(0)
        measures.append("total")
        fig_wf = go.Figure(go.Waterfall(
            x=labels, y=values, measure=measures,
            connector=dict(line=dict(color="#1B3A5C", width=1.5)),
            increasing=dict(marker=dict(color="#C0392B")),
            decreasing=dict(marker=dict(color="#2E7D8C")),
            totals=dict(marker=dict(color="#0F2B46")),
            textposition="outside",
        ))
        fig_wf.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=40, r=20, t=30, b=100),
                           yaxis=dict(title="Millones COP", gridcolor="#E2E8F0"),
                           xaxis=dict(tickangle=-45), showlegend=False)
        st.plotly_chart(fig_wf, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Box Plot: Distribucion de Tiempo de Respuesta por Tipo de Fraude**")
box_df = run_query(f"""
    SELECT TIPO_FRAUDE, TIEMPO_RESPUESTA_MIN
    FROM {TABLE}
    WHERE TIPO_FRAUDE IN (SELECT TIPO_FRAUDE FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 6)
""")
if not box_df.empty:
    fig_box = px.box(box_df, x="TIPO_FRAUDE", y="TIEMPO_RESPUESTA_MIN", color="TIPO_FRAUDE",
                    color_discrete_sequence=NAVY,
                    labels={"TIEMPO_RESPUESTA_MIN": "Tiempo respuesta (min)", "TIPO_FRAUDE": ""})
    fig_box.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                        showlegend=False, yaxis=dict(gridcolor="#E2E8F0"))
    st.plotly_chart(fig_box, use_container_width=True)
