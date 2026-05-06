from app_pages.page_template import render_page

config = {
    "key": "gcb",
    "table": "MGG_TELCO.FACTURACION_Y_COBRANZA.GESTION_COBRANZA",
    "icon": ":material/payments:",
    "title": "Gestion Cobranza",
    "subtitle": "Optimizacion de estrategias de cobranza con priorizacion inteligente y medicion de recuperacion.",
    "cards": [
        "Maximiza la recuperacion de cartera morosa asignando la estrategia y canal optimos segun perfil de cada deudor.",
        "Motor de decisiones que evalua score de contactabilidad, probabilidad de recuperacion, mejor horario y costo-beneficio.",
        "Incrementa tasa de recuperacion 20-30%. Reduce costo por peso recuperado y mejora reactivacion post-pago.",
    ],
    "date_col": "FECHA_GESTION",
    "filter_cols": ["ESTRATEGIA", "CANAL", "RESULTADO"],
    "kpi_query": """SELECT
        ROUND(AVG(TASA_RECUPERACION)*100, 1) AS RECUP_PCT,
        ROUND(SUM(CASE WHEN PAGO_OBTENIDO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS PAGO_PCT,
        ROUND(AVG(SALDO_PENDIENTE_COP)/1e3, 1) AS SALDO_K,
        ROUND(AVG(DIAS_MORA), 0) AS MORA_DIAS,
        ROUND(AVG(PROB_RECUPERACION)*100, 1) AS PROB_REC,
        ROUND(SUM(MONTO_RECUPERADO_COP)/1e9, 1) AS RECUP_B,
        ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS SLA_PCT,
        COUNT(*) AS GESTIONES
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Tasa recuperacion", "Pago obtenido", "Saldo pend. (K)", "Mora (dias)", "Prob. recuperacion", "Recuperado (B)", "SLA", "Gestiones"],
    "kpi_formats": ["{:.1f}%", "{:.1f}%", "${:.1f}K", "{:.0f}", "{:.1f}%", "${:.1f}B", "{:.1f}%", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_GESTION) AS MES,
        ROUND(AVG(TASA_RECUPERACION)*100, 1) AS RECUPERACION,
        ROUND(SUM(CASE WHEN PAGO_OBTENIDO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS PAGO
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Tasa recuperacion (%)", "Pago obtenido (%)"],
    "treemap_query": """SELECT ESTRATEGIA, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Gestiones por estrategia"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Cobranza")

# HEATMAP - FULL WIDTH
st.markdown("**Heatmap: Tasa Recuperacion por Estrategia y Canal**")
hm_df = run_query(f"""
    SELECT ESTRATEGIA, CANAL,
        ROUND(AVG(TASA_RECUPERACION)*100, 1) AS RECUP
    FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="ESTRATEGIA", columns="CANAL", values="RECUP", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#C0392B"], [0.4, "#E8963A"], [0.7, "#29B5E8"], [1, "#2E7D8C"]],
        text=[[f"{v:.1f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=11, color="white"),
        hovertemplate="Estrategia: %{y}<br>Canal: %{x}<br>Recuperacion: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Recuperacion %"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=140, r=20, t=30, b=60),
                        xaxis=dict(title="Canal"), yaxis=dict(title=""))
    st.plotly_chart(fig_hm, use_container_width=True)

# SCATTER + FUNNEL side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Scatter: Dias Mora vs Probabilidad Recuperacion**")
    sc_df = run_query(f"""
        SELECT RESULTADO,
            ROUND(AVG(DIAS_MORA), 0) AS MORA,
            ROUND(AVG(PROB_RECUPERACION)*100, 1) AS PROB,
            ROUND(AVG(SALDO_PENDIENTE_COP)/1e3, 1) AS SALDO_K,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="MORA", y="PROB", size="SALDO_K", color="RESULTADO",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"N": ":,.0f", "SALDO_K": ":,.0f"},
                           labels={"MORA": "Dias mora prom.", "PROB": "Prob. recuperacion (%)"})
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_b:
    st.markdown("**Funnel: Gestiones por Resultado**")
    fn_df = run_query(f"""
        SELECT RESULTADO, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY N DESC
    """)
    if not fn_df.empty:
        fig_fn = go.Figure(go.Funnel(
            y=fn_df["RESULTADO"].tolist(),
            x=fn_df["N"].tolist(),
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=NAVY[:len(fn_df)]),
            connector=dict(line=dict(color="#E2E8F0", width=1)),
        ))
        fig_fn.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_fn, use_container_width=True)

# DONUT + BAR side by side
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Donut: Gestiones por Canal**")
    donut_df = run_query(f"""
        SELECT CANAL, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["CANAL"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f} gestiones<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

with col_d:
    st.markdown("**Bar: Monto Recuperado por Estrategia**")
    bar_df = run_query(f"""
        SELECT ESTRATEGIA, ROUND(SUM(MONTO_RECUPERADO_COP)/1e9, 2) AS RECUP_B
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not bar_df.empty:
        fig_bar = go.Figure(go.Bar(
            x=bar_df["ESTRATEGIA"], y=bar_df["RECUP_B"],
            marker_color="#1B3A5C",
            text=[f"${v:.2f}B" for v in bar_df["RECUP_B"]],
            textposition="outside",
        ))
        fig_bar.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=40, r=20, t=30, b=60),
                            yaxis=dict(title="Recuperado (B COP)", gridcolor="#E2E8F0"))
        st.plotly_chart(fig_bar, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Box Plot: Distribucion de Saldo Pendiente por Estrategia**")
box_df = run_query(f"""
    SELECT ESTRATEGIA, SALDO_PENDIENTE_COP/1e3 AS SALDO_K
    FROM {TABLE}
""")
if not box_df.empty:
    fig_box = px.box(box_df, x="ESTRATEGIA", y="SALDO_K", color="ESTRATEGIA",
                    color_discrete_sequence=NAVY,
                    labels={"SALDO_K": "Saldo pendiente (K COP)", "ESTRATEGIA": ""})
    fig_box.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                        showlegend=False, yaxis=dict(gridcolor="#E2E8F0"))
    st.plotly_chart(fig_box, use_container_width=True)
