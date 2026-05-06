from app_pages.page_template import render_page

config = {
    "key": "ofac",
    "table": "MGG_TELCO.FACTURACION_Y_COBRANZA.OPTIMIZACION_FACTURACION",
    "icon": ":material/receipt_long:",
    "title": "Optimizacion Facturacion",
    "subtitle": "Deteccion de errores de facturacion, mejora de precision y reduccion de reclamos por cobro incorrecto.",
    "cards": [
        "Identifica errores de facturacion antes de que generen reclamos, mejorando la precision y reduciendo notas credito.",
        "Valida cada factura contra reglas de negocio, detecta sobrecobros, aplica prediccion de pago a tiempo y optimiza ciclos.",
        "Reduce reclamos por facturacion en 40%. Mejora flujo de caja con prediccion de pago y reduce costo de gestion por factura.",
    ],
    "date_col": "FECHA_FACTURA",
    "filter_cols": ["TIPO_ERROR_DETECTADO", "ESTADO_VALIDACION", "PLAN"],
    "kpi_query": """SELECT
        ROUND(AVG(PRECISION_FACTURACION)*100, 2) AS PRECISION_PCT,
        ROUND(SUM(CASE WHEN FACTURA_PAGADA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS PAGO_PCT,
        ROUND(AVG(TOTAL_FACTURA_COP)/1e3, 1) AS FACTURA_PROM_K,
        ROUND(AVG(PROBABILIDAD_PAGO_A_TIEMPO)*100, 1) AS PROB_PAGO_PCT,
        ROUND(SUM(CASE WHEN RECLAMO_GENERADO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS RECLAMOS_PCT,
        ROUND(AVG(DIAS_MORA), 1) AS MORA_PROM,
        ROUND(SUM(AJUSTE_NOTA_CREDITO_COP)/1e6, 1) AS NOTAS_CRED_M,
        COUNT(*) AS FACTURAS
    FROM {table} WHERE {where}""",
    "kpi_labels": ["Precision", "Pagadas", "Factura prom. (K)", "Prob. pago", "Reclamos", "Mora (dias)", "Notas credito (M)", "Facturas"],
    "kpi_formats": ["{:.2f}%", "{:.1f}%", "${:.1f}K", "{:.1f}%", "{:.1f}%", "{:.1f}", "${:.1f}M", "{:,.0f}"],
    "trend_query": """SELECT DATE_TRUNC('MONTH', FECHA_FACTURA) AS MES,
        ROUND(AVG(PRECISION_FACTURACION)*100, 2) AS PRECISION,
        ROUND(SUM(CASE WHEN RECLAMO_GENERADO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS RECLAMOS
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1""",
    "trend_cols": ["Precision (%)", "Reclamos (%)"],
    "treemap_query": """SELECT TIPO_ERROR_DETECTADO, COUNT(*) AS N
    FROM {table} WHERE {where} GROUP BY 1 ORDER BY 2 DESC""",
    "treemap_config": {"title": "Errores por tipo"},
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
st.subheader(":material/bar_chart: Analisis Avanzado de Facturacion")

# HEATMAP - FULL WIDTH
st.markdown("**Heatmap: Reclamos (%) por Plan y Tipo de Error**")
hm_df = run_query(f"""
    SELECT PLAN, TIPO_ERROR_DETECTADO,
        ROUND(SUM(CASE WHEN RECLAMO_GENERADO THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS RECLAMO_PCT
    FROM {TABLE}
    WHERE PLAN IN (SELECT PLAN FROM {TABLE} GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 8)
    GROUP BY 1, 2 ORDER BY 1, 2
""")
if not hm_df.empty:
    pivot = hm_df.pivot_table(index="PLAN", columns="TIPO_ERROR_DETECTADO", values="RECLAMO_PCT", aggfunc="mean").fillna(0)
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0, "#E8F4F8"], [0.3, "#29B5E8"], [0.6, "#E8963A"], [1, "#C0392B"]],
        text=[[f"{v:.1f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=10, color="white"),
        hovertemplate="Plan: %{y}<br>Error: %{x}<br>Reclamos: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Reclamos %"),
    ))
    fig_hm.update_layout(**CHART_LAYOUT, height=450, margin=dict(l=140, r=20, t=30, b=80),
                        xaxis=dict(title="Tipo de error", tickangle=-30), yaxis=dict(title=""))
    st.plotly_chart(fig_hm, use_container_width=True)

# SCATTER + DONUT side by side
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Scatter: Prob. Pago vs Dias Mora (color = Tipo Factura)**")
    sc_df = run_query(f"""
        SELECT TIPO_FACTURA,
            ROUND(AVG(PROBABILIDAD_PAGO_A_TIEMPO)*100, 1) AS PROB_PAGO,
            ROUND(AVG(DIAS_MORA), 1) AS MORA,
            ROUND(AVG(TOTAL_FACTURA_COP)/1e3, 1) AS FACTURA_K,
            COUNT(*) AS N
        FROM {TABLE} GROUP BY 1
    """)
    if not sc_df.empty:
        fig_sc = px.scatter(sc_df, x="MORA", y="PROB_PAGO", size="FACTURA_K", color="TIPO_FACTURA",
                           color_discrete_sequence=NAVY, size_max=50,
                           hover_data={"N": ":,.0f"},
                           labels={"MORA": "Dias mora prom.", "PROB_PAGO": "Prob. pago a tiempo (%)"})
        fig_sc.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"))
        st.plotly_chart(fig_sc, use_container_width=True)

with col_b:
    st.markdown("**Donut: Estado de Validacion**")
    donut_df = run_query(f"""
        SELECT ESTADO_VALIDACION, COUNT(*) AS N
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """)
    if not donut_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=donut_df["ESTADO_VALIDACION"].tolist(),
            values=donut_df["N"].tolist(),
            hole=0.5,
            marker=dict(colors=NAVY[:len(donut_df)]),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>%{value:,.0f} facturas<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=10, r=10, t=30, b=10),
                               showlegend=True, legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"))
        st.plotly_chart(fig_donut, use_container_width=True)

# BAR + HISTOGRAM side by side
col_c, col_d = st.columns(2)

with col_c:
    st.markdown("**Bar: Facturas Pagadas (%) por Canal de Pago**")
    bar_df = run_query(f"""
        SELECT CANAL_PAGO,
            COUNT(*) AS TOTAL,
            ROUND(SUM(CASE WHEN FACTURA_PAGADA THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0), 1) AS PAGADAS_PCT
        FROM {TABLE} GROUP BY 1 ORDER BY PAGADAS_PCT DESC
    """)
    if not bar_df.empty:
        fig_bar = go.Figure(go.Bar(
            x=bar_df["CANAL_PAGO"], y=bar_df["PAGADAS_PCT"],
            marker_color="#1B3A5C",
            text=[f"{v:.1f}%" for v in bar_df["PAGADAS_PCT"]],
            textposition="outside",
        ))
        fig_bar.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=60),
                            yaxis=dict(title="% Pagadas", gridcolor="#E2E8F0", range=[0, 100]))
        st.plotly_chart(fig_bar, use_container_width=True)

with col_d:
    st.markdown("**Histograma: Distribucion de Valor Total Factura**")
    hist_df = run_query(f"SELECT TOTAL_FACTURA_COP/1e3 AS FACTURA_K FROM {TABLE}")
    if not hist_df.empty:
        fig_hist = px.histogram(hist_df, x="FACTURA_K", nbins=30,
                               color_discrete_sequence=["#2E7D8C"],
                               labels={"FACTURA_K": "Total factura (K COP)"})
        fig_hist.update_layout(**CHART_LAYOUT, height=400, margin=dict(l=40, r=20, t=30, b=40),
                              yaxis=dict(title="Frecuencia", gridcolor="#E2E8F0"),
                              bargap=0.05, showlegend=False)
        st.plotly_chart(fig_hist, use_container_width=True)

# BOX PLOT - FULL WIDTH
st.markdown("**Box Plot: Dias Mora por Ciclo de Facturacion**")
box_df = run_query(f"SELECT CICLO_FACTURACION, DIAS_MORA FROM {TABLE}")
if not box_df.empty:
    fig_box = px.box(box_df, x="CICLO_FACTURACION", y="DIAS_MORA", color="CICLO_FACTURACION",
                    color_discrete_sequence=NAVY,
                    labels={"DIAS_MORA": "Dias mora", "CICLO_FACTURACION": ""})
    fig_box.update_layout(**CHART_LAYOUT, height=380, margin=dict(l=40, r=20, t=30, b=60),
                        showlegend=False, yaxis=dict(gridcolor="#E2E8F0"))
    st.plotly_chart(fig_box, use_container_width=True)
