import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from app_pages.map_helper import colombia_scatter_map
from decimal import Decimal

TABLE = "MGG_PAGOS.ANALITICA_DE_PAGOS.ANALISIS_MCC"

COLORS = ["#29B5E8", "#11567F", "#71D4F0", "#0E3A53", "#A3E4F7", "#1B8BBF", "#5BC3E8", "#083248"]

COORDS = {
    "Bogota": (4.6097, -74.0817), "Medellin": (6.2442, -75.5812),
    "Cali": (3.4516, -76.5320), "Barranquilla": (10.9685, -74.7813),
    "Bucaramanga": (7.1254, -73.1198), "Cartagena": (10.3997, -75.5144),
    "Cucuta": (7.8939, -72.5078), "Ibague": (4.4389, -75.2322),
    "Manizales": (5.0689, -75.5174), "Pereira": (4.8133, -75.6961),
}

RISK_COLORS = {"Alto": "#DE350B", "Medio": "#FFAB00", "Bajo": "#36B37E"}


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filters():
    redes = run_query(f"SELECT DISTINCT RED FROM {TABLE} ORDER BY 1")["RED"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    niveles = run_query(f"SELECT DISTINCT NIVEL_RIESGO_MCC FROM {TABLE} ORDER BY 1")["NIVEL_RIESGO_MCC"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_PERIODO) AS FMIN, MAX(FECHA_PERIODO) AS FMAX FROM {TABLE}")
    return redes, ciudades, niveles, fechas


def build_where(fi, ff, red_s, ciudad_s, nivel_s, all_r, all_c, all_n):
    clauses = [f"FECHA_PERIODO BETWEEN '{fi}' AND '{ff}'"]
    if red_s and red_s != "Todos" and red_s in all_r:
        clauses.append(f"RED = '{red_s}'")
    if ciudad_s and ciudad_s != "Todos" and ciudad_s in all_c:
        clauses.append(f"CIUDAD = '{ciudad_s}'")
    if nivel_s and nivel_s != "Todos" and nivel_s in all_n:
        clauses.append(f"NIVEL_RIESGO_MCC = '{nivel_s}'")
    return " AND ".join(clauses)


def color_tag(value, good, bad, fmt="{:.1f}%", inverse=False):
    v = float(value)
    if not inverse:
        return f":green[**{fmt.format(v)}**]" if v >= good else f":orange[**{fmt.format(v)}**]" if v >= bad else f":red[**{fmt.format(v)}**]"
    else:
        return f":green[**{fmt.format(v)}**]" if v <= good else f":orange[**{fmt.format(v)}**]" if v <= bad else f":red[**{fmt.format(v)}**]"


st.header(":material/category: Analisis MCC")
st.caption("Analisis detallado por categoria comercial (MCC): volumen, ticket promedio, aprobacion, fraude, contracargos, interchange y adopcion digital.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Falta de entendimiento del portafolio por industria para identificar MCCs de alto riesgo y oportunidades de crecimiento por vertical.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Cada MCC se analiza con volumen, ticket promedio, tasa de aprobacion, fraude, contracargos y costo de interchange. Se compara con benchmarks y se identifica adopcion digital.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Identificar verticales mas rentables, detectar MCCs con fraude elevado para ajustar reglas y optimizar pricing por categoria.")

redes, ciudades, niveles, fechas_df = get_filters()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_w = f"FECHA_PERIODO BETWEEN '{fmin}' AND '{fmax}'"
dx_kpi = run_query(f"""
    SELECT COUNT(DISTINCT MCC) AS TOTAL_MCCS,
           SUM(VOLUMEN_COP) AS VOL_TOTAL,
           SUM(REVENUE_COP) AS REV_TOTAL,
           ROUND(AVG(TASA_APROBACION)*100,1) AS TASA_APROB,
           ROUND(AVG(TASA_FRAUDE)*100,3) AS TASA_FRAUDE,
           SUM(COMERCIOS_ACTIVOS) AS COMERCIOS,
           ROUND(AVG(PCT_ECOMMERCE)*100,1) AS ECOM_PCT,
           SUM(CASE WHEN MCC_RESTRINGIDO THEN 1 ELSE 0 END) AS RESTRINGIDOS,
           COUNT(*) AS TOTAL
    FROM {TABLE} WHERE {dx_w}
""")

if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
    d = dx_kpi.iloc[0]
    dx_tasa = float(d["TASA_APROB"])
    dx_fraude = float(d["TASA_FRAUDE"])
    dx_rev_b = float(d["REV_TOTAL"]) / 1e9
    dx_restringidos = int(d["RESTRINGIDOS"])

    dx_top_rev = run_query(f"""
        SELECT DESCRIPCION_MCC, SUM(REVENUE_COP) AS REV
        FROM {TABLE} WHERE {dx_w}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)
    dx_worst_fraud = run_query(f"""
        SELECT DESCRIPCION_MCC, ROUND(AVG(TASA_FRAUDE)*100,3) AS FRAUDE
        FROM {TABLE} WHERE {dx_w}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)

    dx_lines = []
    if dx_tasa >= 90:
        dx_lines.append(f"La tasa de aprobacion promedio del portafolio MCC es {color_tag(dx_tasa, 90, 85)}, dentro de :green[**parametros saludables**].")
    elif dx_tasa >= 85:
        dx_lines.append(f"La tasa de aprobacion promedio es {color_tag(dx_tasa, 90, 85)}, :orange[**ligeramente por debajo**] del objetivo.")
    else:
        dx_lines.append(f"La tasa de aprobacion promedio es {color_tag(dx_tasa, 90, 85)}, requiere :red[**atencion inmediata**].")
    dx_lines.append(f"- Revenue total del portafolio: **${dx_rev_b:.1f}B COP**")
    if not dx_top_rev.empty:
        dx_lines.append(f"- MCC lider en revenue: **{dx_top_rev.iloc[0]['DESCRIPCION_MCC']}**")
    if not dx_worst_fraud.empty:
        dx_lines.append(f"- MCC con mayor fraude: :red[**{dx_worst_fraud.iloc[0]['DESCRIPCION_MCC']}**] ({float(dx_worst_fraud.iloc[0]['FRAUDE']):.3f}%)")
    if dx_restringidos > 0:
        dx_lines.append(f"- MCCs restringidos activos: :orange[**{dx_restringidos}**]")

    st.markdown("")
    d1, d2 = st.columns(2)
    with d1:
        with st.container(border=True):
            st.markdown("**:material/monitoring: Diagnostico CEO**")
            st.markdown("\n".join(dx_lines))
    with d2:
        with st.container(border=True):
            st.markdown("**:material/recommend: Recomendaciones**")
            recs = []
            if dx_fraude > 0.1:
                recs.append("1. :red[**Revisar MCCs con alta tasa de fraude**] y ajustar reglas de prevencion por categoria.")
            if dx_restringidos > 0:
                recs.append(f"2. :orange[**Auditar {dx_restringidos} MCCs restringidos**] para evaluar si se requiere desactivacion o controles adicionales.")
            if not dx_top_rev.empty:
                recs.append(f"3. :green[**Potenciar MCC '{dx_top_rev.iloc[0]['DESCRIPCION_MCC']}'**] como vertical estrategica de crecimiento.")
            recs.append("4. :blue[**Impulsar adopcion digital**] (contactless + e-commerce) en MCCs con baja penetracion.")
            if not recs:
                recs.append(":green[**Portafolio MCC saludable.**] Mantener monitoreo continuo.")
            st.markdown("\n".join(recs))

st.divider()

fc1, fc2, fc3, fc4 = st.columns(4)
with fc1:
    fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="mcc_fecha")
    if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
        fecha_inicio, fecha_fin = fecha_rng
    else:
        fecha_inicio, fecha_fin = fmin, fmax
with fc2:
    red_sel = st.selectbox("Red", ["Todos"] + redes, key="mcc_red")
with fc3:
    ciudad_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="mcc_ciudad")
with fc4:
    nivel_sel = st.selectbox("Nivel riesgo", ["Todos"] + niveles, key="mcc_nivel")

WHERE = build_where(fecha_inicio, fecha_fin, red_sel, ciudad_sel, nivel_sel, redes, ciudades, niveles)

kpi_df = run_query(f"""
    SELECT COUNT(DISTINCT MCC) AS TOTAL_MCCS,
           SUM(VOLUMEN_COP) AS VOL_TOTAL,
           SUM(REVENUE_COP) AS REV_TOTAL,
           SUM(NUMERO_TRANSACCIONES) AS NUM_TXN,
           ROUND(AVG(TASA_APROBACION)*100,1) AS TASA_APROB,
           ROUND(AVG(TASA_FRAUDE)*100,3) AS TASA_FRAUDE,
           ROUND(AVG(TASA_CONTRACARGOS)*100,3) AS TASA_CB,
           SUM(COMERCIOS_ACTIVOS) AS COMERCIOS,
           ROUND(AVG(TICKET_PROMEDIO_COP),0) AS TICKET_PROM,
           ROUND(AVG(MDR_PROMEDIO_PCT)*100,2) AS MDR_PROM,
           ROUND(AVG(PCT_CONTACTLESS)*100,1) AS CONTACTLESS,
           ROUND(AVG(PCT_ECOMMERCE)*100,1) AS ECOMMERCE,
           ROUND(AVG(CRECIMIENTO_YOY_PCT)*100,1) AS CREC_YOY,
           SUM(CASE WHEN MCC_RESTRINGIDO THEN 1 ELSE 0 END) AS RESTRINGIDOS,
           COUNT(*) AS REGISTROS
    FROM {TABLE} WHERE {WHERE}
""")

if kpi_df.empty or kpi_df["REGISTROS"].iloc[0] == 0:
    st.info("No hay datos para los filtros seleccionados.")
else:
    r = kpi_df.iloc[0]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("MCCs analizados", f"{int(r['TOTAL_MCCS']):,}")
    k2.metric("Volumen total", f"${float(r['VOL_TOTAL'])/1e9:.1f}B COP")
    k3.metric("Revenue total", f"${float(r['REV_TOTAL'])/1e9:.1f}B COP")
    k4.metric("Transacciones", f"{int(r['NUM_TXN']):,}")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Tasa aprobacion", f"{float(r['TASA_APROB']):.1f}%")
    k6.metric("Tasa fraude", f"{float(r['TASA_FRAUDE']):.3f}%")
    k7.metric("Comercios activos", f"{int(r['COMERCIOS']):,}")
    k8.metric("Crecimiento YoY", f"{float(r['CREC_YOY']):+.1f}%")

    st.divider()

    st.markdown("**Landscape MCC — Volumen vs Aprobacion**")
    bubble_df = run_query(f"""
        SELECT DESCRIPCION_MCC, NIVEL_RIESGO_MCC,
               SUM(VOLUMEN_COP)/1e6 AS VOL_MM,
               ROUND(AVG(TASA_APROBACION)*100,1) AS TASA,
               SUM(COMERCIOS_ACTIVOS) AS COMERCIOS,
               SUM(REVENUE_COP)/1e6 AS REV_MM
        FROM {TABLE} WHERE {WHERE}
        GROUP BY 1,2 ORDER BY 3 DESC LIMIT 30
    """)
    if not bubble_df.empty:
        fig_bubble = go.Figure()
        for nivel in bubble_df["NIVEL_RIESGO_MCC"].unique():
            sub = bubble_df[bubble_df["NIVEL_RIESGO_MCC"] == nivel]
            fig_bubble.add_trace(go.Scatter(
                x=[float(v) for v in sub["VOL_MM"]],
                y=[float(v) for v in sub["TASA"]],
                mode="markers",
                name=str(nivel),
                marker=dict(
                    size=[max(8, min(60, float(v)/50)) for v in sub["COMERCIOS"]],
                    color=RISK_COLORS.get(str(nivel), COLORS[0]),
                    opacity=0.7,
                    line=dict(width=1, color="white"),
                ),
                text=sub["DESCRIPCION_MCC"].tolist(),
                customdata=[[float(row["REV_MM"]), int(row["COMERCIOS"])] for _, row in sub.iterrows()],
                hovertemplate="<b>%{text}</b><br>Volumen: $%{x:,.0f}M COP<br>Aprobacion: %{y:.1f}%<br>Revenue: $%{customdata[0]:,.0f}M<br>Comercios: %{customdata[1]:,}<extra></extra>",
            ))
        fig_bubble.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=450,
            margin=dict(l=50, r=20, t=30, b=50),
            xaxis=dict(title="Volumen (Millones COP)"),
            yaxis=dict(title="Tasa de Aprobacion (%)"),
            legend=dict(title="Nivel de Riesgo", orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_bubble, use_container_width=True, key="mcc_bubble")

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Matriz de Riesgo — Nivel vs Tasa de Fraude**")
        risk_df = run_query(f"""
            SELECT NIVEL_RIESGO_MCC,
                   CASE WHEN TASA_FRAUDE*100 < 0.05 THEN '<0.05%'
                        WHEN TASA_FRAUDE*100 < 0.1 THEN '0.05-0.1%'
                        WHEN TASA_FRAUDE*100 < 0.2 THEN '0.1-0.2%'
                        ELSE '>0.2%' END AS BIN_FRAUDE,
                   COUNT(*) AS CNT
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1,2
        """)
        if not risk_df.empty:
            piv = risk_df.pivot_table(index="NIVEL_RIESGO_MCC", columns="BIN_FRAUDE", values="CNT", fill_value=0)
            col_order = ["<0.05%", "0.05-0.1%", "0.1-0.2%", ">0.2%"]
            piv = piv.reindex(columns=[c for c in col_order if c in piv.columns], fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=piv.values.tolist(),
                x=piv.columns.tolist(),
                y=piv.index.tolist(),
                colorscale=[[0, "#E8F5E9"], [0.5, "#FFF3E0"], [1, "#FFCDD2"]],
                text=piv.values.tolist(),
                texttemplate="%{text:,}",
                textfont=dict(size=13),
                hovertemplate="Riesgo: %{y}<br>Fraude: %{x}<br>Registros: %{z:,}<extra></extra>",
            ))
            fig_heat.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=380,
                margin=dict(l=80, r=20, t=30, b=50),
                xaxis=dict(title="Rango Tasa Fraude"), yaxis=dict(title="Nivel Riesgo MCC"),
            )
            st.plotly_chart(fig_heat, use_container_width=True, key="mcc_risk_matrix")

    with col_b:
        st.markdown("**Pareto de Revenue por MCC — 80/20**")
        pareto_df = run_query(f"""
            SELECT DESCRIPCION_MCC, SUM(REVENUE_COP)/1e6 AS REV_MM
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """)
        if not pareto_df.empty:
            rev_vals = [float(v) for v in pareto_df["REV_MM"]]
            total_rev = sum(rev_vals)
            cum_pct = []
            running = 0
            for v in rev_vals:
                running += v
                cum_pct.append(round(running / total_rev * 100, 1) if total_rev > 0 else 0)
            fig_pareto = go.Figure()
            fig_pareto.add_trace(go.Bar(
                x=pareto_df["DESCRIPCION_MCC"].tolist(),
                y=rev_vals,
                name="Revenue (M COP)",
                marker=dict(color=COLORS[0]),
                hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.0f}M COP<extra></extra>",
            ))
            fig_pareto.add_trace(go.Scatter(
                x=pareto_df["DESCRIPCION_MCC"].tolist(),
                y=cum_pct,
                name="% Acumulado",
                yaxis="y2",
                mode="lines+markers",
                line=dict(color="#DE350B", width=2),
                marker=dict(size=6),
                hovertemplate="%{y:.1f}% acumulado<extra></extra>",
            ))
            fig_pareto.add_hline(y=80, yref="y2", line_dash="dash", line_color="#FFAB00", annotation_text="80%")
            fig_pareto.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=380,
                margin=dict(l=50, r=50, t=30, b=100),
                xaxis=dict(tickangle=-45, tickfont=dict(size=9)),
                yaxis=dict(title="Revenue (M COP)"),
                yaxis2=dict(title="% Acumulado", overlaying="y", side="right", range=[0, 105]),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                barmode="group",
            )
            st.plotly_chart(fig_pareto, use_container_width=True, key="mcc_pareto")

    st.divider()

    st.markdown("**Volumen COP por ciudad**")
    map_df = run_query(f"""
        SELECT CIUDAD, SUM(VOLUMEN_COP) AS VOL, SUM(REVENUE_COP)/1e6 AS REV_MM,
               ROUND(AVG(TASA_APROBACION)*100,1) AS TASA,
               SUM(NUMERO_TRANSACCIONES) AS TXN
        FROM {TABLE} WHERE {WHERE}
        GROUP BY 1 ORDER BY 2 DESC
    """)
    if not map_df.empty:
        map_df["LAT"] = map_df["CIUDAD"].map(lambda c: COORDS.get(str(c), (4.5, -74.0))[0]).astype(float)
        map_df["LON"] = map_df["CIUDAD"].map(lambda c: COORDS.get(str(c), (4.5, -74.0))[1]).astype(float)
        map_df["VOL_M"] = map_df["VOL"].astype(float) / 1e6
        map_df["hover"] = map_df.apply(lambda r: f"{r['CIUDAD']}<br>Volumen: ${r['VOL']/1e6:,.0f}M COP<br>Revenue: ${r['REV_MM']:,.0f}M<br>Tasa: {r['TASA']}%<br>Txns: {int(r['TXN']):,}", axis=1)
        deck = colombia_scatter_map(
            map_df, lat_col="LAT", lon_col="LON", size_col="VOL_M", color_col="VOL_M",
            text_col="CIUDAD", hover_col="hover",
            colorscale=["#A3E4F7", "#29B5E8", "#11567F"],
            colorbar_title="Volumen (M COP)",
        )
        st.pydeck_chart(deck, key="mcc_map")

    st.divider()

    col_c, col_d = st.columns(2)

    with col_c:
        st.markdown("**Adopcion Digital por MCC — Contactless vs E-commerce**")
        digital_df = run_query(f"""
            SELECT DESCRIPCION_MCC,
                   ROUND(AVG(PCT_CONTACTLESS)*100,1) AS CONTACTLESS,
                   ROUND(AVG(PCT_ECOMMERCE)*100,1) AS ECOMMERCE
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY CONTACTLESS+ECOMMERCE DESC LIMIT 12
        """)
        if not digital_df.empty:
            fig_digital = go.Figure()
            fig_digital.add_trace(go.Bar(
                y=digital_df["DESCRIPCION_MCC"].tolist(),
                x=[float(v) for v in digital_df["CONTACTLESS"]],
                name="Contactless", orientation="h",
                marker=dict(color=COLORS[0]),
                hovertemplate="%{y}: %{x:.1f}%<extra></extra>",
            ))
            fig_digital.add_trace(go.Bar(
                y=digital_df["DESCRIPCION_MCC"].tolist(),
                x=[float(v) for v in digital_df["ECOMMERCE"]],
                name="E-commerce", orientation="h",
                marker=dict(color=COLORS[1]),
                hovertemplate="%{y}: %{x:.1f}%<extra></extra>",
            ))
            fig_digital.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=420,
                margin=dict(l=180, r=20, t=30, b=40),
                barmode="group", xaxis=dict(title="% Adopcion"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_digital, use_container_width=True, key="mcc_digital")

    with col_d:
        st.markdown("**Cuadrante Crecimiento vs Riesgo**")
        quad_df = run_query(f"""
            SELECT DESCRIPCION_MCC,
                   ROUND(AVG(CRECIMIENTO_YOY_PCT)*100,1) AS CREC,
                   ROUND(AVG(TASA_FRAUDE)*100,3) AS FRAUDE,
                   SUM(VOLUMEN_COP)/1e6 AS VOL_MM,
                   NIVEL_RIESGO_MCC
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1,5 ORDER BY 4 DESC LIMIT 20
        """)
        if not quad_df.empty:
            med_crec = float(quad_df["CREC"].median())
            med_fraude = float(quad_df["FRAUDE"].median())
            fig_quad = go.Figure()
            for nivel in quad_df["NIVEL_RIESGO_MCC"].unique():
                sub = quad_df[quad_df["NIVEL_RIESGO_MCC"] == nivel]
                fig_quad.add_trace(go.Scatter(
                    x=[float(v) for v in sub["CREC"]],
                    y=[float(v) for v in sub["FRAUDE"]],
                    mode="markers+text",
                    name=str(nivel),
                    marker=dict(
                        size=[max(8, min(40, float(v)/100)) for v in sub["VOL_MM"]],
                        color=RISK_COLORS.get(str(nivel), COLORS[0]),
                        opacity=0.7, line=dict(width=1, color="white"),
                    ),
                    text=sub["DESCRIPCION_MCC"].tolist(),
                    textposition="top center", textfont=dict(size=8),
                    hovertemplate="<b>%{text}</b><br>Crecimiento: %{x:.1f}%<br>Fraude: %{y:.3f}%<extra></extra>",
                ))
            fig_quad.add_hline(y=med_fraude, line_dash="dash", line_color="#999", line_width=1)
            fig_quad.add_vline(x=med_crec, line_dash="dash", line_color="#999", line_width=1)
            fig_quad.add_annotation(x=max([float(v) for v in quad_df["CREC"]]), y=min([float(v) for v in quad_df["FRAUDE"]]),
                text="Alto crecimiento<br>Bajo riesgo", showarrow=False, font=dict(size=9, color="#36B37E"), xanchor="right")
            fig_quad.add_annotation(x=max([float(v) for v in quad_df["CREC"]]), y=max([float(v) for v in quad_df["FRAUDE"]]),
                text="Alto crecimiento<br>Alto riesgo", showarrow=False, font=dict(size=9, color="#DE350B"), xanchor="right")
            fig_quad.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=420,
                margin=dict(l=50, r=20, t=30, b=50),
                xaxis=dict(title="Crecimiento YoY (%)"), yaxis=dict(title="Tasa de Fraude (%)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_quad, use_container_width=True, key="mcc_quadrant")

    st.divider()

    st.markdown("**Indicador de MCCs Restringidos**")
    restr_df = run_query(f"""
        SELECT
            SUM(CASE WHEN MCC_RESTRINGIDO THEN 1 ELSE 0 END) AS RESTRINGIDOS,
            COUNT(DISTINCT MCC) AS TOTAL_MCCS,
            ROUND(SUM(CASE WHEN MCC_RESTRINGIDO THEN VOLUMEN_COP ELSE 0 END)/1e6,0) AS VOL_RESTR_MM,
            ROUND(SUM(CASE WHEN MCC_RESTRINGIDO THEN REVENUE_COP ELSE 0 END)/1e6,0) AS REV_RESTR_MM
        FROM {TABLE} WHERE {WHERE}
    """)
    if not restr_df.empty:
        rr = restr_df.iloc[0]
        total_mccs = int(rr["TOTAL_MCCS"]) if int(rr["TOTAL_MCCS"]) > 0 else 1
        pct_restr = round(int(rr["RESTRINGIDOS"]) / total_mccs * 100, 1)
        ri1, ri2, ri3, ri4 = st.columns(4)
        ri1.metric("MCCs restringidos", f"{int(rr['RESTRINGIDOS'])}")
        ri2.metric("% del total", f"{pct_restr:.1f}%")
        ri3.metric("Volumen restringido", f"${float(rr['VOL_RESTR_MM']):,.0f}M COP")
        ri4.metric("Revenue restringido", f"${float(rr['REV_RESTR_MM']):,.0f}M COP")
        if pct_restr > 10:
            st.error(f"Hay un :red[**{pct_restr:.1f}%**] de MCCs restringidos. Revisar politicas de aceptacion.")
        elif pct_restr > 5:
            st.warning(f"El :orange[**{pct_restr:.1f}%**] de MCCs estan restringidos. Monitorear de cerca.")
        else:
            st.success(f"Solo el :green[**{pct_restr:.1f}%**] de MCCs estan restringidos. Portafolio saludable.")
