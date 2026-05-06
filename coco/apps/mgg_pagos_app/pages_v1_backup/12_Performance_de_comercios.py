import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.COMERCIOS_Y_ADQUIRENCIA.PERFORMANCE_COMERCIOS"

COLORS = ["#29B5E8", "#FF8B00", "#36B37E", "#6554C0", "#DE350B", "#11567F", "#FFAB00", "#00A3BF"]

COORDS_CIUDAD = {
    "Bogota": (4.6097, -74.0817), "Medellin": (6.2442, -75.5812),
    "Cali": (3.4516, -76.5320), "Barranquilla": (10.9685, -74.7813),
    "Bucaramanga": (7.1254, -73.1198), "Cartagena": (10.3997, -75.5144),
    "Cucuta": (7.8939, -72.5078), "Ibague": (4.4389, -75.2322),
    "Pereira": (4.8133, -75.6961), "Manizales": (5.0689, -75.5174),
    "Villavicencio": (4.1420, -73.6266), "Santa Marta": (11.2404, -74.1990),
}


def _is_sis():
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session()
        return True
    except Exception:
        return False


@st.cache_resource
def _get_connector():
    import snowflake.connector
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "mariogalvisg"
    )


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    if _is_sis():
        from snowflake.snowpark.context import get_active_session
        df = get_active_session().sql(sql).to_pandas()
    else:
        conn = _get_connector()
        df = pd.read_sql(sql, conn)
    df.columns = [str(c).upper() for c in df.columns]
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass
    return df


@st.cache_data(ttl=300, show_spinner=False)
def get_filter_options():
    segmentos = run_query(f"SELECT DISTINCT SEGMENTO FROM {TABLE} ORDER BY 1")["SEGMENTO"].tolist()
    mccs = run_query(f"SELECT DISTINCT MCC FROM {TABLE} ORDER BY 1")["MCC"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    niveles = run_query(f"SELECT DISTINCT NIVEL_RIESGO FROM {TABLE} ORDER BY 1")["NIVEL_RIESGO"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_CORTE) AS FMIN, MAX(FECHA_CORTE) AS FMAX FROM {TABLE}")
    return segmentos, mccs, ciudades, niveles, fechas


def build_where(fecha_ini, fecha_f, seg_s, mcc_s, ciu_s, niv_s,
                all_seg, all_mcc, all_ciu, all_niv):
    clauses = [f"FECHA_CORTE BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if seg_s and seg_s != "Todos" and seg_s in all_seg:
        clauses.append(f"SEGMENTO = '{seg_s}'")
    if mcc_s and mcc_s != "Todos" and mcc_s in all_mcc:
        clauses.append(f"MCC = '{mcc_s}'")
    if ciu_s and ciu_s != "Todos" and ciu_s in all_ciu:
        clauses.append(f"CIUDAD = '{ciu_s}'")
    if niv_s and niv_s != "Todos":
        clauses.append(f"NIVEL_RIESGO = {niv_s}")
    return " AND ".join(clauses)


def color_tag(value, good_threshold, bad_threshold, fmt="{:.1f}%", inverse=False):
    v = float(value)
    if not inverse:
        if v >= good_threshold:
            return f":green[**{fmt.format(v)}**]"
        elif v >= bad_threshold:
            return f":orange[**{fmt.format(v)}**]"
        else:
            return f":red[**{fmt.format(v)}**]"
    else:
        if v <= good_threshold:
            return f":green[**{fmt.format(v)}**]"
        elif v <= bad_threshold:
            return f":orange[**{fmt.format(v)}**]"
        else:
            return f":red[**{fmt.format(v)}**]"


st.header(":material/trending_up: Performance de comercios")
st.caption("Metricas de desempeno por comercio. Volumen, tasa aprobacion, ticket promedio, fraude, contracargos y crecimiento.")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Imposibilidad de identificar comercios de alto riesgo, detectar churners y priorizar acciones comerciales.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Metricas periodicas: volumen, tasa aprobacion, ticket promedio, fraude, contracargos, crecimiento vs periodo anterior.")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Retener comercios clave, reducir perdidas en alto riesgo y crecer la base activa con acciones dirigidas.")

    segmentos, mccs, ciudades, niveles, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_CORTE BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(TRANSACCIONES_PERIODO) AS TXN_SUM,
            ROUND(SUM(VOLUMEN_BRUTO_COP)/1e9, 2) AS VOL_B,
            ROUND(AVG(TICKET_PROMEDIO_COP), 0) AS TICKET_PROM,
            ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA_APROB_PROM,
            ROUND(SUM(CASE WHEN RIESGO_CHURN THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_CHURN,
            ROUND(SUM(CASE WHEN EN_MONITOREO_FRAUDE THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_FRAUDE_MON,
            ROUND(AVG(SCORE_SATISFACCION), 1) AS SCORE_SAT
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL"])
        dx_tasa_aprob = float(dx["TASA_APROB_PROM"])
        dx_pct_churn = float(dx["PCT_CHURN"])
        dx_pct_fraude = float(dx["PCT_FRAUDE_MON"])
        dx_score_sat = float(dx["SCORE_SAT"])

        dx_worst_seg = run_query(f"""
            SELECT SEGMENTO, ROUND(AVG(TASA_FRAUDE)*100, 2) AS TASA_FRAUDE
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_top_ciudad = run_query(f"""
            SELECT CIUDAD, ROUND(SUM(VOLUMEN_BRUTO_COP)/1e9, 2) AS VOL_B
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)

        if dx_tasa_aprob >= 90:
            dx_estado = f"Tasa de aprobacion promedio ({color_tag(dx_tasa_aprob, 90, 80)}) :green[**saludable**]. {dx_total:,} registros de performance."
        elif dx_tasa_aprob >= 80:
            dx_estado = f"Tasa de aprobacion ({color_tag(dx_tasa_aprob, 90, 80)}) en :orange[**zona de atencion**]. Revisar segmentos criticos."
        else:
            dx_estado = f"Tasa de aprobacion ({color_tag(dx_tasa_aprob, 90, 80)}) :red[**critica**]. Accion inmediata requerida."

        dx_lines = [dx_estado]
        if not dx_worst_seg.empty:
            ws = dx_worst_seg.iloc[0]
            dx_lines.append(f"- Segmento mayor fraude: **{ws['SEGMENTO']}** ({color_tag(float(ws['TASA_FRAUDE']), 0.5, 1.5, fmt='{:.2f}%', inverse=True)})")
        if not dx_top_ciudad.empty:
            tc = dx_top_ciudad.iloc[0]
            dx_lines.append(f"- Ciudad mayor volumen: **{tc['CIUDAD']}** (${float(tc['VOL_B']):.2f}B COP)")
        dx_lines.append(f"- Riesgo churn: **{dx_pct_churn:.1f}%** — {':green[**bajo**]' if dx_pct_churn < 15 else ':red[**alerta de retencion**]'}")
        dx_lines.append(f"- Score satisfaccion: **{dx_score_sat:.1f}/10** {'— :green[**alto**]' if dx_score_sat > 7 else '— :orange[**mejorar experiencia**]'}")

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
                if dx_tasa_aprob < 85:
                    recs.append("1. :red[**Mejorar tasa de aprobacion**]: negociar con emisores y revisar reglas de fraude.")
                if dx_pct_churn > 20:
                    recs.append("2. :red[**Plan de retencion urgente**]: alto porcentaje de comercios en riesgo de abandono.")
                if dx_pct_fraude > 15:
                    recs.append("3. :orange[**Reforzar monitoreo de fraude**]: porcentaje elevado de comercios bajo vigilancia.")
                if dx_score_sat < 6:
                    recs.append("4. :orange[**Mejorar satisfaccion**]: score bajo impacta retencion y NPS.")
                if not recs:
                    recs.append(":green[**Performance de comercios dentro de parametros optimos.**] Mantener monitoreo continuo.")
                st.markdown("\n".join(recs))

    st.divider()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="perf_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        seg_sel = st.selectbox("Segmento", ["Todos"] + segmentos, key="perf_seg")
    with fc3:
        mcc_sel = st.selectbox("MCC", ["Todos"] + mccs, key="perf_mcc")
    with fc4:
        ciu_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="perf_ciu")
    with fc5:
        niv_sel = st.selectbox("Nivel riesgo", ["Todos"] + [str(n) for n in niveles], key="perf_niv")

    WHERE = build_where(fecha_inicio, fecha_fin, seg_sel, mcc_sel, ciu_sel, niv_sel,
                        segmentos, mccs, ciudades, [str(n) for n in niveles])

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(TRANSACCIONES_PERIODO) AS TXN_SUM,
            ROUND(SUM(VOLUMEN_BRUTO_COP)/1e9, 2) AS VOL_B,
            ROUND(AVG(TICKET_PROMEDIO_COP), 0) AS TICKET_PROM,
            ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA_APROB_PROM,
            ROUND(SUM(CASE WHEN RIESGO_CHURN THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_CHURN,
            ROUND(SUM(CASE WHEN EN_MONITOREO_FRAUDE THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_FRAUDE_MON,
            ROUND(AVG(SCORE_SATISFACCION), 1) AS SCORE_SAT
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL"])
        txn_sum = int(r["TXN_SUM"])
        vol_b = float(r["VOL_B"])
        ticket_prom = float(r["TICKET_PROM"])
        tasa_aprob = float(r["TASA_APROB_PROM"])
        pct_churn = float(r["PCT_CHURN"])
        pct_fraude_mon = float(r["PCT_FRAUDE_MON"])
        score_sat = float(r["SCORE_SAT"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total registros", f"{total:,}")
        k2.metric("Txns periodo", f"{txn_sum:,}")
        k3.metric("Volumen bruto", f"${vol_b:.2f}B COP")
        k4.metric("Ticket promedio", f"${ticket_prom:,.0f} COP")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Tasa aprobacion prom", f"{tasa_aprob:.1f}%")
        k6.metric("Riesgo churn", f"{pct_churn:.1f}%", delta=f"{pct_churn:.1f}%", delta_color="inverse")
        k7.metric("En monitoreo fraude", f"{pct_fraude_mon:.1f}%", delta=f"{pct_fraude_mon:.1f}%", delta_color="inverse")
        k8.metric("Score satisfaccion", f"{score_sat:.1f}/10")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_CORTE) AS MES,
                   SUM(TRANSACCIONES_PERIODO) AS TXN,
                   ROUND(SUM(VOLUMEN_BRUTO_COP)/1e9, 2) AS VOL_B
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Transacciones y volumen**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["TXN"].tolist(), name="Transacciones",
                    fill="tozeroy", line=dict(color=COLORS[0], width=2),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["VOL_B"].tolist(), name="Volumen (B COP)",
                    yaxis="y2", line=dict(color=COLORS[1], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="Transacciones"),
                    yaxis2=dict(title="Volumen (B COP)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Segmento y MCC**")
            tree_df = run_query(f"""
                SELECT SEGMENTO, MCC, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
                LIMIT 50
            """)
            if tree_df.empty:
                st.info("Sin datos.")
            else:
                labels, parents, values, colors_t = [], [], [], []
                seg_totals = tree_df.groupby("SEGMENTO")["N"].sum().sort_values(ascending=False)
                for i, (seg, seg_total) in enumerate(seg_totals.items()):
                    labels.append(seg)
                    parents.append("")
                    values.append(int(seg_total))
                    colors_t.append(COLORS[i % len(COLORS)])
                    sub = tree_df[tree_df["SEGMENTO"] == seg].head(5)
                    for _, row in sub.iterrows():
                        labels.append(str(row["MCC"]))
                        parents.append(seg)
                        values.append(int(row["N"]))
                        colors_t.append(COLORS[i % len(COLORS)])
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    marker=dict(colors=colors_t),
                    textinfo="label+value+percent parent",
                    hovertemplate="<b>%{label}</b><br>Registros: %{value:,}<br>%{percentParent:.1%} del padre<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("**Heatmap — Volumen promedio por segmento y ciudad**")
        heat_df = run_query(f"""
            SELECT SEGMENTO, CIUDAD, ROUND(AVG(VOLUMEN_BRUTO_COP)/1e6, 1) AS VOL_M
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="SEGMENTO", columns="CIUDAD", values="VOL_M", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E3F2FD"], [0.3, "#29B5E8"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"${v:.0f}M" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Segmento: %{y}<br>Ciudad: %{x}<br>Volumen prom: $%{z:.1f}M COP<extra></extra>",
                colorbar=dict(title="$M COP"),
            ))
            fig_heat.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF",
                height=500, margin=dict(l=140, r=40, t=30, b=80),
                xaxis=dict(tickangle=-45, tickfont=dict(size=12)),
                yaxis=dict(tickfont=dict(size=12)),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        col3, col4 = st.columns(2)

        with col3:
            st.markdown("**Funnel — Calidad de la base**")
            activos_n = int(run_query(f"SELECT COUNT(DISTINCT ID_COMERCIO) AS N FROM {TABLE} WHERE {WHERE} AND INDICE_ACTIVIDAD > 0.5")["N"].iloc[0])
            bajo_riesgo_n = int(run_query(f"SELECT COUNT(DISTINCT ID_COMERCIO) AS N FROM {TABLE} WHERE {WHERE} AND NIVEL_RIESGO <= 2")["N"].iloc[0])
            sin_churn_n = int(run_query(f"SELECT COUNT(DISTINCT ID_COMERCIO) AS N FROM {TABLE} WHERE {WHERE} AND NOT RIESGO_CHURN")["N"].iloc[0])
            satisfechos_n = int(run_query(f"SELECT COUNT(DISTINCT ID_COMERCIO) AS N FROM {TABLE} WHERE {WHERE} AND SCORE_SATISFACCION >= 7")["N"].iloc[0])
            total_comercios = int(run_query(f"SELECT COUNT(DISTINCT ID_COMERCIO) AS N FROM {TABLE} WHERE {WHERE}")["N"].iloc[0])
            funnel_data = [
                ("Total comercios", total_comercios),
                ("Activos (idx>0.5)", activos_n),
                ("Bajo riesgo", bajo_riesgo_n),
                ("Sin riesgo churn", sin_churn_n),
                ("Satisfechos (>=7)", satisfechos_n),
            ]
            fig_funnel = go.Figure(go.Funnel(
                y=[f[0] for f in funnel_data],
                x=[f[1] for f in funnel_data],
                textinfo="value+percent initial",
                marker=dict(color=[COLORS[0], COLORS[2], COLORS[5], COLORS[1], COLORS[3]]),
                hovertemplate="%{y}: %{x:,}<extra></extra>",
            ))
            fig_funnel.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=10, r=10, t=30, b=10),
            )
            st.plotly_chart(fig_funnel, use_container_width=True)

        with col4:
            st.markdown("**Distribucion por segmento**")
            seg_df = run_query(f"""
                SELECT SEGMENTO, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if seg_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=seg_df["SEGMENTO"].tolist(),
                    values=seg_df["N"].tolist(),
                    hole=0.5,
                    marker=dict(colors=COLORS[:len(seg_df)]),
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
                ))
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.markdown("**Tasa de aprobacion vs tasa de fraude (burbuja = volumen)**")
        scatter_df = run_query(f"""
            SELECT ID_COMERCIO,
                   ROUND(TASA_APROBACION*100, 1) AS TASA_APROB,
                   ROUND(TASA_FRAUDE*100, 2) AS TASA_FRAUDE,
                   VOLUMEN_BRUTO_COP,
                   SEGMENTO
            FROM {TABLE}
            WHERE {WHERE}
            ORDER BY VOLUMEN_BRUTO_COP DESC
            LIMIT 200
        """)
        if not scatter_df.empty:
            segs = scatter_df["SEGMENTO"].unique().tolist()
            fig_scatter = go.Figure()
            for i, seg in enumerate(segs):
                sub = scatter_df[scatter_df["SEGMENTO"] == seg]
                fig_scatter.add_trace(go.Scatter(
                    x=sub["TASA_APROB"].tolist(),
                    y=sub["TASA_FRAUDE"].tolist(),
                    mode="markers",
                    name=seg,
                    marker=dict(
                        size=[max(5, min(40, float(v) / 1e7)) for v in sub["VOLUMEN_BRUTO_COP"].tolist()],
                        color=COLORS[i % len(COLORS)],
                        opacity=0.7,
                    ),
                    hovertemplate="Comercio: %{text}<br>Aprob: %{x}%<br>Fraude: %{y}%<extra></extra>",
                    text=sub["ID_COMERCIO"].tolist(),
                ))
            fig_scatter.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=450,
                margin=dict(l=60, r=40, t=30, b=60),
                xaxis=dict(title="Tasa aprobacion (%)"),
                yaxis=dict(title="Tasa fraude (%)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

        st.markdown("**Top 10 comercios por volumen bruto**")
        top_df = run_query(f"""
            SELECT ID_COMERCIO, SEGMENTO, CIUDAD, MCC,
                   VOLUMEN_BRUTO_COP, TRANSACCIONES_PERIODO,
                   ROUND(TASA_APROBACION*100, 1) AS TASA_APROB,
                   ROUND(TASA_FRAUDE*100, 2) AS TASA_FRAUDE,
                   ROUND(CRECIMIENTO_MOM_PCT, 1) AS MOM,
                   ROUND(SCORE_SATISFACCION, 1) AS SCORE_SAT
            FROM {TABLE} WHERE {WHERE}
            ORDER BY VOLUMEN_BRUTO_COP DESC LIMIT 10
        """)
        if not top_df.empty:
            top_df["VOLUMEN_BRUTO_COP"] = (top_df["VOLUMEN_BRUTO_COP"] / 1e6).round(1)
            st.dataframe(top_df.rename(columns={
                "ID_COMERCIO": "Comercio", "SEGMENTO": "Segmento", "CIUDAD": "Ciudad", "MCC": "MCC",
                "VOLUMEN_BRUTO_COP": "Volumen ($M COP)", "TRANSACCIONES_PERIODO": "Txns",
                "TASA_APROB": "% Aprobacion", "TASA_FRAUDE": "% Fraude",
                "MOM": "Crec MoM %", "SCORE_SAT": "Satisfaccion"
            }), use_container_width=True, hide_index=True)

with tab2:
    st.markdown("**Simulador de retencion y revenue**")
    st.caption("Estime el impacto en retencion y revenue ajustando las variables clave de performance. El modelo pondera cada factor segun su impacto relativo.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_mdr = st.slider("MDR efectivo (%)", 0.5, 5.0, 2.0, step=0.1, key="perf_sim_mdr",
                            help="Mayor MDR genera mas ingreso pero puede impactar satisfaccion")
        sim_fraude = st.slider("Tasa de fraude (%)", 0.0, 5.0, 0.5, step=0.1, key="perf_sim_fraude",
                               help="Mayor fraude genera perdidas y deteriora confianza")
        sim_cbk = st.slider("Tasa de contracargos (%)", 0.0, 5.0, 0.5, step=0.1, key="perf_sim_cbk",
                            help="Contracargos reducen ingreso neto y aumentan costos operativos")
        sim_crec = st.slider("Crecimiento MoM (%)", -20, 50, 5, key="perf_sim_crec",
                             help="Crecimiento positivo indica base saludable")
        sim_actividad = st.slider("Indice de actividad (0-1)", 0.0, 1.0, 0.7, step=0.05, key="perf_sim_act",
                                  help="Mayor actividad indica comercios mas comprometidos")
        sim_satisf = st.slider("Score satisfaccion (1-10)", 1.0, 10.0, 7.0, step=0.5, key="perf_sim_sat",
                               help="Mayor satisfaccion reduce churn y mejora NPS")

    def calcular_retencion(mdr, fraude, cbk, crec, act, sat):
        s_mdr = min(mdr / 5.0, 1.0) * 0.8 + 0.2
        s_fraude = max(0.0, 1.0 - fraude / 5.0)
        s_cbk = max(0.0, 1.0 - cbk / 5.0)
        s_crec = min(max((crec + 20) / 70, 0.0), 1.0)
        s_act = act
        s_sat = (sat - 1) / 9
        score = (0.15 * s_mdr + 0.15 * s_fraude + 0.12 * s_cbk + 0.18 * s_crec + 0.18 * s_act + 0.22 * s_sat)
        ret = 0.40 + score * 0.55
        return round(min(max(ret, 0.40), 0.95), 3)

    ret_pred = calcular_retencion(sim_mdr, sim_fraude, sim_cbk, sim_crec, sim_actividad, sim_satisf)
    ret_pct = ret_pred * 100

    with col_result:
        if ret_pct >= 85:
            nivel = "Alta"
            color_nivel = "#36B37E"
            interpretacion = "Excelente retencion estimada. La combinacion de crecimiento, satisfaccion y bajo riesgo genera un ecosistema saludable. Mantener estrategia actual."
        elif ret_pct >= 70:
            nivel = "Media-Alta"
            color_nivel = "#29B5E8"
            interpretacion = "Retencion aceptable con espacio de mejora. Revisar comercios en zona de riesgo y priorizar acciones de satisfaccion."
        elif ret_pct >= 55:
            nivel = "Media"
            color_nivel = "#FFAB00"
            interpretacion = "Retencion por debajo del benchmark. Priorizar reduccion de fraude/contracargos y mejorar experiencia del comercio."
        else:
            nivel = "Baja"
            color_nivel = "#DE350B"
            interpretacion = "Retencion critica. Accion inmediata: plan de retencion, reducir fricciones, mejorar comunicacion y resolver problemas de fraude."

        st.metric("Retencion estimada", f"{ret_pct:.1f}%")
        if ret_pct >= 85:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif ret_pct >= 70:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif ret_pct >= 55:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(ret_pct),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[40, 95], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[40, 50], color="#FFCDD2"),
                    dict(range=[50, 55], color="#FFEBEE"),
                    dict(range=[55, 65], color="#FFF3E0"),
                    dict(range=[65, 70], color="#FFF8E1"),
                    dict(range=[70, 85], color="#E3F2FD"),
                    dict(range=[85, 95], color="#E8F5E9"),
                ],
                threshold=dict(line=dict(color="#FF8B00", width=3), thickness=0.8, value=80),
            ),
        ))
        fig_gauge.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF",
            height=250, margin=dict(l=30, r=30, t=30, b=10),
        )
        st.plotly_chart(fig_gauge, use_container_width=True)

        st.info(interpretacion)

    st.divider()

    st.markdown("**Comparador de escenarios**")
    st.caption("Guarde hasta 3 configuraciones para comparar lado a lado.")

    if "perf_escenarios" not in st.session_state:
        st.session_state.perf_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="perf_save"):
        if len(st.session_state.perf_escenarios) >= 3:
            st.session_state.perf_escenarios.pop(0)
        st.session_state.perf_escenarios.append({
            "MDR (%)": sim_mdr,
            "Fraude (%)": sim_fraude,
            "Contracargos (%)": sim_cbk,
            "Crec MoM (%)": sim_crec,
            "Idx actividad": sim_actividad,
            "Satisfaccion": sim_satisf,
            "Retencion est": f"{ret_pct:.1f}%",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.perf_escenarios:
        esc_df = pd.DataFrame(st.session_state.perf_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="perf_clear"):
            st.session_state.perf_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
