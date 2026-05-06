import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.COMERCIOS_Y_ADQUIRENCIA.AFILIACION_COMERCIOS"

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
    canales = run_query(f"SELECT DISTINCT CANAL_CAPTACION FROM {TABLE} ORDER BY 1")["CANAL_CAPTACION"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO_AFILIACION FROM {TABLE} ORDER BY 1")["ESTADO_AFILIACION"].tolist()
    pasos = run_query(f"SELECT DISTINCT PASO_ACTUAL FROM {TABLE} ORDER BY 1")["PASO_ACTUAL"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    tipos = run_query(f"SELECT DISTINCT TIPO_COMERCIO FROM {TABLE} ORDER BY 1")["TIPO_COMERCIO"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_SOLICITUD) AS FMIN, MAX(FECHA_SOLICITUD) AS FMAX FROM {TABLE}")
    return canales, estados, pasos, ciudades, tipos, fechas


def build_where(fecha_ini, fecha_f, canal_s, est_s, paso_s, ciu_s, tipo_s,
                all_can, all_est, all_paso, all_ciu, all_tipo):
    clauses = [f"FECHA_SOLICITUD BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if canal_s and canal_s != "Todos" and canal_s in all_can:
        clauses.append(f"CANAL_CAPTACION = '{canal_s}'")
    if est_s and est_s != "Todos" and est_s in all_est:
        clauses.append(f"ESTADO_AFILIACION = '{est_s}'")
    if paso_s and paso_s != "Todos" and paso_s in all_paso:
        clauses.append(f"PASO_ACTUAL = '{paso_s}'")
    if ciu_s and ciu_s != "Todos" and ciu_s in all_ciu:
        clauses.append(f"CIUDAD = '{ciu_s}'")
    if tipo_s and tipo_s != "Todos" and tipo_s in all_tipo:
        clauses.append(f"TIPO_COMERCIO = '{tipo_s}'")
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


st.header(":material/person_add: Afiliacion de comercios")
st.caption("Proceso de onboarding de nuevos comercios paso a paso. Canal de captacion, documentos, validaciones DIAN, due diligence y estado.")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Procesos de afiliacion lentos y opacos que retrasan el time-to-revenue y generan abandono.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Se rastrea cada etapa del onboarding: solicitud, documentacion, validacion DIAN, due diligence, aprobacion de riesgo, configuracion tecnica y activacion.")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Reducir el tiempo de afiliacion, disminuir abandono y acelerar generacion de ingresos por nuevos comercios.")

    canales, estados, pasos, ciudades, tipos, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_SOLICITUD BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(CASE WHEN ESTADO_AFILIACION='Aprobada' THEN 1 ELSE 0 END) AS APROBADAS,
            SUM(CASE WHEN ESTADO_AFILIACION='Rechazada' THEN 1 ELSE 0 END) AS RECHAZADAS,
            SUM(CASE WHEN ESTADO_AFILIACION NOT IN ('Aprobada','Rechazada') THEN 1 ELSE 0 END) AS EN_PROCESO,
            ROUND(AVG(DIAS_PROCESO), 1) AS DIAS_PROM,
            ROUND(SUM(CASE WHEN DOCUMENTOS_COMPLETOS THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_DOCS,
            ROUND(SUM(CASE WHEN VERIFICACION_DIAN_OK THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_DIAN,
            ROUND(SUM(CASE WHEN DUE_DILIGENCE_OK THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_DD
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL"])
        dx_aprobadas = int(dx["APROBADAS"])
        dx_tasa_aprob = round(dx_aprobadas / dx_total * 100, 1)
        dx_dias = float(dx["DIAS_PROM"])
        dx_pct_docs = float(dx["PCT_DOCS"])
        dx_pct_dian = float(dx["PCT_DIAN"])

        dx_worst_canal = run_query(f"""
            SELECT CANAL_CAPTACION, ROUND(SUM(CASE WHEN ESTADO_AFILIACION='Rechazada' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA_RECH
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_top_motivo = run_query(f"""
            SELECT MOTIVO_RECHAZO, COUNT(*) AS N
            FROM {TABLE} WHERE {dx_where} AND ESTADO_AFILIACION='Rechazada' AND MOTIVO_RECHAZO IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)

        if dx_tasa_aprob >= 70:
            dx_estado = f"Tasa de aprobacion de afiliaciones ({color_tag(dx_tasa_aprob, 70, 50)}) :green[**saludable**]. Procesando {dx_total:,} solicitudes."
        elif dx_tasa_aprob >= 50:
            dx_estado = f"Tasa de aprobacion ({color_tag(dx_tasa_aprob, 70, 50)}) en :orange[**zona de atencion**]. Dias promedio: {dx_dias:.0f}."
        else:
            dx_estado = f"Tasa de aprobacion ({color_tag(dx_tasa_aprob, 70, 50)}) :red[**critica**]. Requiere revision del proceso."

        dx_lines = [dx_estado]
        if not dx_worst_canal.empty:
            wc = dx_worst_canal.iloc[0]
            dx_lines.append(f"- Canal mas rechazos: **{wc['CANAL_CAPTACION']}** ({color_tag(float(wc['TASA_RECH']), 10, 25, fmt='{:.1f}%', inverse=True)})")
        if not dx_top_motivo.empty:
            tm = dx_top_motivo.iloc[0]
            dx_lines.append(f"- Principal motivo rechazo: **{tm['MOTIVO_RECHAZO']}** ({int(tm['N']):,} casos)")
        dx_lines.append(f"- Documentos completos: **{dx_pct_docs:.1f}%** — {':green[**buen nivel**]' if dx_pct_docs > 70 else ':orange[**cuello de botella**]'}")
        dx_lines.append(f"- Dias promedio proceso: **{dx_dias:.0f} dias** {'— :green[**dentro de SLA**]' if dx_dias < 15 else '— :red[**excede SLA**]'}")

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
                if dx_tasa_aprob < 60:
                    recs.append("1. :red[**Revisar criterios de aprobacion**]: tasa muy baja, posible sobre-restriccion.")
                if dx_dias > 20:
                    recs.append("2. :red[**Optimizar tiempos de proceso**]: SLA de 15 dias excedido significativamente.")
                if dx_pct_docs < 60:
                    recs.append("3. :orange[**Automatizar recoleccion de documentos**]: alto porcentaje incompleto retrasa proceso.")
                if dx_pct_dian < 70:
                    recs.append("4. :orange[**Mejorar prevalidacion DIAN**]: alta tasa de fallo en verificacion fiscal.")
                if not recs:
                    recs.append(":green[**Proceso de afiliacion operando dentro de parametros optimos.**] Mantener monitoreo continuo.")
                st.markdown("\n".join(recs))

    st.divider()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="afil_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        canal_sel = st.selectbox("Canal captacion", ["Todos"] + canales, key="afil_canal")
    with fc3:
        est_sel = st.selectbox("Estado", ["Todos"] + estados, key="afil_est")
    with fc4:
        ciu_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="afil_ciu")
    with fc5:
        tipo_sel = st.selectbox("Tipo comercio", ["Todos"] + tipos, key="afil_tipo")

    paso_sel = "Todos"

    WHERE = build_where(fecha_inicio, fecha_fin, canal_sel, est_sel, paso_sel, ciu_sel, tipo_sel,
                        canales, estados, pasos, ciudades, tipos)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(CASE WHEN ESTADO_AFILIACION='Aprobada' THEN 1 ELSE 0 END) AS APROBADAS,
            SUM(CASE WHEN ESTADO_AFILIACION='Rechazada' THEN 1 ELSE 0 END) AS RECHAZADAS,
            SUM(CASE WHEN ESTADO_AFILIACION NOT IN ('Aprobada','Rechazada') THEN 1 ELSE 0 END) AS EN_PROCESO,
            ROUND(AVG(DIAS_PROCESO), 1) AS DIAS_PROM,
            ROUND(SUM(CASE WHEN DOCUMENTOS_COMPLETOS THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_DOCS,
            ROUND(SUM(CASE WHEN VERIFICACION_DIAN_OK THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_DIAN,
            ROUND(SUM(CASE WHEN DUE_DILIGENCE_OK THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_DD
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL"])
        aprobadas = int(r["APROBADAS"])
        rechazadas = int(r["RECHAZADAS"])
        en_proceso = int(r["EN_PROCESO"])
        dias_prom = float(r["DIAS_PROM"])
        pct_docs = float(r["PCT_DOCS"])
        pct_dian = float(r["PCT_DIAN"])
        pct_dd = float(r["PCT_DD"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total solicitudes", f"{total:,}")
        k2.metric("Aprobadas", f"{aprobadas:,}", delta=f"{round(aprobadas/total*100,1):.1f}%")
        k3.metric("Rechazadas", f"{rechazadas:,}", delta=f"{round(rechazadas/total*100,1):.1f}%", delta_color="inverse")
        k4.metric("En proceso", f"{en_proceso:,}")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Dias prom proceso", f"{dias_prom:.0f}")
        k6.metric("% Docs completos", f"{pct_docs:.1f}%")
        k7.metric("% DIAN OK", f"{pct_dian:.1f}%")
        k8.metric("% Due Diligence OK", f"{pct_dd:.1f}%")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_SOLICITUD) AS MES,
                   COUNT(*) AS SOLICITUDES,
                   ROUND(SUM(CASE WHEN ESTADO_AFILIACION='Aprobada' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA_APROB
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Solicitudes y tasa aprobacion**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["SOLICITUDES"].tolist(), name="Solicitudes",
                    fill="tozeroy", line=dict(color=COLORS[0], width=2),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["TASA_APROB"].tolist(), name="Tasa aprobacion (%)",
                    yaxis="y2", line=dict(color=COLORS[1], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="Solicitudes"),
                    yaxis2=dict(title="Tasa aprobacion (%)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Canal de captacion y estado**")
            tree_df = run_query(f"""
                SELECT CANAL_CAPTACION, ESTADO_AFILIACION, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
            """)
            if tree_df.empty:
                st.info("Sin datos.")
            else:
                labels, parents, values, colors_t = [], [], [], []
                canal_totals = tree_df.groupby("CANAL_CAPTACION")["N"].sum().sort_values(ascending=False)
                for i, (canal, canal_total) in enumerate(canal_totals.items()):
                    labels.append(canal)
                    parents.append("")
                    values.append(int(canal_total))
                    colors_t.append(COLORS[i % len(COLORS)])
                    sub = tree_df[tree_df["CANAL_CAPTACION"] == canal]
                    for _, row in sub.iterrows():
                        labels.append(str(row["ESTADO_AFILIACION"]))
                        parents.append(canal)
                        values.append(int(row["N"]))
                        colors_t.append(COLORS[i % len(COLORS)])
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    marker=dict(colors=colors_t),
                    textinfo="label+value+percent parent",
                    hovertemplate="<b>%{label}</b><br>Solicitudes: %{value:,}<br>%{percentParent:.1%} del padre<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("**Heatmap — Solicitudes por paso actual y canal de captacion**")
        heat_df = run_query(f"""
            SELECT PASO_ACTUAL, CANAL_CAPTACION, COUNT(*) AS N
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="PASO_ACTUAL", columns="CANAL_CAPTACION", values="N", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E3F2FD"], [0.3, "#29B5E8"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"{int(v)}" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Paso: %{y}<br>Canal: %{x}<br>Solicitudes: %{z:,}<extra></extra>",
                colorbar=dict(title="Solicitudes"),
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
            st.markdown("**Funnel — Proceso de afiliacion**")
            docs_n = int(run_query(f"SELECT SUM(CASE WHEN DOCUMENTOS_COMPLETOS THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")["N"].iloc[0])
            dian_n = int(run_query(f"SELECT SUM(CASE WHEN VERIFICACION_DIAN_OK THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")["N"].iloc[0])
            dd_n = int(run_query(f"SELECT SUM(CASE WHEN DUE_DILIGENCE_OK THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")["N"].iloc[0])
            funnel_data = [
                ("Solicitudes", total),
                ("Docs completos", docs_n),
                ("DIAN OK", dian_n),
                ("Due Diligence OK", dd_n),
                ("Aprobadas", aprobadas),
            ]
            fig_funnel = go.Figure(go.Funnel(
                y=[f[0] for f in funnel_data],
                x=[f[1] for f in funnel_data],
                textinfo="value+percent initial",
                marker=dict(color=[COLORS[0], COLORS[5], COLORS[1], COLORS[3], COLORS[2]]),
                hovertemplate="%{y}: %{x:,}<extra></extra>",
            ))
            fig_funnel.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=10, r=10, t=30, b=10),
            )
            st.plotly_chart(fig_funnel, use_container_width=True)

        with col4:
            st.markdown("**Distribucion por tipo de comercio**")
            tipo_df = run_query(f"""
                SELECT TIPO_COMERCIO, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if tipo_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=tipo_df["TIPO_COMERCIO"].tolist(),
                    values=tipo_df["N"].tolist(),
                    hole=0.5,
                    marker=dict(colors=COLORS[:len(tipo_df)]),
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
                ))
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.markdown("**Motivos de rechazo**")
        motivo_df = run_query(f"""
            SELECT MOTIVO_RECHAZO, COUNT(*) AS N
            FROM {TABLE}
            WHERE {WHERE} AND ESTADO_AFILIACION='Rechazada' AND MOTIVO_RECHAZO IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """)
        if not motivo_df.empty:
            fig_bar = go.Figure(go.Bar(
                y=motivo_df["MOTIVO_RECHAZO"].tolist()[::-1],
                x=motivo_df["N"].tolist()[::-1],
                orientation="h",
                marker=dict(color=COLORS[4]),
                hovertemplate="%{y}: %{x:,}<extra></extra>",
            ))
            fig_bar.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=200, r=40, t=30, b=40),
                xaxis=dict(title="Solicitudes rechazadas"),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        st.markdown("**Top ejecutivos por volumen de afiliacion**")
        ejec_df = run_query(f"""
            SELECT EJECUTIVO_ASIGNADO,
                   COUNT(*) AS TOTAL,
                   SUM(CASE WHEN ESTADO_AFILIACION='Aprobada' THEN 1 ELSE 0 END) AS APROBADAS,
                   ROUND(AVG(DIAS_PROCESO), 1) AS DIAS_PROM,
                   ROUND(SUM(VOLUMEN_ESTIMADO_MENSUAL_COP)/1e6, 1) AS VOL_EST_M
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """)
        if not ejec_df.empty:
            ejec_df["TASA_APROB"] = (ejec_df["APROBADAS"] / ejec_df["TOTAL"] * 100).round(1)
            st.dataframe(ejec_df[["EJECUTIVO_ASIGNADO", "TOTAL", "APROBADAS", "TASA_APROB", "DIAS_PROM", "VOL_EST_M"]].rename(columns={
                "EJECUTIVO_ASIGNADO": "Ejecutivo", "TOTAL": "Solicitudes", "APROBADAS": "Aprobadas",
                "TASA_APROB": "% Aprobacion", "DIAS_PROM": "Dias prom", "VOL_EST_M": "Vol estimado ($M COP)"
            }), use_container_width=True, hide_index=True)

with tab2:
    st.markdown("**Simulador de tasa de aprobacion de afiliaciones**")
    st.caption("Estime la tasa de aprobacion ajustando las variables clave del proceso de onboarding. El modelo pondera cada factor segun su impacto en la conversion.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_dias = st.slider("Dias promedio de proceso", 1, 60, 12, key="afil_sim_dias",
                             help="Menos dias reduce abandono y mejora conversion")
        sim_docs = st.slider("% Documentos completos al ingresar", 0, 100, 70, key="afil_sim_docs",
                             help="Mayor completitud documental acelera proceso y reduce rechazo")
        sim_dian = st.slider("% Aprobacion DIAN", 0, 100, 80, key="afil_sim_dian",
                             help="Prevalidacion fiscal reduce rechazos tardios")
        sim_dd = st.slider("% Due Diligence aprobado", 0, 100, 75, key="afil_sim_dd",
                           help="Mejores filtros iniciales mejoran tasa de due diligence")
        sim_riesgo = st.slider("Nivel de riesgo promedio (1=bajo, 5=alto)", 1, 5, 2, key="afil_sim_riesgo",
                               help="Solicitudes de menor riesgo tienen mayor probabilidad de aprobacion")
        sim_term = st.slider("Terminales solicitadas promedio", 1, 30, 3, key="afil_sim_term",
                             help="Mayor volumen de terminales puede indicar comercio mas grande y estable")

    def calcular_tasa_afil(dias, docs, dian, dd, riesgo, term):
        s_dias = max(0.0, 1.0 - (dias / 60) * 0.8)
        s_docs = docs / 100
        s_dian = dian / 100
        s_dd = dd / 100
        s_riesgo = max(0.0, 1.0 - (riesgo - 1) / 4 * 0.7)
        s_term = min(term / 30, 1.0) * 0.5 + 0.5
        score = (0.15 * s_dias + 0.20 * s_docs + 0.22 * s_dian + 0.22 * s_dd + 0.15 * s_riesgo + 0.06 * s_term)
        tasa = 0.30 + score * 0.65
        return round(min(max(tasa, 0.30), 0.95), 3)

    tasa_pred = calcular_tasa_afil(sim_dias, sim_docs, sim_dian, sim_dd, sim_riesgo, sim_term)
    tasa_pct = tasa_pred * 100

    with col_result:
        if tasa_pct >= 80:
            nivel = "Alta"
            color_nivel = "#36B37E"
            interpretacion = "Excelente tasa de aprobacion. El proceso de onboarding esta optimizado. Mantener controles y escalar modelo a nuevos canales."
        elif tasa_pct >= 65:
            nivel = "Media-Alta"
            color_nivel = "#29B5E8"
            interpretacion = "Tasa aceptable con espacio de mejora. Revisar pasos con mayor abandono y automatizar validaciones."
        elif tasa_pct >= 50:
            nivel = "Media"
            color_nivel = "#FFAB00"
            interpretacion = "Tasa por debajo del benchmark. Priorizar completitud documental y prevalidacion DIAN para mejorar conversion."
        else:
            nivel = "Baja"
            color_nivel = "#DE350B"
            interpretacion = "Tasa critica. Accion inmediata: simplificar requisitos, automatizar DIAN y due diligence, reducir friccion en cada paso."

        st.metric("Tasa de aprobacion estimada", f"{tasa_pct:.1f}%")
        if tasa_pct >= 80:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif tasa_pct >= 65:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif tasa_pct >= 50:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(tasa_pct),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[30, 95], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[30, 40], color="#FFCDD2"),
                    dict(range=[40, 50], color="#FFEBEE"),
                    dict(range=[50, 60], color="#FFF3E0"),
                    dict(range=[60, 65], color="#FFF8E1"),
                    dict(range=[65, 80], color="#E3F2FD"),
                    dict(range=[80, 95], color="#E8F5E9"),
                ],
                threshold=dict(line=dict(color="#FF8B00", width=3), thickness=0.8, value=75),
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

    if "afil_escenarios" not in st.session_state:
        st.session_state.afil_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="afil_save"):
        if len(st.session_state.afil_escenarios) >= 3:
            st.session_state.afil_escenarios.pop(0)
        st.session_state.afil_escenarios.append({
            "Dias proceso": sim_dias,
            "% Docs completos": sim_docs,
            "% DIAN OK": sim_dian,
            "% Due Diligence": sim_dd,
            "Nivel riesgo": sim_riesgo,
            "Terminales sol": sim_term,
            "Tasa estimada": f"{tasa_pct:.1f}%",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.afil_escenarios:
        esc_df = pd.DataFrame(st.session_state.afil_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="afil_clear"):
            st.session_state.afil_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
