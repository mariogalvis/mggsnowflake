import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.PROCESAMIENTO_TRANSACCIONES.AUTORIZACIONES"

COLORS = ["#29B5E8", "#FF8B00", "#36B37E", "#6554C0", "#DE350B", "#11567F", "#FFAB00", "#00A3BF"]


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
    redes = run_query(f"SELECT DISTINCT RED FROM {TABLE} ORDER BY 1")["RED"].tolist()
    resultados = run_query(f"SELECT DISTINCT RESULTADO FROM {TABLE} ORDER BY 1")["RESULTADO"].tolist()
    bancos = run_query(f"SELECT DISTINCT BANCO_EMISOR FROM {TABLE} ORDER BY 1")["BANCO_EMISOR"].tolist()
    tipos = run_query(f"SELECT DISTINCT TIPO_TARJETA FROM {TABLE} ORDER BY 1")["TIPO_TARJETA"].tolist()
    entornos = run_query(f"SELECT DISTINCT ENTORNO FROM {TABLE} ORDER BY 1")["ENTORNO"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return redes, resultados, bancos, tipos, entornos, fechas


def build_where(fecha_ini, fecha_f, red_s, resultado_s, banco_s, tipo_s, entorno_s,
                all_redes, all_resultados, all_bancos, all_tipos, all_entornos):
    clauses = [f"FECHA_HORA::DATE BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if red_s and red_s != "Todos" and red_s in all_redes:
        clauses.append(f"RED = '{red_s}'")
    if resultado_s and resultado_s != "Todos" and resultado_s in all_resultados:
        clauses.append(f"RESULTADO = '{resultado_s}'")
    if banco_s and banco_s != "Todos" and banco_s in all_bancos:
        clauses.append(f"BANCO_EMISOR = '{banco_s}'")
    if tipo_s and tipo_s != "Todos" and tipo_s in all_tipos:
        clauses.append(f"TIPO_TARJETA = '{tipo_s}'")
    if entorno_s and entorno_s != "Todos" and entorno_s in all_entornos:
        clauses.append(f"ENTORNO = '{entorno_s}'")
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


st.header(":material/verified: Autorizaciones")
st.caption("Flujo completo de autorizacion de cada transaccion ante el banco emisor. Codigo respuesta, motivo rechazo, latencia de red y validaciones (CVV, AVS, 3DS).")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Falta de visibilidad sobre por que se rechazan transacciones y donde se generan cuellos de botella en el flujo de autorizacion con los emisores.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Cada solicitud de autorizacion se registra con codigo de respuesta ISO, motivo de rechazo, latencia de red/emisor y validaciones (CVV, AVS, 3D Secure).")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Optimizar la tasa de aprobacion identificando emisores lentos, motivos de rechazo recurrentes y validaciones que agregan friccion. Cada punto porcentual = millones en revenue.")

    redes, resultados, bancos, tipos, entornos, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_HORA::DATE BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL_AUTH,
            SUM(CASE WHEN RESULTADO='Aprobada' THEN 1 ELSE 0 END) AS APROBADAS,
            SUM(CASE WHEN RESULTADO='Rechazada' THEN 1 ELSE 0 END) AS RECHAZADAS,
            ROUND(AVG(LATENCIA_TOTAL_MS), 0) AS LATENCIA_PROM,
            ROUND(AVG(LATENCIA_EMISOR_MS), 0) AS LATENCIA_EMISOR,
            ROUND(SUM(MONTO_COP)/1e9, 2) AS MONTO_B,
            ROUND(SUM(CASE WHEN STAND_IN_PROCESSING THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS STANDIN_PCT
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL_AUTH"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL_AUTH"])
        dx_aprob = int(dx["APROBADAS"])
        dx_tasa = round(dx_aprob / dx_total * 100, 1)
        dx_latencia = float(dx["LATENCIA_PROM"])

        dx_worst_banco = run_query(f"""
            SELECT BANCO_EMISOR, ROUND(SUM(CASE WHEN RESULTADO='Rechazada' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS TASA_RECH
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_top_rechazo = run_query(f"""
            SELECT CODIGO_RESPUESTA, COUNT(*) AS N
            FROM {TABLE} WHERE {dx_where} AND RESULTADO='Rechazada'
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)

        if dx_tasa >= 90:
            dx_estado = f"Tasa de aprobacion ({color_tag(dx_tasa, 90, 80)}) :green[**saludable**]. Volumen: {dx_total:,} autorizaciones."
        elif dx_tasa >= 80:
            dx_estado = f"Tasa de aprobacion ({color_tag(dx_tasa, 90, 80)}) en :orange[**zona de atencion**]."
        else:
            dx_estado = f"Tasa de aprobacion ({color_tag(dx_tasa, 90, 80)}) :red[**critica**]. Requiere accion inmediata."

        dx_lines = [dx_estado]
        if not dx_worst_banco.empty:
            wb = dx_worst_banco.iloc[0]
            dx_lines.append(f"- Emisor mas rechazos: **{wb['BANCO_EMISOR']}** ({color_tag(float(wb['TASA_RECH']), 10, 20, fmt='{:.1f}%', inverse=True)})")
        if not dx_top_rechazo.empty:
            tr = dx_top_rechazo.iloc[0]
            dx_lines.append(f"- Motivo rechazo #1: :red[**{tr['CODIGO_RESPUESTA']}**] ({int(tr['N']):,} rechazos)")
        dx_lines.append(f"- Latencia total promedio: **{dx_latencia:.0f} ms** {'— :green[**dentro de SLA**]' if dx_latencia < 800 else '— :red[**excede SLA 800ms**]'}")
        dx_lines.append(f"- Stand-in processing: **{float(dx['STANDIN_PCT']):.1f}%** {'— :green[**bajo**]' if float(dx['STANDIN_PCT']) < 5 else '— :orange[**elevado**]'}")

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
                if dx_tasa < 85:
                    recs.append("1. :red[**Negociar con emisores**]: tasa de aprobacion por debajo del objetivo.")
                if dx_latencia > 800:
                    recs.append("2. :red[**Optimizar latencia**]: emisores lentos impactan conversion.")
                if float(dx["STANDIN_PCT"]) > 5:
                    recs.append("3. :orange[**Investigar stand-in**]: porcentaje elevado indica problemas de conectividad con emisores.")
                if not dx_top_rechazo.empty and "51" in str(dx_top_rechazo.iloc[0]["CODIGO_RESPUESTA"]):
                    recs.append("4. :orange[**Fondos insuficientes dominante**]: considerar reintentos inteligentes o split payments.")
                if not recs:
                    recs.append(":green[**Flujo de autorizacion operando dentro de parametros optimos.**] Mantener monitoreo continuo.")
                st.markdown("\n".join(recs))

    st.divider()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="auth_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        red_sel = st.selectbox("Red", ["Todos"] + redes, key="auth_red")
    with fc3:
        resultado_sel = st.selectbox("Resultado", ["Todos"] + resultados, key="auth_resultado")
    with fc4:
        banco_sel = st.selectbox("Banco emisor", ["Todos"] + bancos, key="auth_banco")
    with fc5:
        tipo_sel = st.selectbox("Tipo tarjeta", ["Todos"] + tipos, key="auth_tipo")

    entorno_sel = "Todos"

    WHERE = build_where(fecha_inicio, fecha_fin, red_sel, resultado_sel, banco_sel, tipo_sel, entorno_sel,
                        redes, resultados, bancos, tipos, entornos)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL_AUTH,
            SUM(CASE WHEN RESULTADO='Aprobada' THEN 1 ELSE 0 END) AS APROBADAS,
            SUM(CASE WHEN RESULTADO='Rechazada' THEN 1 ELSE 0 END) AS RECHAZADAS,
            ROUND(AVG(LATENCIA_TOTAL_MS), 0) AS LATENCIA_TOTAL,
            ROUND(AVG(LATENCIA_RED_MS), 0) AS LATENCIA_RED,
            ROUND(AVG(LATENCIA_EMISOR_MS), 0) AS LATENCIA_EMISOR,
            ROUND(SUM(MONTO_COP)/1e9, 2) AS MONTO_B,
            ROUND(SUM(CASE WHEN CVV_VALIDADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS CVV_PCT
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL_AUTH"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL_AUTH"])
        aprobadas = int(r["APROBADAS"])
        rechazadas = int(r["RECHAZADAS"])
        tasa = round(aprobadas / total * 100, 1)

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Autorizaciones", f"{total:,}")
        k2.metric("Tasa aprobacion", f"{tasa:.1f}%")
        k3.metric("Monto autorizado", f"${float(r['MONTO_B']):.2f}B COP")
        k4.metric("Latencia total prom", f"{float(r['LATENCIA_TOTAL']):.0f} ms")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Aprobadas", f"{aprobadas:,}")
        k6.metric("Rechazadas", f"{rechazadas:,}", delta=f"{round(rechazadas/total*100,1):.1f}%", delta_color="inverse")
        k7.metric("Latencia emisor prom", f"{float(r['LATENCIA_EMISOR']):.0f} ms")
        k8.metric("CVV validado", f"{float(r['CVV_PCT']):.1f}%")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES,
                   COUNT(*) AS TOTAL,
                   SUM(CASE WHEN RESULTADO='Aprobada' THEN 1 ELSE 0 END) AS APROBADAS,
                   ROUND(AVG(LATENCIA_TOTAL_MS), 0) AS LATENCIA
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Tasa aprobacion y latencia**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                tasa_vals = [round(float(a)/float(t)*100, 1) if float(t) > 0 else 0
                             for a, t in zip(trend_df["APROBADAS"], trend_df["TOTAL"])]
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=tasa_vals, name="% Aprobacion",
                    fill="tozeroy", line=dict(color=COLORS[2], width=2),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["LATENCIA"].tolist(), name="Latencia (ms)",
                    yaxis="y2", line=dict(color=COLORS[4], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="% Aprobacion", range=[0, 100]),
                    yaxis2=dict(title="Latencia (ms)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Rechazos por codigo de respuesta y red**")
            tree_df = run_query(f"""
                SELECT RED, CODIGO_RESPUESTA, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE} AND RESULTADO='Rechazada'
                GROUP BY 1, 2 ORDER BY 3 DESC
            """)
            if tree_df.empty:
                st.info("Sin datos de rechazos.")
            else:
                labels, parents, values, colors_t = [], [], [], []
                red_totals = tree_df.groupby("RED")["N"].sum().sort_values(ascending=False)
                for i, (red, red_total) in enumerate(red_totals.items()):
                    labels.append(str(red))
                    parents.append("")
                    values.append(int(red_total))
                    colors_t.append(COLORS[i % len(COLORS)])
                    sub = tree_df[tree_df["RED"] == red].head(5)
                    for _, row in sub.iterrows():
                        labels.append(str(row["CODIGO_RESPUESTA"]))
                        parents.append(str(red))
                        values.append(int(row["N"]))
                        colors_t.append(COLORS[i % len(COLORS)])
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    marker=dict(colors=colors_t),
                    textinfo="label+value+percent parent",
                    hovertemplate="<b>%{label}</b><br>Rechazos: %{value:,}<br>%{percentParent:.1%} del padre<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("**Heatmap — Latencia promedio por banco emisor y red**")
        heat_df = run_query(f"""
            SELECT BANCO_EMISOR, RED, ROUND(AVG(LATENCIA_TOTAL_MS), 0) AS LATENCIA
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="BANCO_EMISOR", columns="RED", values="LATENCIA", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E8F5E9"], [0.3, "#FFF3E0"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"{int(v)}ms" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Banco: %{y}<br>Red: %{x}<br>Latencia: %{z}ms<extra></extra>",
                colorbar=dict(title="ms"),
            ))
            fig_heat.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF",
                height=500, margin=dict(l=160, r=40, t=30, b=80),
                xaxis=dict(tickangle=-45, tickfont=dict(size=12)),
                yaxis=dict(tickfont=dict(size=12)),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        col3, col4 = st.columns(2)

        with col3:
            st.markdown("**Funnel — Flujo de autorizacion**")
            cvv_n = run_query(f"SELECT SUM(CASE WHEN CVV_VALIDADO THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")
            avs_n = run_query(f"SELECT SUM(CASE WHEN AVS_VALIDADO THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")
            ds3_n = run_query(f"SELECT SUM(CASE WHEN REQUIRIO_3DS THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")
            funnel_data = [
                ("Solicitudes", total),
                ("CVV validado", int(cvv_n.iloc[0]["N"]) if not cvv_n.empty else 0),
                ("AVS validado", int(avs_n.iloc[0]["N"]) if not avs_n.empty else 0),
                ("3DS requerido", int(ds3_n.iloc[0]["N"]) if not ds3_n.empty else 0),
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
            st.markdown("**Distribucion por modo de autorizacion**")
            modo_df = run_query(f"""
                SELECT MODO_AUTORIZACION, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if modo_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=modo_df["MODO_AUTORIZACION"].tolist(),
                    values=modo_df["N"].tolist(),
                    hole=0.5,
                    marker=dict(colors=COLORS[:len(modo_df)]),
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
                ))
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.markdown("**Top 10 codigos de rechazo**")
        rechazo_df = run_query(f"""
            SELECT CODIGO_RESPUESTA, COUNT(*) AS N,
                   ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM {TABLE} WHERE {WHERE} AND RESULTADO='Rechazada'), 1) AS PCT
            FROM {TABLE} WHERE {WHERE} AND RESULTADO='Rechazada'
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """)
        if not rechazo_df.empty:
            fig_bar = go.Figure(go.Bar(
                x=rechazo_df["N"].tolist(),
                y=rechazo_df["CODIGO_RESPUESTA"].tolist(),
                orientation="h",
                marker=dict(color=COLORS[4]),
                text=[f"{int(n):,} ({float(p):.1f}%)" for n, p in zip(rechazo_df["N"], rechazo_df["PCT"])],
                textposition="outside",
                hovertemplate="%{y}: %{x:,}<extra></extra>",
            ))
            fig_bar.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=200, r=80, t=30, b=30),
                xaxis=dict(title="Cantidad"),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

with tab2:
    st.markdown("**Simulador de autorizacion**")
    st.caption("Estime la probabilidad de aprobacion de una transaccion ajustando los parametros del flujo de autorizacion ante el emisor.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_monto = st.slider("Monto de transaccion (miles COP)", 10, 10000, 500, step=50, key="auth_sim_monto",
                              help="Montos altos tienen mayor escrutinio del emisor")
        sim_cvv = st.slider("CVV presente y valido (%)", 0, 100, 85, key="auth_sim_cvv",
                            help="Transacciones sin CVV tienen mayor rechazo")
        sim_3ds = st.slider("% Transacciones con 3D Secure", 0, 100, 40, key="auth_sim_3ds",
                            help="3DS reduce fraude pero agrega friccion; emisores lo valoran positivamente")
        sim_latencia_red = st.slider("Latencia de red esperada (ms)", 50, 2000, 300, step=50, key="auth_sim_lat",
                                     help="Alta latencia puede causar timeouts en el emisor")
        sim_historico_emisor = st.slider("Tasa historica del emisor (%)", 50, 99, 88, key="auth_sim_emisor",
                                         help="Emisores con baja tasa historica tienen mayor probabilidad de rechazo")
        sim_standin = st.slider("% Stand-in processing", 0, 30, 3, key="auth_sim_standin",
                                help="Stand-in alto indica problemas de conectividad con el emisor")

    def calcular_tasa_auth(monto, cvv, ds3, lat, hist, standin):
        if monto < 100:
            s_monto = 0.9
        elif monto < 500:
            s_monto = 1.0
        elif monto < 2000:
            s_monto = 0.7
        elif monto < 5000:
            s_monto = 0.4
        else:
            s_monto = 0.2
        s_cvv = cvv / 100
        s_3ds = 0.6 + (ds3 / 100) * 0.4
        s_lat = max(0.0, 1.0 - (lat / 2000) * 0.9)
        s_hist = hist / 100
        s_standin = max(0.0, 1.0 - (standin / 30) * 0.8)
        tasa = (0.12 * s_monto + 0.18 * s_cvv + 0.14 * s_3ds + 0.16 * s_lat + 0.25 * s_hist + 0.15 * s_standin)
        tasa_final = 0.50 + tasa * 0.48
        return round(min(max(tasa_final, 0.50), 0.98), 3)

    tasa_pred = calcular_tasa_auth(sim_monto, sim_cvv, sim_3ds, sim_latencia_red, sim_historico_emisor, sim_standin)
    tasa_pct = tasa_pred * 100

    with col_result:
        if tasa_pct >= 92:
            nivel = "Alta"
            color_nivel = "#36B37E"
            interpretacion = "Alta probabilidad de aprobacion. CVV, 3DS y perfil del emisor alineados. Transaccion de bajo riesgo para el emisor."
        elif tasa_pct >= 80:
            nivel = "Media-Alta"
            color_nivel = "#29B5E8"
            interpretacion = "Probabilidad aceptable. Oportunidad de mejora en validaciones o seleccion de emisor. Revisar parametros de autenticacion."
        elif tasa_pct >= 65:
            nivel = "Media"
            color_nivel = "#FFAB00"
            interpretacion = "Probabilidad moderada. Multiples factores de riesgo presentes. Considerar ajustar monto, agregar 3DS o mejorar conectividad."
        else:
            nivel = "Baja"
            color_nivel = "#DE350B"
            interpretacion = "Alta probabilidad de rechazo. Revisar urgente: latencia, validaciones ausentes o emisor con baja tasa historica."

        st.metric("Tasa de aprobacion estimada", f"{tasa_pct:.1f}%")
        if tasa_pct >= 92:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif tasa_pct >= 80:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif tasa_pct >= 65:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(tasa_pct),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[50, 98], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[50, 65], color="#FFCDD2"),
                    dict(range=[65, 75], color="#FFEBEE"),
                    dict(range=[75, 80], color="#FFF3E0"),
                    dict(range=[80, 85], color="#FFF8E1"),
                    dict(range=[85, 92], color="#E3F2FD"),
                    dict(range=[92, 98], color="#E8F5E9"),
                ],
                threshold=dict(line=dict(color="#FF8B00", width=3), thickness=0.8, value=90),
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

    if "auth_escenarios" not in st.session_state:
        st.session_state.auth_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="auth_save"):
        if len(st.session_state.auth_escenarios) >= 3:
            st.session_state.auth_escenarios.pop(0)
        st.session_state.auth_escenarios.append({
            "Monto (K COP)": sim_monto,
            "CVV valido %": sim_cvv,
            "% 3DS": sim_3ds,
            "Latencia red (ms)": sim_latencia_red,
            "Tasa emisor %": sim_historico_emisor,
            "% Stand-in": sim_standin,
            "Tasa estimada": f"{tasa_pct:.1f}%",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.auth_escenarios:
        esc_df = pd.DataFrame(st.session_state.auth_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="auth_clear"):
            st.session_state.auth_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
