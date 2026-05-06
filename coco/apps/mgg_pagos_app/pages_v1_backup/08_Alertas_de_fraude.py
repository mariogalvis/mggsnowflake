import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.FRAUDE_Y_SEGURIDAD_PAGOS.ALERTAS_FRAUDE"

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
    tipos = run_query(f"SELECT DISTINCT TIPO_ALERTA FROM {TABLE} ORDER BY 1")["TIPO_ALERTA"].tolist()
    severidades = run_query(f"SELECT DISTINCT SEVERIDAD FROM {TABLE} ORDER BY 1")["SEVERIDAD"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO FROM {TABLE} ORDER BY 1")["ESTADO"].tolist()
    acciones = run_query(f"SELECT DISTINCT ACCION_TOMADA FROM {TABLE} ORDER BY 1")["ACCION_TOMADA"].tolist()
    origenes = run_query(f"SELECT DISTINCT ORIGEN_ALERTA FROM {TABLE} ORDER BY 1")["ORIGEN_ALERTA"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return tipos, severidades, estados, acciones, origenes, fechas


def build_where(fecha_ini, fecha_f, tipo_s, sev_s, estado_s, accion_s,
                all_tipos, all_sevs, all_estados, all_acciones):
    clauses = [f"FECHA_HORA::DATE BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if tipo_s and tipo_s != "Todos" and tipo_s in all_tipos:
        clauses.append(f"TIPO_ALERTA = '{tipo_s}'")
    if sev_s and sev_s != "Todos" and sev_s in all_sevs:
        clauses.append(f"SEVERIDAD = '{sev_s}'")
    if estado_s and estado_s != "Todos" and estado_s in all_estados:
        clauses.append(f"ESTADO = '{estado_s}'")
    if accion_s and accion_s != "Todos" and accion_s in all_acciones:
        clauses.append(f"ACCION_TOMADA = '{accion_s}'")
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


st.header("🚨 Alertas de fraude")
st.caption("Gestion del ciclo de vida de alertas: desde la deteccion hasta la resolucion. Severidad, acciones, tiempos de respuesta y efectividad del equipo de prevencion.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container():
        st.markdown("**💡 Que resuelve**")
        st.markdown("Gestion manual e ineficiente de los casos de fraude detectados, sin trazabilidad del ciclo de vida desde la deteccion hasta la resolucion.")
with c2:
    with st.container():
        st.markdown("**⚙️ Como funciona**")
        st.markdown("Cada alerta se clasifica por tipo (velocidad, monto, geolocalizacion, dispositivo), severidad (critica, alta, media, baja), se asigna a un analista y se rastrea la accion tomada.")
with c3:
    with st.container():
        st.markdown("**📈 Valor de negocio**")
        st.markdown("Reducir el tiempo medio de respuesta ante fraude, priorizar los casos de mayor impacto y medir la efectividad del equipo de prevencion con metricas claras.")

tipos, severidades, estados, acciones, origenes, fechas_df = get_filter_options()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_where = f"FECHA_HORA::DATE BETWEEN '{fmin}' AND '{fmax}'"

dx_kpi = run_query(f"""
    SELECT
        COUNT(*) AS TOTAL_ALERTAS,
        SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END) AS FRAUDES_CONF,
        ROUND(AVG(TIEMPO_RESOLUCION_MIN), 0) AS TMRR,
        ROUND(SUM(PERDIDA_EVITADA_COP)/1e9, 1) AS EVITADO_B,
        ROUND(SUM(PERDIDA_ESTIMADA_COP)/1e9, 1) AS PERDIDA_B,
        SUM(CASE WHEN ESTADO = 'Abierta' OR ESTADO = 'En investigacion' THEN 1 ELSE 0 END) AS PENDIENTES
    FROM {TABLE}
    WHERE {dx_where}
""")

if not dx_kpi.empty and dx_kpi["TOTAL_ALERTAS"].iloc[0] > 0:
    dx_r = dx_kpi.iloc[0]
    dx_total = int(dx_r["TOTAL_ALERTAS"])
    dx_fraudes = int(dx_r["FRAUDES_CONF"])
    dx_tasa_conf = round(dx_fraudes / dx_total * 100, 1)
    dx_tmrr = float(dx_r["TMRR"])
    dx_evitado = float(dx_r["EVITADO_B"])
    dx_perdida = float(dx_r["PERDIDA_B"])
    dx_pendientes = int(dx_r["PENDIENTES"])

    dx_worst_tipo = run_query(f"""
        SELECT TIPO_ALERTA, COUNT(*) AS N FROM {TABLE}
        WHERE {dx_where} AND FRAUDE_CONFIRMADO
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """)
    dx_criticas = run_query(f"""
        SELECT COUNT(*) AS N FROM {TABLE}
        WHERE {dx_where} AND SEVERIDAD = 'Critica' AND (ESTADO = 'Abierta' OR ESTADO = 'En investigacion')
    """)

    if dx_tmrr <= 30:
        dx_estado = f"Tiempo medio de resolucion ({color_tag(dx_tmrr, 30, 60, fmt='{:.0f} min', inverse=True)}) esta :green[**dentro del SLA**]. Tasa de confirmacion: {dx_tasa_conf:.1f}%."
    elif dx_tmrr <= 60:
        dx_estado = f"Tiempo medio de resolucion ({color_tag(dx_tmrr, 30, 60, fmt='{:.0f} min', inverse=True)}) :orange[**cerca del limite**]. Tasa de confirmacion: {dx_tasa_conf:.1f}%."
    else:
        dx_estado = f"Tiempo medio de resolucion ({color_tag(dx_tmrr, 30, 60, fmt='{:.0f} min', inverse=True)}) :red[**excede el SLA**]. Requiere :red[**mas analistas o automatizacion**]."

    dx_lines = [dx_estado]
    if not dx_worst_tipo.empty:
        dx_lines.append(f"- Tipo de alerta mas confirmado como fraude: :red[**{dx_worst_tipo.iloc[0]['TIPO_ALERTA']}**]")
    if dx_pendientes > 0:
        dx_lines.append(f"- Alertas pendientes: {'**:red[' if dx_pendientes > 100 else '**:orange['}{dx_pendientes:,}]**")
    dx_lines.append(f"- Perdida evitada: :green[**${dx_evitado:.1f}B COP**] | Perdida estimada: :red[**${dx_perdida:.1f}B COP**]")
    if not dx_criticas.empty and int(dx_criticas.iloc[0]["N"]) > 0:
        dx_lines.append(f"- Alertas criticas sin resolver: :red[**{int(dx_criticas.iloc[0]['N'])}**]")

    st.markdown("")
    d1, d2 = st.columns(2)
    with d1:
        with st.container():
            st.markdown("**📊 Diagnostico CEO**")
            st.markdown("\n".join(dx_lines))
    with d2:
        with st.container():
            st.markdown("**💡 Recomendaciones**")
            recs = []
            if dx_tmrr > 45:
                recs.append("1. :red[**Reducir TMRR**]: el tiempo de resolucion impacta directamente las perdidas. Evaluar automatizacion de acciones para alertas de severidad baja/media.")
            if dx_pendientes > 50:
                recs.append("2. :orange[**Descongestionar cola**]: reasignar recursos para cerrar alertas pendientes antes de que escalen.")
            if not dx_worst_tipo.empty:
                recs.append(f"3. :orange[**Reforzar deteccion de '{dx_worst_tipo.iloc[0]['TIPO_ALERTA']}'**]: tipo de fraude mas frecuente, mejorar reglas especificas.")
            if dx_evitado < dx_perdida:
                recs.append("4. :red[**Mejorar tasa de prevencion**]: las perdidas superan lo evitado. Revisar umbrales de bloqueo automatico.")
            if not recs:
                recs.append(":green[**El equipo de prevencion opera dentro de parametros optimos.**] Mantener monitoreo.")
            st.markdown("\n".join(recs))

st.divider()

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="al_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        tipo_sel = st.selectbox("Tipo alerta", ["Todos"] + tipos, key="al_tipo")
    with fc3:
        sev_sel = st.selectbox("Severidad", ["Todos"] + severidades, key="al_sev")
    with fc4:
        estado_sel = st.selectbox("Estado", ["Todos"] + estados, key="al_estado")
    with fc5:
        accion_sel = st.selectbox("Accion tomada", ["Todos"] + acciones, key="al_accion")

    WHERE = build_where(fecha_inicio, fecha_fin, tipo_sel, sev_sel, estado_sel, accion_sel,
                        tipos, severidades, estados, acciones)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL_ALERTAS,
            SUM(CASE WHEN FRAUDE_CONFIRMADO THEN 1 ELSE 0 END) AS FRAUDES_CONF,
            SUM(CASE WHEN ESTADO = 'Falso positivo' THEN 1 ELSE 0 END) AS FALSOS_POS,
            ROUND(AVG(TIEMPO_RESOLUCION_MIN), 0) AS TMRR,
            ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE_PROM,
            ROUND(SUM(PERDIDA_EVITADA_COP)/1e9, 1) AS EVITADO_B,
            ROUND(SUM(PERDIDA_ESTIMADA_COP)/1e9, 1) AS PERDIDA_B,
            SUM(CASE WHEN ESTADO IN ('Abierta', 'En investigacion', 'Escalada') THEN 1 ELSE 0 END) AS PENDIENTES
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL_ALERTAS"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL_ALERTAS"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total alertas", f"{total:,}")
        k2.metric("Fraude confirmado", f"{int(r['FRAUDES_CONF']):,}", delta=f"{round(int(r['FRAUDES_CONF'])/total*100, 1)}%", delta_color="inverse")
        k3.metric("Falsos positivos", f"{int(r['FALSOS_POS']):,}", delta=f"{round(int(r['FALSOS_POS'])/total*100, 1)}%", delta_color="inverse")
        k4.metric("TMRR", f"{float(r['TMRR']):.0f} min")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Score fraude prom", f"{float(r['SCORE_PROM']):.1f}%")
        k6.metric("Perdida evitada", f"${float(r['EVITADO_B']):.1f}B COP")
        k7.metric("Perdida estimada", f"${float(r['PERDIDA_B']):.1f}B COP", delta_color="inverse")
        k8.metric("Pendientes", f"{int(r['PENDIENTES']):,}", delta_color="inverse")

        st.divider()

        # --- ROW 1: Stacked area trend + Treemap severidad×tipo ---
        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES,
                   SEVERIDAD, COUNT(*) AS N
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)

        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.markdown("**Tendencia mensual por severidad**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                sev_colors = {"Critica": "#DE350B", "Alta": "#FF8B00", "Media": "#FFAB00", "Baja": "#29B5E8"}
                sev_order = ["Baja", "Media", "Alta", "Critica"]
                fig_stack = go.Figure()
                for sev in sev_order:
                    sub = trend_df[trend_df["SEVERIDAD"] == sev]
                    if not sub.empty:
                        fig_stack.add_trace(go.Scatter(
                            x=sub["MES"].tolist(),
                            y=[float(v) for v in sub["N"]],
                            name=sev, mode="lines",
                            stackgroup="one",
                            line=dict(color=sev_colors.get(sev, "#999"), width=1),
                            fillcolor=sev_colors.get(sev, "#999").replace("#", "rgba(") + ")" if False else
                                f"rgba({int(sev_colors.get(sev, '#999999')[1:3], 16)},{int(sev_colors.get(sev, '#999999')[3:5], 16)},{int(sev_colors.get(sev, '#999999')[5:7], 16)},0.6)",
                        ))
                fig_stack.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=40, r=20, t=30, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    yaxis=dict(title="Alertas"),
                )
                st.plotly_chart(fig_stack, use_container_width=True)

        with col_chart2:
            st.markdown("**Impacto por tipo de alerta — Perdida evitada**")
            tree_df = run_query(f"""
                SELECT TIPO_ALERTA, COUNT(*) AS N,
                       ROUND(SUM(PERDIDA_EVITADA_COP)/1e6, 0) AS EVITADO_MM
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 3 DESC
            """)
            if tree_df.empty:
                st.info("Sin datos de tipos.")
            else:
                fig_tree = go.Figure(go.Treemap(
                    labels=tree_df["TIPO_ALERTA"].tolist(),
                    values=[float(v) for v in tree_df["EVITADO_MM"]],
                    parents=[""] * len(tree_df),
                    textinfo="label+value+percent root",
                    texttemplate="<b>%{label}</b><br>$%{value:,.0f}M<br>%{percentRoot:.1%}",
                    marker=dict(
                        colors=[float(v) for v in tree_df["EVITADO_MM"]],
                        colorscale=[[0, "#E8F5E9"], [0.3, "#36B37E"], [0.6, "#29B5E8"], [1, "#11567F"]],
                        line=dict(width=2, color="white"),
                    ),
                    hovertemplate="<b>%{label}</b><br>Evitado: $%{value:,.0f}M COP<br>%{percentRoot:.1%}<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=5, r=5, t=30, b=5),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        # --- ROW 2a: Heatmap (full width) ---
        st.markdown("**Heatmap — Severidad vs accion tomada**")
        heat_df = run_query(f"""
            SELECT SEVERIDAD, ACCION_TOMADA, COUNT(*) AS N
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="SEVERIDAD", columns="ACCION_TOMADA", values="N", fill_value=0)
            sev_order_map = {"Baja": 0, "Media": 1, "Alta": 2, "Critica": 3}
            pivot["_order"] = pivot.index.map(lambda x: sev_order_map.get(x, 99))
            pivot = pivot.sort_values("_order").drop(columns=["_order"])
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E3F2FD"], [0.3, "#29B5E8"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"{int(v)}" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Severidad: %{y}<br>Accion: %{x}<br>Alertas: %{z:,}<extra></extra>",
                colorbar=dict(title="Alertas"),
            ))
            fig_heat.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF",
                height=500, margin=dict(l=100, r=40, t=30, b=100),
                xaxis=dict(tickangle=-45, tickfont=dict(size=12)),
                yaxis=dict(tickfont=dict(size=12)),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        # --- ROW 2b: Funnel + Donut ---
        col_chart4, col_chart5 = st.columns(2)

        with col_chart4:
            st.markdown("**Funnel — Estado del ciclo de vida**")
            estado_df = run_query(f"""
                SELECT ESTADO, COUNT(*) AS N
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if estado_df.empty:
                st.info("Sin datos de estados.")
            else:
                estado_colors = {
                    "Abierta": "#DE350B", "En investigacion": "#FF8B00",
                    "Escalada": "#FFAB00", "Confirmada fraude": "#6554C0",
                    "Cerrada": "#36B37E", "Falso positivo": "#29B5E8",
                }
                est_list = estado_df["ESTADO"].tolist()
                est_vals = [int(v) for v in estado_df["N"]]
                est_colors = [estado_colors.get(e, "#999") for e in est_list]
                fig_funnel = go.Figure(go.Funnel(
                    y=est_list, x=est_vals,
                    textinfo="value+percent initial",
                    texttemplate="%{value:,} (%{percentInitial:.1%})",
                    marker=dict(color=est_colors, line=dict(width=1, color="white")),
                    connector=dict(line=dict(color="#DFE1E6", width=1)),
                ))
                fig_funnel.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=130, r=20, t=30, b=40),
                    showlegend=False,
                )
                st.plotly_chart(fig_funnel, use_container_width=True)

        with col_chart5:
            st.markdown("**Distribucion de acciones tomadas**")
            acc_df = run_query(f"""
                SELECT ACCION_TOMADA, COUNT(*) AS N
                FROM {TABLE}
                WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if acc_df.empty:
                st.info("Sin datos de acciones.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=acc_df["ACCION_TOMADA"].tolist(),
                    values=[float(v) for v in acc_df["N"]],
                    hole=0.55,
                    marker=dict(colors=["#DE350B", "#FF8B00", "#FFAB00", "#29B5E8", "#36B37E", "#6554C0", "#11567F", "#00A3BF"],
                                line=dict(color="white", width=2)),
                    textinfo="percent+label",
                    textposition="outside",
                    textfont=dict(size=9),
                    hovertemplate="<b>%{label}</b><br>Cantidad: %{value:,}<br>%{percent:.1%}<extra></extra>",
                    pull=[0.05, 0, 0, 0, 0, 0, 0, 0],
                ))
                total_acc = int(acc_df["N"].sum())
                fig_donut.add_annotation(
                    text=f"<b>{total_acc:,}</b><br>acciones",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=13, color="#11567F"),
                )
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=380, margin=dict(l=5, r=5, t=30, b=5),
                    showlegend=False,
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.divider()

        st.markdown("**Top 10 alertas por perdida estimada**")
        top_df = run_query(f"""
            SELECT REFERENCIA_ALERTA, TIPO_ALERTA, SEVERIDAD, ESTADO,
                   ACCION_TOMADA, ROUND(SCORE_FRAUDE*100,1) AS SCORE,
                   PERDIDA_ESTIMADA_COP, PERDIDA_EVITADA_COP,
                   TIEMPO_RESOLUCION_MIN AS TMRR_MIN,
                   FRAUDE_CONFIRMADO
            FROM {TABLE}
            WHERE {WHERE}
            ORDER BY PERDIDA_ESTIMADA_COP DESC
            LIMIT 10
        """)
        if not top_df.empty:
            st.dataframe(top_df, use_container_width=True)

        st.divider()

        st.markdown("**Alertas e insights automaticos**")

        criticas_abiertas = run_query(f"""
            SELECT COUNT(*) AS N FROM {TABLE}
            WHERE {WHERE} AND SEVERIDAD = 'Critica' AND ESTADO IN ('Abierta', 'En investigacion')
        """)
        slow_resolution = run_query(f"""
            SELECT COUNT(*) AS N FROM {TABLE}
            WHERE {WHERE} AND TIEMPO_RESOLUCION_MIN > 60
        """)
        high_loss = run_query(f"""
            SELECT SUM(PERDIDA_ESTIMADA_COP) - SUM(PERDIDA_EVITADA_COP) AS NETO
            FROM {TABLE} WHERE {WHERE}
        """)

        if not criticas_abiertas.empty and int(criticas_abiertas.iloc[0]["N"]) > 0:
            st.error(f"**{int(criticas_abiertas.iloc[0]['N'])}** alertas criticas sin resolver. Requieren atencion inmediata.")
        if not slow_resolution.empty and int(slow_resolution.iloc[0]["N"]) > 0:
            pct_slow = round(int(slow_resolution.iloc[0]["N"]) / total * 100, 1)
            if pct_slow > 20:
                st.error(f"**{pct_slow}%** de las alertas superaron 60 min de resolucion. El SLA esta comprometido.")
            else:
                st.warning(f"{pct_slow}% de las alertas superaron 60 min de resolucion.")
        if not high_loss.empty and float(high_loss.iloc[0]["NETO"]) > 0:
            neto_b = float(high_loss.iloc[0]["NETO"]) / 1e9
            if neto_b > 1:
                st.error(f"Perdida neta (estimada - evitada): **${neto_b:.1f}B COP**. Las acciones preventivas no alcanzan.")
            else:
                st.success(f"Perdida neta controlada: ${neto_b:.1f}B COP.")


with tab2:
    st.markdown("**Simulador de gestion de alertas**")
    st.caption("Estime el impacto de cambios en la operacion del equipo de prevencion de fraude.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_analistas = st.slider("Numero de analistas", 1, 50, 10, key="al_sim_analistas",
                                  help="Mas analistas reducen el TMRR y la cola de pendientes")
        sim_auto_pct = st.slider("% de alertas con accion automatica", 0, 100, 30, key="al_sim_auto",
                                 help="Automatizar acciones para alertas de baja severidad")
        sim_umbral_score = st.slider("Umbral de score para bloqueo automatico", 50, 99, 80, key="al_sim_umbral",
                                     help="Score por encima del cual se bloquea sin revision manual")
        sim_sla_min = st.slider("SLA objetivo (minutos)", 10, 120, 30, key="al_sim_sla",
                                help="Tiempo maximo objetivo para resolver una alerta")
        sim_tasa_fraude = st.slider("Tasa de fraude esperada (%)", 0.1, 5.0, 1.5, step=0.1, key="al_sim_fraude",
                                    help="Tasa base de fraude en el portafolio")
        sim_vol_alertas = st.slider("Volumen diario de alertas", 10, 500, 100, key="al_sim_vol",
                                    help="Cantidad esperada de alertas por dia")

    def calcular_operacion(analistas, auto_pct, umbral, sla, tasa_fraude, vol):
        alertas_manuales = vol * (1 - auto_pct / 100)
        cap_analista = max(1, 60 / max(sla, 1) * 8)
        cap_total = analistas * cap_analista
        cobertura = min(100, cap_total / max(alertas_manuales, 1) * 100)
        tmrr_est = max(5, sla * (alertas_manuales / max(cap_total, 1)))
        falsos_pos = max(5, 100 - umbral) * 0.5
        tasa_deteccion = min(99, 60 + umbral * 0.35 + auto_pct * 0.1)
        perdida_evitada_pct = min(95, tasa_deteccion * 0.9)
        return round(cobertura, 1), round(tmrr_est, 0), round(falsos_pos, 1), round(tasa_deteccion, 1), round(perdida_evitada_pct, 1)

    cob, tmrr_e, fp_e, det_e, prev_e = calcular_operacion(
        sim_analistas, sim_auto_pct, sim_umbral_score, sim_sla_min, sim_tasa_fraude, sim_vol_alertas
    )

    with col_result:
        if cob >= 90 and tmrr_e <= sim_sla_min:
            nivel = "Optimo"
            color_nivel = "#36B37E"
            interpretacion = "Operacion bien dimensionada. El equipo puede manejar el volumen de alertas dentro del SLA con alta cobertura."
        elif cob >= 70:
            nivel = "Aceptable"
            color_nivel = "#29B5E8"
            interpretacion = "Cobertura razonable pero con riesgo de saturacion en picos. Considerar aumentar automatizacion o analistas."
        elif cob >= 50:
            nivel = "En riesgo"
            color_nivel = "#FFAB00"
            interpretacion = "Capacidad insuficiente para el volumen. Se acumularan alertas pendientes. Aumentar analistas o automatizacion urgentemente."
        else:
            nivel = "Critico"
            color_nivel = "#DE350B"
            interpretacion = "Equipo completamente sobrepasado. Las alertas se acumularan y el fraude pasara sin revision. Accion inmediata requerida."

        st.metric("Cobertura operativa", f"{cob:.1f}%")
        if cob >= 90:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif cob >= 70:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif cob >= 50:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

        m1, m2 = st.columns(2)
        m1.metric("TMRR estimado", f"{tmrr_e:.0f} min")
        m2.metric("Tasa deteccion", f"{det_e:.1f}%")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(cob),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[0, 100], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[0, 50], color="#FFEBEE"),
                    dict(range=[50, 70], color="#FFF3E0"),
                    dict(range=[70, 90], color="#E3F2FD"),
                    dict(range=[90, 100], color="#E8F5E9"),
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

    if "al_escenarios" not in st.session_state:
        st.session_state.al_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="al_save"):
        if len(st.session_state.al_escenarios) >= 3:
            st.session_state.al_escenarios.pop(0)
        st.session_state.al_escenarios.append({
            "Analistas": sim_analistas,
            "% Automatizacion": sim_auto_pct,
            "Umbral bloqueo": sim_umbral_score,
            "SLA (min)": sim_sla_min,
            "Vol alertas/dia": sim_vol_alertas,
            "Cobertura": f"{cob:.1f}%",
            "TMRR est": f"{tmrr_e:.0f} min",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.al_escenarios:
        esc_df = pd.DataFrame(st.session_state.al_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="al_clear"):
            st.session_state.al_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados.")
