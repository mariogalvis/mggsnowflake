import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.LIQUIDACION_Y_COMPENSACION.COMPENSACION"

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
    tipos = run_query(f"SELECT DISTINCT TIPO_CLEARING FROM {TABLE} ORDER BY 1")["TIPO_CLEARING"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO FROM {TABLE} ORDER BY 1")["ESTADO"].tolist()
    ciclos = run_query(f"SELECT DISTINCT CICLO_SETTLEMENT FROM {TABLE} ORDER BY 1")["CICLO_SETTLEMENT"].tolist()
    modos = run_query(f"SELECT DISTINCT MODO_CONCILIACION FROM {TABLE} ORDER BY 1")["MODO_CONCILIACION"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_CLEARING) AS FMIN, MAX(FECHA_CLEARING) AS FMAX FROM {TABLE}")
    return redes, tipos, estados, ciclos, modos, fechas


def build_where(fecha_ini, fecha_f, red_s, tipo_s, estado_s, ciclo_s, modo_s,
                all_redes, all_tipos, all_estados, all_ciclos, all_modos):
    clauses = [f"FECHA_CLEARING BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if red_s and red_s != "Todos" and red_s in all_redes:
        clauses.append(f"RED = '{red_s}'")
    if tipo_s and tipo_s != "Todos" and tipo_s in all_tipos:
        clauses.append(f"TIPO_CLEARING = '{tipo_s}'")
    if estado_s and estado_s != "Todos" and estado_s in all_estados:
        clauses.append(f"ESTADO = '{estado_s}'")
    if ciclo_s and ciclo_s != "Todos" and ciclo_s in all_ciclos:
        clauses.append(f"CICLO_SETTLEMENT = '{ciclo_s}'")
    if modo_s and modo_s != "Todos" and modo_s in all_modos:
        clauses.append(f"MODO_CONCILIACION = '{modo_s}'")
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


st.header(":material/balance: Compensacion")
st.caption("Clearing y settlement entre entidades. Montos por entidad, diferencias, conciliacion y ciclo de settlement.")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Discrepancias en compensacion entre emisores, adquirentes y redes que generan diferencias no conciliadas.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Cada ciclo registra montos por entidad, diferencias detectadas, estado de conciliacion y fecha de settlement.")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Asegurar que todas las partes reciban fondos correctos, detectar discrepancias y reducir riesgo financiero.")

    redes, tipos, estados, ciclos, modos, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_CLEARING BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            ROUND(SUM(MONTO_BRUTO_COP)/1e9, 2) AS BRUTO_B,
            ROUND(SUM(MONTO_NETO_COP)/1e9, 2) AS NETO_B,
            ROUND(SUM(ABS(DIFERENCIA_DETECTADA_COP))/1e6, 1) AS DIF_M,
            ROUND(SUM(CASE WHEN REQUIERE_AJUSTE THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_AJUSTE,
            ROUND(AVG(HORAS_PROCESAMIENTO), 1) AS HORAS_PROM,
            ROUND(AVG(TASA_CONCILIACION)*100, 1) AS TASA_CONC,
            ROUND(SUM(CASE WHEN CONCILIACION_COMPLETA THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_COMPLETA
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL"])
        dx_tasa_conc = float(dx["TASA_CONC"])
        dx_pct_ajuste = float(dx["PCT_AJUSTE"])
        dx_pct_completa = float(dx["PCT_COMPLETA"])
        dx_horas = float(dx["HORAS_PROM"])

        dx_worst_red = run_query(f"""
            SELECT RED, ROUND(SUM(ABS(DIFERENCIA_DETECTADA_COP))/1e6, 1) AS DIF_M
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_worst_entidad = run_query(f"""
            SELECT ENTIDAD_EMISORA, ROUND(SUM(ABS(DIFERENCIA_DETECTADA_COP))/1e6, 1) AS DIF_M
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)

        if dx_tasa_conc >= 95:
            dx_estado = f"Tasa de conciliacion ({color_tag(dx_tasa_conc, 95, 85)}) :green[**saludable**]. {dx_pct_completa:.1f}% conciliacion completa."
        elif dx_tasa_conc >= 85:
            dx_estado = f"Tasa de conciliacion ({color_tag(dx_tasa_conc, 95, 85)}) en :orange[**zona de atencion**]. Diferencias: ${float(dx['DIF_M']):.0f}M COP."
        else:
            dx_estado = f"Tasa de conciliacion ({color_tag(dx_tasa_conc, 95, 85)}) :red[**critica**]. Requiere revision inmediata."

        dx_lines = [dx_estado]
        if not dx_worst_red.empty:
            wr = dx_worst_red.iloc[0]
            dx_lines.append(f"- Red con mayor diferencia: **{wr['RED']}** (${float(wr['DIF_M']):.0f}M COP)")
        if not dx_worst_entidad.empty:
            we = dx_worst_entidad.iloc[0]
            dx_lines.append(f"- Entidad emisora con mayor discrepancia: **{we['ENTIDAD_EMISORA']}** (${float(we['DIF_M']):.0f}M COP)")
        dx_lines.append(f"- Requiere ajuste: **{dx_pct_ajuste:.1f}%** {'— :green[**bajo**]' if dx_pct_ajuste < 10 else '— :red[**alto, revisar**]'}")
        dx_lines.append(f"- Horas procesamiento: **{dx_horas:.1f}h** {'— :green[**dentro de SLA**]' if dx_horas < 24 else '— :red[**excede SLA**]'}")

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
                if dx_tasa_conc < 90:
                    recs.append("1. :red[**Mejorar conciliacion**]: tasa por debajo del objetivo del 95%.")
                if dx_pct_ajuste > 10:
                    recs.append("2. :orange[**Reducir ajustes**]: alto porcentaje de registros requieren correccion manual.")
                if dx_horas > 24:
                    recs.append("3. :orange[**Optimizar tiempos**]: horas de procesamiento exceden SLA T+1.")
                if dx_pct_completa < 80:
                    recs.append("4. :red[**Cerrar conciliaciones pendientes**]: bajo porcentaje de conciliacion completa.")
                if not recs:
                    recs.append(":green[**Proceso de compensacion operando dentro de parametros optimos.**] Mantener monitoreo continuo.")
                st.markdown("\n".join(recs))

    st.divider()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="comp_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        red_sel = st.selectbox("Red", ["Todos"] + redes, key="comp_red")
    with fc3:
        tipo_sel = st.selectbox("Tipo clearing", ["Todos"] + tipos, key="comp_tipo")
    with fc4:
        estado_sel = st.selectbox("Estado", ["Todos"] + estados, key="comp_estado")
    with fc5:
        modo_sel = st.selectbox("Modo conciliacion", ["Todos"] + modos, key="comp_modo")

    ciclo_sel = "Todos"

    WHERE = build_where(fecha_inicio, fecha_fin, red_sel, tipo_sel, estado_sel, ciclo_sel, modo_sel,
                        redes, tipos, estados, ciclos, modos)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            ROUND(SUM(MONTO_BRUTO_COP)/1e9, 2) AS BRUTO_B,
            ROUND(SUM(MONTO_NETO_COP)/1e9, 2) AS NETO_B,
            ROUND(SUM(ABS(DIFERENCIA_DETECTADA_COP))/1e6, 1) AS DIF_M,
            ROUND(SUM(CASE WHEN REQUIERE_AJUSTE THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_AJUSTE,
            ROUND(AVG(HORAS_PROCESAMIENTO), 1) AS HORAS_PROM,
            ROUND(AVG(TASA_CONCILIACION)*100, 1) AS TASA_CONC,
            ROUND(SUM(CASE WHEN CONCILIACION_COMPLETA THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_COMPLETA
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total registros clearing", f"{total:,}")
        k2.metric("Monto bruto", f"${float(r['BRUTO_B']):.2f}B COP")
        k3.metric("Monto neto", f"${float(r['NETO_B']):.2f}B COP")
        k4.metric("Diferencias detectadas", f"${float(r['DIF_M']):,.0f}M COP")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("% Requiere ajuste", f"{float(r['PCT_AJUSTE']):.1f}%")
        k6.metric("Horas procesamiento prom", f"{float(r['HORAS_PROM']):.1f}h")
        k7.metric("Tasa conciliacion prom", f"{float(r['TASA_CONC']):.1f}%")
        k8.metric("% Conciliacion completa", f"{float(r['PCT_COMPLETA']):.1f}%")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_CLEARING) AS MES,
                   ROUND(SUM(MONTO_BRUTO_COP)/1e9, 2) AS BRUTO_B,
                   ROUND(SUM(ABS(DIFERENCIA_DETECTADA_COP))/1e6, 1) AS DIF_M
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Monto bruto y diferencias**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["BRUTO_B"].tolist(), name="Monto bruto (B COP)",
                    fill="tozeroy", line=dict(color=COLORS[0], width=2),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["DIF_M"].tolist(), name="Diferencias ($M COP)",
                    yaxis="y2", line=dict(color=COLORS[4], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="Bruto (B COP)"),
                    yaxis2=dict(title="Diferencias ($M COP)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Red y tipo de clearing**")
            tree_df = run_query(f"""
                SELECT RED, TIPO_CLEARING, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
            """)
            if tree_df.empty:
                st.info("Sin datos.")
            else:
                labels, parents, values, colors_t = [], [], [], []
                parent_totals = tree_df.groupby("RED")["N"].sum().sort_values(ascending=False)
                for i, (parent, parent_total) in enumerate(parent_totals.items()):
                    labels.append(parent)
                    parents.append("")
                    values.append(int(parent_total))
                    colors_t.append(COLORS[i % len(COLORS)])
                    sub = tree_df[tree_df["RED"] == parent]
                    for _, row in sub.iterrows():
                        labels.append(str(row["TIPO_CLEARING"]))
                        parents.append(parent)
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

        st.markdown("**Heatmap — Monto neto promedio por entidad emisora y adquirente**")
        heat_df = run_query(f"""
            SELECT ENTIDAD_EMISORA, ENTIDAD_ADQUIRENTE, ROUND(AVG(MONTO_NETO_COP)/1e6, 1) AS NETO_M
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="ENTIDAD_EMISORA", columns="ENTIDAD_ADQUIRENTE", values="NETO_M", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E3F2FD"], [0.3, "#29B5E8"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"${v:.1f}M" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Emisora: %{y}<br>Adquirente: %{x}<br>Neto prom: $%{z:.1f}M COP<extra></extra>",
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
            st.markdown("**Funnel — Ciclo de compensacion**")
            funnel_kpi = run_query(f"""
                SELECT
                    COUNT(*) AS TOTAL,
                    SUM(CASE WHEN ESTADO IN ('Procesado','Conciliado','Settlement') THEN 1 ELSE 0 END) AS PROCESADOS,
                    SUM(CASE WHEN CONCILIACION_COMPLETA THEN 1 ELSE 0 END) AS CONCILIADOS,
                    SUM(CASE WHEN CONCILIACION_COMPLETA AND NOT REQUIERE_AJUSTE THEN 1 ELSE 0 END) AS SIN_DIF,
                    SUM(CASE WHEN ESTADO = 'Settlement' OR FECHA_SETTLEMENT IS NOT NULL THEN 1 ELSE 0 END) AS SETTLED
                FROM {TABLE} WHERE {WHERE}
            """)
            if not funnel_kpi.empty:
                fk = funnel_kpi.iloc[0]
                funnel_data = [
                    ("Total", int(fk["TOTAL"])),
                    ("Procesados", int(fk["PROCESADOS"])),
                    ("Conciliados", int(fk["CONCILIADOS"])),
                    ("Sin diferencia", int(fk["SIN_DIF"])),
                    ("Settlement OK", int(fk["SETTLED"])),
                ]
                fig_funnel = go.Figure(go.Funnel(
                    y=[f[0] for f in funnel_data],
                    x=[f[1] for f in funnel_data],
                    textinfo="value+percent initial",
                    marker=dict(color=[COLORS[0], COLORS[1], COLORS[2], COLORS[3], COLORS[5]]),
                    hovertemplate="%{y}: %{x:,}<extra></extra>",
                ))
                fig_funnel.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_funnel, use_container_width=True)

        with col4:
            st.markdown("**Distribucion por modo de conciliacion**")
            modo_df = run_query(f"""
                SELECT MODO_CONCILIACION, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if modo_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=modo_df["MODO_CONCILIACION"].tolist(),
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

        st.markdown("**Top entidades por diferencias detectadas**")
        ent_df = run_query(f"""
            SELECT ENTIDAD_EMISORA AS ENTIDAD,
                   COUNT(*) AS REGISTROS,
                   ROUND(SUM(ABS(DIFERENCIA_DETECTADA_COP))/1e6, 1) AS DIF_M,
                   ROUND(SUM(CASE WHEN REQUIERE_AJUSTE THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_AJUSTE
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 3 DESC LIMIT 10
        """)
        if not ent_df.empty:
            fig_ent = go.Figure(go.Bar(
                y=ent_df["ENTIDAD"].tolist()[::-1],
                x=ent_df["DIF_M"].tolist()[::-1],
                orientation="h",
                marker=dict(color=COLORS[4]),
                text=[f"${v:.1f}M" for v in ent_df["DIF_M"].tolist()[::-1]],
                textposition="outside",
                hovertemplate="Entidad: %{y}<br>Diferencias: $%{x:.1f}M COP<extra></extra>",
            ))
            fig_ent.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=140, r=60, t=30, b=40),
                xaxis=dict(title="Diferencias ($M COP)"),
                yaxis=dict(title=""),
            )
            st.plotly_chart(fig_ent, use_container_width=True)

        st.markdown("**Items de excepcion por red**")
        exc_df = run_query(f"""
            SELECT RED,
                   COUNT(*) AS REGISTROS,
                   SUM(ITEMS_EXCEPCION) AS TOTAL_EXCEPCIONES,
                   ROUND(AVG(ITEMS_EXCEPCION), 1) AS PROM_EXCEPCIONES,
                   ROUND(AVG(TASA_CONCILIACION)*100, 1) AS TASA_CONC
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 3 DESC
        """)
        if not exc_df.empty:
            st.dataframe(exc_df.rename(columns={
                "RED": "Red", "REGISTROS": "Registros",
                "TOTAL_EXCEPCIONES": "Total excepciones", "PROM_EXCEPCIONES": "Prom excepciones",
                "TASA_CONC": "Tasa conciliacion %"
            }), use_container_width=True, hide_index=True)

with tab2:
    st.markdown("**Simulador de riesgo financiero en compensacion**")
    st.caption("Estime el score de riesgo financiero ajustando variables clave del proceso de compensacion. El modelo pondera cada factor segun su impacto relativo.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_tasa_conc = st.slider("Tasa de conciliacion (%)", 50, 100, 92, key="comp_sim_conc",
                                  help="Mayor tasa indica mejor alineacion entre entidades")
        sim_horas = st.slider("Horas de procesamiento", 4, 72, 18, step=2, key="comp_sim_horas",
                              help="Tiempo promedio para completar el ciclo de compensacion")
        sim_pct_ajuste = st.slider("% Registros con ajuste", 0, 40, 8, key="comp_sim_ajuste",
                                   help="Porcentaje de registros que requieren correccion manual")
        sim_excepciones = st.slider("Items de excepcion promedio", 0, 50, 5, key="comp_sim_exc",
                                    help="Cantidad promedio de items que no cuadran por lote")
        sim_pct_completa = st.slider("% Conciliacion completa", 50, 100, 85, key="comp_sim_comp",
                                     help="Porcentaje de ciclos que llegan a conciliacion completa")
        sim_mix_ciclo = st.slider("% Settlement mismo dia (T+0)", 0, 100, 30, key="comp_sim_mix",
                                  help="Mayor % T+0 reduce riesgo pero requiere mayor liquidez")

    def calcular_riesgo_comp(tasa_conc, horas, pct_ajuste, excepciones, pct_completa, mix_t0):
        s_conc = (tasa_conc - 50) / 50.0
        s_horas = max(0, 1.0 - horas / 72.0)
        s_ajuste = 1.0 - (pct_ajuste / 40.0)
        s_exc = max(0, 1.0 - excepciones / 50.0)
        s_completa = (pct_completa - 50) / 50.0
        s_mix = 0.3 + (mix_t0 / 100.0) * 0.7
        score = 0.20 * s_conc + 0.15 * s_horas + 0.20 * max(0, s_ajuste) + 0.15 * s_exc + 0.20 * s_completa + 0.10 * s_mix
        final = 0.50 + score * 0.48
        return round(min(max(final, 0.50), 0.98), 3)

    score_pred = calcular_riesgo_comp(sim_tasa_conc, sim_horas, sim_pct_ajuste, sim_excepciones, sim_pct_completa, sim_mix_ciclo)
    score_pct = score_pred * 100

    with col_result:
        if score_pct >= 92:
            nivel = "Bajo"
            color_nivel = "#36B37E"
            interpretacion = "Riesgo financiero bajo. Conciliacion eficiente con pocas discrepancias. Mantener controles actuales."
        elif score_pct >= 80:
            nivel = "Moderado-Bajo"
            color_nivel = "#29B5E8"
            interpretacion = "Riesgo moderado. Algunas areas de mejora en conciliacion o tiempos de procesamiento."
        elif score_pct >= 65:
            nivel = "Moderado"
            color_nivel = "#FFAB00"
            interpretacion = "Riesgo elevado. Priorizar reduccion de diferencias, automatizar conciliacion y acelerar settlement."
        else:
            nivel = "Alto"
            color_nivel = "#DE350B"
            interpretacion = "Riesgo critico. Accion inmediata: investigar diferencias no conciliadas, escalar con redes y revisar procesos."

        st.metric("Score de salud financiera", f"{score_pct:.1f}%")
        if score_pct >= 92:
            st.markdown(f"**Nivel de riesgo:** :green[**{nivel}**]")
        elif score_pct >= 80:
            st.markdown(f"**Nivel de riesgo:** :blue[**{nivel}**]")
        elif score_pct >= 65:
            st.markdown(f"**Nivel de riesgo:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel de riesgo:** :red[**{nivel}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(score_pct),
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

    if "comp_escenarios" not in st.session_state:
        st.session_state.comp_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="comp_save"):
        if len(st.session_state.comp_escenarios) >= 3:
            st.session_state.comp_escenarios.pop(0)
        st.session_state.comp_escenarios.append({
            "Tasa conc %": sim_tasa_conc,
            "Horas proc": sim_horas,
            "% Ajuste": sim_pct_ajuste,
            "Items exc": sim_excepciones,
            "% Completa": sim_pct_completa,
            "% T+0": sim_mix_ciclo,
            "Score salud": f"{score_pct:.1f}%",
            "Nivel riesgo": nivel,
        })
        st.rerun()

    if st.session_state.comp_escenarios:
        esc_df = pd.DataFrame(st.session_state.comp_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="comp_clear"):
            st.session_state.comp_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
