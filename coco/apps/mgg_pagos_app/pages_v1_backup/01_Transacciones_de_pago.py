import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.PROCESAMIENTO_TRANSACCIONES.TRANSACCIONES_PAGO"

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
    canales = run_query(f"SELECT DISTINCT CANAL FROM {TABLE} ORDER BY 1")["CANAL"].tolist()
    redes = run_query(f"SELECT DISTINCT RED_TARJETA FROM {TABLE} ORDER BY 1")["RED_TARJETA"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO FROM {TABLE} ORDER BY 1")["ESTADO"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    tipos_tx = run_query(f"SELECT DISTINCT TIPO_TRANSACCION FROM {TABLE} ORDER BY 1")["TIPO_TRANSACCION"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return canales, redes, estados, ciudades, tipos_tx, fechas


def build_where(fecha_ini, fecha_f, canal_s, red_s, estado_s, ciudad_s, tipo_tx_s,
                all_canales, all_redes, all_estados, all_ciudades, all_tipos):
    clauses = [f"FECHA_HORA::DATE BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if canal_s and canal_s != "Todos" and canal_s in all_canales:
        clauses.append(f"CANAL = '{canal_s}'")
    if red_s and red_s != "Todos" and red_s in all_redes:
        clauses.append(f"RED_TARJETA = '{red_s}'")
    if estado_s and estado_s != "Todos" and estado_s in all_estados:
        clauses.append(f"ESTADO = '{estado_s}'")
    if ciudad_s and ciudad_s != "Todos" and ciudad_s in all_ciudades:
        clauses.append(f"CIUDAD = '{ciudad_s}'")
    if tipo_tx_s and tipo_tx_s != "Todos" and tipo_tx_s in all_tipos:
        clauses.append(f"TIPO_TRANSACCION = '{tipo_tx_s}'")
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


st.header(":material/receipt_long: Transacciones de pago")
st.caption("Registro maestro de cada transaccion procesada por el switch de pagos. Monto, comercio, MCC, canal, tarjeta, banco emisor, red y estado.")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Visibilidad completa sobre cada transaccion que pasa por el switch, con todos sus atributos para analisis, conciliacion y deteccion de anomalias.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Cada transaccion se registra con monto, comercio, MCC, canal (POS, e-commerce, QR), tarjeta enmascarada, emisor, red y resultado. Se segmenta por tipo y estado.")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Medir la salud del ecosistema de pagos, identificar patrones de uso por canal y region, y alimentar todos los reportes del negocio con datos transaccionales confiables.")

    canales, redes, estados, ciudades, tipos_tx, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_HORA::DATE BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL_TX,
            SUM(CASE WHEN ESTADO='Aprobada' THEN 1 ELSE 0 END) AS APROBADAS,
            SUM(CASE WHEN ESTADO='Rechazada' THEN 1 ELSE 0 END) AS RECHAZADAS,
            ROUND(SUM(MONTO_COP)/1e9, 2) AS MONTO_B,
            ROUND(AVG(LATENCIA_TOTAL_MS), 0) AS LATENCIA_PROM,
            ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE_FRAUDE_PROM,
            SUM(CASE WHEN CONTACTLESS THEN 1 ELSE 0 END) AS CONTACTLESS_N,
            SUM(CASE WHEN TOKENIZADA THEN 1 ELSE 0 END) AS TOKENIZADAS_N
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL_TX"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL_TX"])
        dx_aprob = int(dx["APROBADAS"])
        dx_tasa_aprob = round(dx_aprob / dx_total * 100, 1)
        dx_rechazadas = int(dx["RECHAZADAS"])
        dx_tasa_rech = round(dx_rechazadas / dx_total * 100, 1)

        dx_worst_canal = run_query(f"""
            SELECT CANAL, ROUND(SUM(CASE WHEN ESTADO='Rechazada' THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) AS TASA_RECH
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_top_ciudad = run_query(f"""
            SELECT CIUDAD, ROUND(SUM(MONTO_COP)/1e9, 2) AS MONTO_B
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)

        if dx_tasa_aprob >= 90:
            dx_estado = f"Tasa de aprobacion ({color_tag(dx_tasa_aprob, 90, 80)}) :green[**saludable**]. Procesando ${float(dx['MONTO_B']):.1f}B COP."
        elif dx_tasa_aprob >= 80:
            dx_estado = f"Tasa de aprobacion ({color_tag(dx_tasa_aprob, 90, 80)}) en :orange[**zona de atencion**]. Rechazos: {dx_tasa_rech:.1f}%."
        else:
            dx_estado = f"Tasa de aprobacion ({color_tag(dx_tasa_aprob, 90, 80)}) :red[**critica**]. Requiere revision inmediata."

        dx_lines = [dx_estado]
        if not dx_worst_canal.empty:
            wc = dx_worst_canal.iloc[0]
            dx_lines.append(f"- Canal mas rechazos: **{wc['CANAL']}** ({color_tag(float(wc['TASA_RECH']), 5, 15, fmt='{:.1f}%', inverse=True)})")
        if not dx_top_ciudad.empty:
            tc = dx_top_ciudad.iloc[0]
            dx_lines.append(f"- Ciudad mayor volumen: **{tc['CIUDAD']}** (${float(tc['MONTO_B']):.2f}B COP)")
        dx_contactless_pct = round(int(dx["CONTACTLESS_N"]) / dx_total * 100, 1)
        dx_lines.append(f"- Contactless: **{dx_contactless_pct:.1f}%** — {':green[**adopcion alta**]' if dx_contactless_pct > 30 else ':orange[**oportunidad de crecimiento**]'}")
        dx_lines.append(f"- Latencia promedio: **{float(dx['LATENCIA_PROM']):.0f} ms** {'— :green[**dentro de SLA**]' if float(dx['LATENCIA_PROM']) < 500 else '— :red[**excede SLA**]'}")

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
                    recs.append("1. :red[**Revisar motivos de rechazo**]: tasa de aprobacion por debajo del objetivo.")
                if dx_tasa_rech > 15:
                    recs.append("2. :orange[**Analizar rechazos por red**]: alta tasa de declinacion puede indicar problemas de conectividad.")
                if dx_contactless_pct < 20:
                    recs.append("3. :orange[**Impulsar contactless**]: baja adopcion limita experiencia de pago rapido.")
                if float(dx["LATENCIA_PROM"]) > 500:
                    recs.append("4. :red[**Optimizar latencia**]: SLA de 500ms excedido, impacta conversion.")
                if not recs:
                    recs.append(":green[**Ecosistema transaccional operando dentro de parametros optimos.**] Mantener monitoreo continuo.")
                st.markdown("\n".join(recs))

    st.divider()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="tx_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        canal_sel = st.selectbox("Canal", ["Todos"] + canales, key="tx_canal")
    with fc3:
        red_sel = st.selectbox("Red", ["Todos"] + redes, key="tx_red")
    with fc4:
        estado_sel = st.selectbox("Estado", ["Todos"] + estados, key="tx_estado")
    with fc5:
        ciudad_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="tx_ciudad")

    tipo_tx_sel = "Todos"

    WHERE = build_where(fecha_inicio, fecha_fin, canal_sel, red_sel, estado_sel, ciudad_sel, tipo_tx_sel,
                        canales, redes, estados, ciudades, tipos_tx)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL_TX,
            SUM(CASE WHEN ESTADO='Aprobada' THEN 1 ELSE 0 END) AS APROBADAS,
            SUM(CASE WHEN ESTADO='Rechazada' THEN 1 ELSE 0 END) AS RECHAZADAS,
            SUM(CASE WHEN ESTADO='Reversada' THEN 1 ELSE 0 END) AS REVERSADAS,
            ROUND(SUM(MONTO_COP)/1e9, 2) AS MONTO_B,
            ROUND(AVG(MONTO_COP), 0) AS TICKET_PROM,
            ROUND(AVG(LATENCIA_TOTAL_MS), 0) AS LATENCIA_PROM,
            ROUND(AVG(SCORE_FRAUDE)*100, 1) AS SCORE_FRAUDE_PROM
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL_TX"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL_TX"])
        aprobadas = int(r["APROBADAS"])
        rechazadas = int(r["RECHAZADAS"])
        reversadas = int(r["REVERSADAS"])
        tasa_aprob = round(aprobadas / total * 100, 1)

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Transacciones", f"{total:,}")
        k2.metric("Tasa aprobacion", f"{tasa_aprob:.1f}%")
        k3.metric("Monto procesado", f"${float(r['MONTO_B']):.2f}B COP")
        k4.metric("Ticket promedio", f"${float(r['TICKET_PROM']):,.0f} COP")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Aprobadas", f"{aprobadas:,}")
        k6.metric("Rechazadas", f"{rechazadas:,}", delta=f"{round(rechazadas/total*100,1):.1f}%", delta_color="inverse")
        k7.metric("Reversadas", f"{reversadas:,}")
        k8.metric("Latencia prom", f"{float(r['LATENCIA_PROM']):.0f} ms")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES,
                   COUNT(*) AS TOTAL,
                   SUM(CASE WHEN ESTADO='Aprobada' THEN 1 ELSE 0 END) AS APROBADAS,
                   ROUND(SUM(MONTO_COP)/1e9, 2) AS MONTO_B
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Volumen y monto**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["TOTAL"].tolist(), name="Transacciones",
                    fill="tozeroy", line=dict(color=COLORS[0], width=2),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["MONTO_B"].tolist(), name="Monto (B COP)",
                    yaxis="y2", line=dict(color=COLORS[1], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="Transacciones"),
                    yaxis2=dict(title="Monto (B COP)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Volumen por canal y red**")
            tree_df = run_query(f"""
                SELECT CANAL, RED_TARJETA, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
            """)
            if tree_df.empty:
                st.info("Sin datos.")
            else:
                labels, parents, values, colors_t = [], [], [], []
                canal_totals = tree_df.groupby("CANAL")["N"].sum().sort_values(ascending=False)
                for i, (canal, canal_total) in enumerate(canal_totals.items()):
                    labels.append(canal)
                    parents.append("")
                    values.append(int(canal_total))
                    colors_t.append(COLORS[i % len(COLORS)])
                    sub = tree_df[tree_df["CANAL"] == canal]
                    for _, row in sub.iterrows():
                        labels.append(str(row["RED_TARJETA"]))
                        parents.append(canal)
                        values.append(int(row["N"]))
                        colors_t.append(COLORS[i % len(COLORS)])
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    marker=dict(colors=colors_t),
                    textinfo="label+value+percent parent",
                    hovertemplate="<b>%{label}</b><br>Transacciones: %{value:,}<br>%{percentParent:.1%} del padre<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("**Heatmap — Monto promedio por canal y estado**")
        heat_df = run_query(f"""
            SELECT CANAL, ESTADO, ROUND(AVG(MONTO_COP)/1000, 1) AS MONTO_K
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="CANAL", columns="ESTADO", values="MONTO_K", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E3F2FD"], [0.3, "#29B5E8"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"${v:.0f}K" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Canal: %{y}<br>Estado: %{x}<br>Monto prom: $%{z:.1f}K COP<extra></extra>",
                colorbar=dict(title="$K COP"),
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
            st.markdown("**Funnel — Estado de transacciones**")
            funnel_data = [
                ("Total", total),
                ("Aprobadas", aprobadas),
                ("Rechazadas", rechazadas),
                ("Reversadas", reversadas),
            ]
            fig_funnel = go.Figure(go.Funnel(
                y=[f[0] for f in funnel_data],
                x=[f[1] for f in funnel_data],
                textinfo="value+percent initial",
                marker=dict(color=[COLORS[0], COLORS[2], COLORS[1], COLORS[4]]),
                hovertemplate="%{y}: %{x:,}<extra></extra>",
            ))
            fig_funnel.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=10, r=10, t=30, b=10),
            )
            st.plotly_chart(fig_funnel, use_container_width=True)

        with col4:
            st.markdown("**Distribucion por tipo de transaccion**")
            tipo_df = run_query(f"""
                SELECT TIPO_TRANSACCION, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if tipo_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=tipo_df["TIPO_TRANSACCION"].tolist(),
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

        st.markdown("**Mapa — Volumen transaccional por ciudad**")
        map_df = run_query(f"""
            SELECT CIUDAD, COUNT(*) AS N, ROUND(SUM(MONTO_COP)/1e6, 1) AS MONTO_M
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC
        """)
        if not map_df.empty:
            import folium
            from folium import plugins
            import branca.colormap as cm

            map_data = []
            for _, row in map_df.iterrows():
                city = str(row["CIUDAD"])
                if city in COORDS_CIUDAD:
                    lat, lon = COORDS_CIUDAD[city]
                    map_data.append({"lat": lat, "lon": lon, "city": city, "n": int(row["N"]), "monto_m": float(row["MONTO_M"])})
            if map_data:
                pdf = pd.DataFrame(map_data)
                max_n = pdf["n"].max()
                min_n = pdf["n"].min()
                colormap = cm.LinearColormap(
                    colors=["#E3F2FD", "#29B5E8", "#11567F"],
                    vmin=min_n, vmax=max_n,
                    caption="Transacciones",
                )
                m = folium.Map(location=[5.0, -74.0], zoom_start=6, tiles="CartoDB positron")
                for _, r in pdf.iterrows():
                    radius = max(8, (r["n"] / max_n) * 35)
                    folium.CircleMarker(
                        location=[r["lat"], r["lon"]],
                        radius=radius,
                        color="#11567F",
                        fill=True,
                        fill_color=colormap(r["n"]),
                        fill_opacity=0.8,
                        weight=1.5,
                        popup=folium.Popup(f"<b>{r['city']}</b><br>Txns: {r['n']:,}<br>${r['monto_m']:.1f}M COP", max_width=200),
                        tooltip=f"{r['city']}: {r['n']:,} txns",
                    ).add_to(m)
                    folium.Marker(
                        location=[r["lat"] + 0.15, r["lon"]],
                        icon=folium.DivIcon(
                            html=f'<div style="font-size:11px;font-weight:bold;color:#11567F;white-space:nowrap;text-shadow:1px 1px 2px white,-1px -1px 2px white,1px -1px 2px white,-1px 1px 2px white;">{r["city"]}</div>',
                            icon_size=(100, 20),
                            icon_anchor=(50, 10),
                        ),
                    ).add_to(m)
                colormap.add_to(m)
                st.components.v1.html(m._repr_html_(), height=500)

        st.markdown("**Top 10 bancos emisores por volumen**")
        banco_df = run_query(f"""
            SELECT BANCO_EMISOR,
                   COUNT(*) AS TOTAL,
                   SUM(CASE WHEN ESTADO='Aprobada' THEN 1 ELSE 0 END) AS APROBADAS,
                   ROUND(SUM(MONTO_COP)/1e6, 1) AS MONTO_M
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """)
        if not banco_df.empty:
            banco_df["TASA_APROB"] = (banco_df["APROBADAS"] / banco_df["TOTAL"] * 100).round(1)
            st.dataframe(banco_df[["BANCO_EMISOR", "TOTAL", "APROBADAS", "TASA_APROB", "MONTO_M"]].rename(columns={
                "BANCO_EMISOR": "Banco", "TOTAL": "Transacciones", "APROBADAS": "Aprobadas",
                "TASA_APROB": "% Aprobacion", "MONTO_M": "Monto ($M COP)"
            }), use_container_width=True, hide_index=True)

with tab2:
    st.markdown("**Simulador de volumen transaccional**")
    st.caption("Estime la tasa de aprobacion y el monto procesado ajustando las variables clave del ecosistema de pagos. El modelo pondera cada factor segun su impacto relativo.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_monto = st.slider("Monto promedio de transaccion (miles COP)", 10, 5000, 500, step=50, key="tx_sim_monto",
                              help="Transacciones de menor monto tienden a tener mayor aprobacion")
        sim_pct_ecommerce = st.slider("% Transacciones e-commerce / CNP", 0, 100, 35, key="tx_sim_ecom",
                                      help="Card-not-present tiene mas friccion y rechazo por fraude")
        sim_pct_contactless = st.slider("% Transacciones contactless", 0, 100, 40, key="tx_sim_ctls",
                                        help="Contactless tiene mayor tasa de aprobacion por menor friccion")
        sim_pct_internacional = st.slider("% Transacciones internacionales", 0, 100, 15, key="tx_sim_intl",
                                          help="Internacionales enfrentan mas validaciones del emisor")
        sim_score_fraude = st.slider("Score promedio de fraude (0=bajo, 100=alto)", 0, 100, 20, key="tx_sim_fraude",
                                     help="Mayor score de fraude reduce aprobacion por reglas del emisor")
        sim_latencia = st.slider("Latencia esperada (ms)", 50, 2000, 350, step=50, key="tx_sim_lat",
                                 help="Latencia alta puede causar timeouts y rechazos tecnicos")

    def calcular_tasa_tx(monto, ecom, ctls, intl, fraude, lat):
        if monto < 50:
            s_monto = 0.3
        elif monto < 200:
            s_monto = 0.6
        elif monto < 1000:
            s_monto = 1.0
        elif monto < 3000:
            s_monto = 0.7
        else:
            s_monto = 0.3
        s_canal = 1.0 - (ecom / 100) * 0.8
        s_ctls = 0.5 + (ctls / 100) * 0.5
        s_intl = 1.0 - (intl / 100) * 0.9
        s_fraude = 1.0 - (fraude / 100) * 1.0
        s_lat = max(0.0, 1.0 - (lat / 2000) * 0.8)
        tasa = (0.15 * s_monto + 0.20 * s_canal + 0.12 * s_ctls + 0.15 * s_intl + 0.22 * s_fraude + 0.16 * s_lat)
        tasa_final = 0.50 + tasa * 0.48
        return round(min(max(tasa_final, 0.50), 0.98), 3)

    tasa_pred = calcular_tasa_tx(sim_monto, sim_pct_ecommerce, sim_pct_contactless, sim_pct_internacional, sim_score_fraude, sim_latencia)
    tasa_pct = tasa_pred * 100

    with col_result:
        if tasa_pct >= 92:
            nivel = "Alta"
            color_nivel = "#36B37E"
            interpretacion = "Excelente tasa de aprobacion. El perfil transaccional esta optimizado. Mantener las condiciones actuales y monitorear cambios en reglas de emisores."
        elif tasa_pct >= 80:
            nivel = "Media-Alta"
            color_nivel = "#29B5E8"
            interpretacion = "Tasa aceptable pero con espacio de mejora. Revisar segmentos con mayor rechazo (canal CNP o transacciones internacionales)."
        elif tasa_pct >= 65:
            nivel = "Media"
            color_nivel = "#FFAB00"
            interpretacion = "Tasa por debajo del benchmark. Priorizar negociacion con emisores en segmentos de mayor volumen y revisar reglas de fraude."
        else:
            nivel = "Baja"
            color_nivel = "#DE350B"
            interpretacion = "Tasa critica. Accion inmediata: revisar parametros de fraude, negociar con emisores, optimizar canales CNP y reducir friccion internacional."

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

    if "tx_escenarios" not in st.session_state:
        st.session_state.tx_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="tx_save"):
        if len(st.session_state.tx_escenarios) >= 3:
            st.session_state.tx_escenarios.pop(0)
        st.session_state.tx_escenarios.append({
            "Monto prom (K COP)": sim_monto,
            "% E-commerce": sim_pct_ecommerce,
            "% Contactless": sim_pct_contactless,
            "% Internacional": sim_pct_internacional,
            "Score fraude": sim_score_fraude,
            "Latencia (ms)": sim_latencia,
            "Tasa estimada": f"{tasa_pct:.1f}%",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.tx_escenarios:
        esc_df = pd.DataFrame(st.session_state.tx_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="tx_clear"):
            st.session_state.tx_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
