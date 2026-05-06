import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.COMERCIOS_Y_ADQUIRENCIA.TERMINALES_POS"

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
    modelos = run_query(f"SELECT DISTINCT MODELO FROM {TABLE} ORDER BY 1")["MODELO"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO FROM {TABLE} ORDER BY 1")["ESTADO"].tolist()
    conectividades = run_query(f"SELECT DISTINCT TIPO_CONECTIVIDAD FROM {TABLE} ORDER BY 1")["TIPO_CONECTIVIDAD"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    modelos_com = run_query(f"SELECT DISTINCT MODELO_COMERCIAL FROM {TABLE} ORDER BY 1")["MODELO_COMERCIAL"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_INSTALACION) AS FMIN, MAX(FECHA_INSTALACION) AS FMAX FROM {TABLE}")
    return modelos, estados, conectividades, ciudades, modelos_com, fechas


def build_where(fecha_ini, fecha_f, mod_s, est_s, con_s, ciu_s, mcom_s,
                all_mod, all_est, all_con, all_ciu, all_mcom):
    clauses = [f"FECHA_INSTALACION BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if mod_s and mod_s != "Todos" and mod_s in all_mod:
        clauses.append(f"MODELO = '{mod_s}'")
    if est_s and est_s != "Todos" and est_s in all_est:
        clauses.append(f"ESTADO = '{est_s}'")
    if con_s and con_s != "Todos" and con_s in all_con:
        clauses.append(f"TIPO_CONECTIVIDAD = '{con_s}'")
    if ciu_s and ciu_s != "Todos" and ciu_s in all_ciu:
        clauses.append(f"CIUDAD = '{ciu_s}'")
    if mcom_s and mcom_s != "Todos" and mcom_s in all_mcom:
        clauses.append(f"MODELO_COMERCIAL = '{mcom_s}'")
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


st.header(":material/point_of_sale: Terminales POS")
st.caption("Inventario y estado de terminales POS. Modelo, conectividad, software, claves, bateria y actividad transaccional.")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Falta de control sobre parque de terminales: dispositivos inactivos, software desactualizado, claves vencidas.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Cada terminal se monitorea con modelo, conectividad, version software, estado claves, bateria y ultima transaccion.")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Reducir costo mantenimiento, evitar caidas por terminales desactualizadas, maximizar utilizacion del parque.")

    modelos, estados, conectividades, ciudades, modelos_com, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_INSTALACION BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(CASE WHEN ESTADO='Activa' THEN 1 ELSE 0 END) AS ACTIVAS,
            ROUND(SUM(CASE WHEN CONTACTLESS_HABILITADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_CTLS,
            ROUND(SUM(CASE WHEN QR_HABILITADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_QR,
            ROUND(AVG(TRANSACCIONES_MES), 0) AS TXN_PROM,
            ROUND(AVG(ERRORES_MES), 1) AS ERR_PROM,
            ROUND(SUM(CASE WHEN REQUIERE_ACTUALIZACION THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_ACT,
            ROUND(SUM(CASE WHEN KEYS_INJECTED THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_KEYS
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL"])
        dx_activas = int(dx["ACTIVAS"])
        dx_pct_activas = round(dx_activas / dx_total * 100, 1)
        dx_pct_ctls = float(dx["PCT_CTLS"])
        dx_pct_act = float(dx["PCT_ACT"])
        dx_pct_keys = float(dx["PCT_KEYS"])

        dx_worst_modelo = run_query(f"""
            SELECT MODELO, ROUND(AVG(ERRORES_MES), 1) AS ERR_PROM
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_top_ciudad = run_query(f"""
            SELECT CIUDAD, COUNT(*) AS N
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)

        if dx_pct_activas >= 85:
            dx_estado = f"Parque de terminales ({color_tag(dx_pct_activas, 85, 70)}) activas) :green[**saludable**]. {dx_total:,} terminales en inventario."
        elif dx_pct_activas >= 70:
            dx_estado = f"Parque de terminales ({color_tag(dx_pct_activas, 85, 70)}) activas) en :orange[**zona de atencion**]. Revisar inactivas."
        else:
            dx_estado = f"Parque de terminales ({color_tag(dx_pct_activas, 85, 70)}) activas) :red[**critico**]. Alta proporcion de terminales inactivas."

        dx_lines = [dx_estado]
        if not dx_worst_modelo.empty:
            wm = dx_worst_modelo.iloc[0]
            dx_lines.append(f"- Modelo con mas errores: **{wm['MODELO']}** ({float(wm['ERR_PROM']):.1f} errores/mes)")
        if not dx_top_ciudad.empty:
            tc = dx_top_ciudad.iloc[0]
            dx_lines.append(f"- Ciudad mayor concentracion: **{tc['CIUDAD']}** ({int(tc['N']):,} terminales)")
        dx_lines.append(f"- Contactless habilitado: **{dx_pct_ctls:.1f}%** — {':green[**buena cobertura**]' if dx_pct_ctls > 60 else ':orange[**oportunidad de mejora**]'}")
        dx_lines.append(f"- Requieren actualizacion: **{dx_pct_act:.1f}%** {'— :green[**bajo**]' if dx_pct_act < 20 else '— :red[**requiere plan de actualizacion**]'}")

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
                if dx_pct_activas < 80:
                    recs.append("1. :red[**Activar terminales inactivas**]: alto porcentaje sin uso genera costo sin retorno.")
                if dx_pct_act > 30:
                    recs.append("2. :red[**Actualizar software masivamente**]: alto porcentaje desactualizado genera vulnerabilidades.")
                if dx_pct_ctls < 50:
                    recs.append("3. :orange[**Habilitar contactless**]: baja penetracion limita experiencia de pago.")
                if dx_pct_keys < 80:
                    recs.append("4. :orange[**Inyeccion de claves pendiente**]: terminales sin keys no pueden operar de forma segura.")
                if not recs:
                    recs.append(":green[**Parque de terminales operando dentro de parametros optimos.**] Mantener monitoreo continuo.")
                st.markdown("\n".join(recs))

    st.divider()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="term_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        mod_sel = st.selectbox("Modelo", ["Todos"] + modelos, key="term_mod")
    with fc3:
        est_sel = st.selectbox("Estado", ["Todos"] + estados, key="term_est")
    with fc4:
        con_sel = st.selectbox("Conectividad", ["Todos"] + conectividades, key="term_con")
    with fc5:
        ciu_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="term_ciu")

    mcom_sel = "Todos"

    WHERE = build_where(fecha_inicio, fecha_fin, mod_sel, est_sel, con_sel, ciu_sel, mcom_sel,
                        modelos, estados, conectividades, ciudades, modelos_com)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(CASE WHEN ESTADO='Activa' THEN 1 ELSE 0 END) AS ACTIVAS,
            ROUND(SUM(CASE WHEN CONTACTLESS_HABILITADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_CTLS,
            ROUND(SUM(CASE WHEN QR_HABILITADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_QR,
            ROUND(AVG(TRANSACCIONES_MES), 0) AS TXN_PROM,
            ROUND(AVG(ERRORES_MES), 1) AS ERR_PROM,
            ROUND(SUM(CASE WHEN REQUIERE_ACTUALIZACION THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_ACT,
            ROUND(SUM(CASE WHEN KEYS_INJECTED THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_KEYS
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL"])
        activas = int(r["ACTIVAS"])
        pct_ctls = float(r["PCT_CTLS"])
        pct_qr = float(r["PCT_QR"])
        txn_prom = float(r["TXN_PROM"])
        err_prom = float(r["ERR_PROM"])
        pct_act = float(r["PCT_ACT"])
        pct_keys = float(r["PCT_KEYS"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total terminales", f"{total:,}")
        k2.metric("Activas", f"{activas:,}", delta=f"{round(activas/total*100,1):.1f}%")
        k3.metric("% Contactless", f"{pct_ctls:.1f}%")
        k4.metric("% QR habilitado", f"{pct_qr:.1f}%")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Txns prom/mes", f"{txn_prom:,.0f}")
        k6.metric("Errores prom/mes", f"{err_prom:.1f}", delta=f"{err_prom:.1f}", delta_color="inverse")
        k7.metric("% Requieren actualizacion", f"{pct_act:.1f}%", delta=f"{pct_act:.1f}%", delta_color="inverse")
        k8.metric("% Keys injected", f"{pct_keys:.1f}%")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_INSTALACION) AS MES,
                   COUNT(*) AS INSTALADAS,
                   ROUND(SUM(VOLUMEN_MENSUAL_COP)/1e9, 2) AS VOL_B
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Terminales instaladas y volumen**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["INSTALADAS"].tolist(), name="Terminales instaladas",
                    fill="tozeroy", line=dict(color=COLORS[0], width=2),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["VOL_B"].tolist(), name="Volumen (B COP)",
                    yaxis="y2", line=dict(color=COLORS[1], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="Terminales instaladas"),
                    yaxis2=dict(title="Volumen (B COP)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Modelo y conectividad**")
            tree_df = run_query(f"""
                SELECT MODELO, TIPO_CONECTIVIDAD, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
            """)
            if tree_df.empty:
                st.info("Sin datos.")
            else:
                labels, parents, values, colors_t = [], [], [], []
                mod_totals = tree_df.groupby("MODELO")["N"].sum().sort_values(ascending=False)
                for i, (mod, mod_total) in enumerate(mod_totals.items()):
                    labels.append(mod)
                    parents.append("")
                    values.append(int(mod_total))
                    colors_t.append(COLORS[i % len(COLORS)])
                    sub = tree_df[tree_df["MODELO"] == mod]
                    for _, row in sub.iterrows():
                        labels.append(str(row["TIPO_CONECTIVIDAD"]))
                        parents.append(mod)
                        values.append(int(row["N"]))
                        colors_t.append(COLORS[i % len(COLORS)])
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    marker=dict(colors=colors_t),
                    textinfo="label+value+percent parent",
                    hovertemplate="<b>%{label}</b><br>Terminales: %{value:,}<br>%{percentParent:.1%} del padre<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("**Heatmap — Terminales por modelo y estado**")
        heat_df = run_query(f"""
            SELECT MODELO, ESTADO, COUNT(*) AS N
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="MODELO", columns="ESTADO", values="N", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E3F2FD"], [0.3, "#29B5E8"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"{int(v)}" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Modelo: %{y}<br>Estado: %{x}<br>Terminales: %{z:,}<extra></extra>",
                colorbar=dict(title="Terminales"),
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
            st.markdown("**Funnel — Estado del parque**")
            ctls_n = int(run_query(f"SELECT SUM(CASE WHEN CONTACTLESS_HABILITADO THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")["N"].iloc[0])
            qr_n = int(run_query(f"SELECT SUM(CASE WHEN QR_HABILITADO THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")["N"].iloc[0])
            keys_n = int(run_query(f"SELECT SUM(CASE WHEN KEYS_INJECTED THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")["N"].iloc[0])
            funnel_data = [
                ("Total terminales", total),
                ("Activas", activas),
                ("Contactless", ctls_n),
                ("QR habilitado", qr_n),
                ("Keys OK", keys_n),
            ]
            fig_funnel = go.Figure(go.Funnel(
                y=[f[0] for f in funnel_data],
                x=[f[1] for f in funnel_data],
                textinfo="value+percent initial",
                marker=dict(color=[COLORS[0], COLORS[2], COLORS[1], COLORS[3], COLORS[5]]),
                hovertemplate="%{y}: %{x:,}<extra></extra>",
            ))
            fig_funnel.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=10, r=10, t=30, b=10),
            )
            st.plotly_chart(fig_funnel, use_container_width=True)

        with col4:
            st.markdown("**Distribucion por modelo comercial**")
            mcom_df = run_query(f"""
                SELECT MODELO_COMERCIAL, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if mcom_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=mcom_df["MODELO_COMERCIAL"].tolist(),
                    values=mcom_df["N"].tolist(),
                    hole=0.5,
                    marker=dict(colors=COLORS[:len(mcom_df)]),
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
                ))
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.markdown("**Mapa — Terminales por ciudad**")
        map_df = run_query(f"""
            SELECT CIUDAD, COUNT(*) AS N, ROUND(SUM(VOLUMEN_MENSUAL_COP)/1e6, 1) AS VOL_M
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
                    map_data.append({"lat": lat, "lon": lon, "city": city, "n": int(row["N"]), "vol_m": float(row["VOL_M"])})
            if map_data:
                pdf = pd.DataFrame(map_data)
                max_n = pdf["n"].max()
                min_n = pdf["n"].min()
                colormap = cm.LinearColormap(
                    colors=["#E3F2FD", "#29B5E8", "#11567F"],
                    vmin=min_n, vmax=max_n,
                    caption="Terminales",
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
                        popup=folium.Popup(f"<b>{r['city']}</b><br>Terminales: {r['n']:,}<br>${r['vol_m']:.1f}M COP", max_width=200),
                        tooltip=f"{r['city']}: {r['n']:,} terminales",
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

        st.markdown("**Top 10 terminales por volumen mensual**")
        top_df = run_query(f"""
            SELECT SERIAL_TERMINAL, MODELO, ESTADO, CIUDAD,
                   VOLUMEN_MENSUAL_COP, TRANSACCIONES_MES,
                   ROUND(TASA_APROBACION_TERMINAL*100, 1) AS TASA_APROB,
                   ERRORES_MES
            FROM {TABLE} WHERE {WHERE}
            ORDER BY VOLUMEN_MENSUAL_COP DESC LIMIT 10
        """)
        if not top_df.empty:
            top_df["VOLUMEN_MENSUAL_COP"] = (top_df["VOLUMEN_MENSUAL_COP"] / 1e6).round(1)
            st.dataframe(top_df.rename(columns={
                "SERIAL_TERMINAL": "Serial", "MODELO": "Modelo", "ESTADO": "Estado",
                "CIUDAD": "Ciudad", "VOLUMEN_MENSUAL_COP": "Volumen ($M COP)", "TRANSACCIONES_MES": "Txns/mes",
                "TASA_APROB": "% Aprobacion", "ERRORES_MES": "Errores/mes"
            }), use_container_width=True, hide_index=True)

with tab2:
    st.markdown("**Simulador de salud operativa del parque**")
    st.caption("Estime el score de salud operativa ajustando las variables clave del parque de terminales. El modelo pondera cada factor segun su impacto.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_ctls = st.slider("% Terminales con contactless", 0, 100, 60, key="term_sim_ctls",
                             help="Contactless mejora experiencia y reduce tiempo de transaccion")
        sim_qr = st.slider("% Terminales con QR", 0, 100, 40, key="term_sim_qr",
                           help="QR habilita pagos sin tarjeta y wallet digital")
        sim_aprob = st.slider("Tasa de aprobacion promedio (%)", 50, 100, 90, key="term_sim_aprob",
                              help="Mayor aprobacion indica mejor operacion del terminal")
        sim_errores = st.slider("Errores promedio por mes", 0, 50, 5, key="term_sim_err",
                                help="Menos errores indica mejor salud del dispositivo")
        sim_bateria = st.slider("Bateria promedio (%)", 0, 100, 70, key="term_sim_bat",
                                help="Terminales con baja bateria pueden fallar en campo")
        sim_mant = st.slider("Dias sin mantenimiento", 0, 365, 60, key="term_sim_mant",
                             help="Mantenimiento regular previene fallas y mejora vida util")

    def calcular_salud(ctls, qr, aprob, errores, bateria, mant):
        s_ctls = ctls / 100
        s_qr = qr / 100
        s_aprob = (aprob - 50) / 50
        s_err = max(0.0, 1.0 - errores / 50)
        s_bat = bateria / 100
        s_mant = max(0.0, 1.0 - mant / 365)
        score = (0.15 * s_ctls + 0.10 * s_qr + 0.25 * s_aprob + 0.20 * s_err + 0.15 * s_bat + 0.15 * s_mant)
        health = 0.30 + score * 0.65
        return round(min(max(health, 0.30), 0.95), 3)

    salud_pred = calcular_salud(sim_ctls, sim_qr, sim_aprob, sim_errores, sim_bateria, sim_mant)
    salud_pct = salud_pred * 100

    with col_result:
        if salud_pct >= 80:
            nivel = "Optima"
            color_nivel = "#36B37E"
            interpretacion = "Parque en excelente estado operativo. Continuar con plan de mantenimiento preventivo y expansion de funcionalidades."
        elif salud_pct >= 65:
            nivel = "Buena"
            color_nivel = "#29B5E8"
            interpretacion = "Salud aceptable con oportunidades de mejora. Priorizar actualizaciones y mantenimiento preventivo en segmentos criticos."
        elif salud_pct >= 50:
            nivel = "Regular"
            color_nivel = "#FFAB00"
            interpretacion = "Salud por debajo del benchmark. Revisar terminales con altos errores, baja bateria y software desactualizado."
        else:
            nivel = "Critica"
            color_nivel = "#DE350B"
            interpretacion = "Estado critico. Accion inmediata: plan de mantenimiento masivo, actualizacion de software y reemplazo de terminales con fallas recurrentes."

        st.metric("Score de salud operativa", f"{salud_pct:.1f}%")
        if salud_pct >= 80:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif salud_pct >= 65:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif salud_pct >= 50:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(salud_pct),
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

    if "term_escenarios" not in st.session_state:
        st.session_state.term_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="term_save"):
        if len(st.session_state.term_escenarios) >= 3:
            st.session_state.term_escenarios.pop(0)
        st.session_state.term_escenarios.append({
            "% Contactless": sim_ctls,
            "% QR": sim_qr,
            "Tasa aprob (%)": sim_aprob,
            "Errores/mes": sim_errores,
            "Bateria (%)": sim_bateria,
            "Dias sin mant": sim_mant,
            "Salud estimada": f"{salud_pct:.1f}%",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.term_escenarios:
        esc_df = pd.DataFrame(st.session_state.term_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="term_clear"):
            st.session_state.term_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
