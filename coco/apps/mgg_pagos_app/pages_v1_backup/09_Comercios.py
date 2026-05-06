import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.COMERCIOS_Y_ADQUIRENCIA.COMERCIOS"

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
    adquirentes = run_query(f"SELECT DISTINCT ADQUIRENTE FROM {TABLE} ORDER BY 1")["ADQUIRENTE"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO FROM {TABLE} ORDER BY 1")["ESTADO"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    niveles = run_query(f"SELECT DISTINCT NIVEL_RIESGO FROM {TABLE} ORDER BY 1")["NIVEL_RIESGO"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_AFILIACION) AS FMIN, MAX(FECHA_AFILIACION) AS FMAX FROM {TABLE}")
    return segmentos, adquirentes, estados, ciudades, niveles, fechas


def build_where(fecha_ini, fecha_f, seg_s, adq_s, est_s, ciu_s, niv_s,
                all_seg, all_adq, all_est, all_ciu, all_niv):
    clauses = [f"FECHA_AFILIACION BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if seg_s and seg_s != "Todos" and seg_s in all_seg:
        clauses.append(f"SEGMENTO = '{seg_s}'")
    if adq_s and adq_s != "Todos" and adq_s in all_adq:
        clauses.append(f"ADQUIRENTE = '{adq_s}'")
    if est_s and est_s != "Todos" and est_s in all_est:
        clauses.append(f"ESTADO = '{est_s}'")
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


st.header(":material/store: Comercios")
st.caption("Maestro de comercios afiliados a la red de pagos. NIT, MCC, segmento, adquirente, terminales, redes aceptadas, tasa de aprobacion, fraude y contracargos.")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Falta de un repositorio centralizado con la informacion completa de cada comercio afiliado para gestion comercial, riesgo y pricing.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Cada comercio se registra con sus datos fiscales (NIT), categoria (MCC), segmento, adquirente responsable, terminales desplegadas, redes aceptadas y metricas clave de operacion.")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Gestionar la base de comercios con datos unificados para tomar decisiones de pricing diferenciado, asignacion de riesgo y acciones comerciales basadas en el comportamiento real.")

    segmentos, adquirentes, estados, ciudades, niveles, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_AFILIACION BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(CASE WHEN ESTADO='Activo' THEN 1 ELSE 0 END) AS ACTIVOS,
            SUM(CASE WHEN ECOMMERCE_ACTIVO THEN 1 ELSE 0 END) AS ECOMMERCE_N,
            SUM(TERMINALES_ACTIVAS) AS TERMINALES,
            ROUND(AVG(MDR_PCT), 2) AS MDR_PROM,
            ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA_APROB_PROM,
            ROUND(AVG(TASA_FRAUDE)*100, 2) AS TASA_FRAUDE_PROM,
            SUM(CASE WHEN PCI_CERTIFICADO THEN 1 ELSE 0 END) AS PCI_N
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL"])
        dx_activos = int(dx["ACTIVOS"])
        dx_pct_activos = round(dx_activos / dx_total * 100, 1)
        dx_pct_ecom = round(int(dx["ECOMMERCE_N"]) / dx_total * 100, 1)
        dx_tasa_aprob = float(dx["TASA_APROB_PROM"])
        dx_tasa_fraude = float(dx["TASA_FRAUDE_PROM"])
        dx_pci_pct = round(int(dx["PCI_N"]) / dx_total * 100, 1)

        dx_worst_seg = run_query(f"""
            SELECT SEGMENTO, ROUND(AVG(TASA_FRAUDE)*100, 2) AS TASA_FRAUDE
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_top_ciudad = run_query(f"""
            SELECT CIUDAD, ROUND(SUM(VOLUMEN_MENSUAL_COP)/1e9, 2) AS VOL_B
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)

        if dx_tasa_aprob >= 90:
            dx_estado = f"Tasa de aprobacion promedio ({color_tag(dx_tasa_aprob, 90, 80)}) :green[**saludable**]. Base de {dx_total:,} comercios."
        elif dx_tasa_aprob >= 80:
            dx_estado = f"Tasa de aprobacion promedio ({color_tag(dx_tasa_aprob, 90, 80)}) en :orange[**zona de atencion**]. Revisar segmentos con mayor rechazo."
        else:
            dx_estado = f"Tasa de aprobacion promedio ({color_tag(dx_tasa_aprob, 90, 80)}) :red[**critica**]. Requiere revision inmediata de la base."

        dx_lines = [dx_estado]
        if not dx_worst_seg.empty:
            ws = dx_worst_seg.iloc[0]
            dx_lines.append(f"- Segmento mayor fraude: **{ws['SEGMENTO']}** ({color_tag(float(ws['TASA_FRAUDE']), 0.5, 1.5, fmt='{:.2f}%', inverse=True)})")
        if not dx_top_ciudad.empty:
            tc = dx_top_ciudad.iloc[0]
            dx_lines.append(f"- Ciudad mayor volumen: **{tc['CIUDAD']}** (${float(tc['VOL_B']):.2f}B COP)")
        dx_lines.append(f"- E-commerce activo: **{dx_pct_ecom:.1f}%** — {':green[**buena adopcion**]' if dx_pct_ecom > 40 else ':orange[**oportunidad de crecimiento**]'}")
        dx_lines.append(f"- PCI certificados: **{dx_pci_pct:.1f}%** {'— :green[**buen nivel de cumplimiento**]' if dx_pci_pct > 60 else '— :red[**riesgo de cumplimiento**]'}")

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
                    recs.append("1. :red[**Revisar comercios con baja aprobacion**]: tasa promedio por debajo del objetivo.")
                if dx_tasa_fraude > 1.0:
                    recs.append("2. :red[**Activar monitoreo reforzado de fraude**]: tasa promedio elevada.")
                if dx_pct_ecom < 30:
                    recs.append("3. :orange[**Impulsar e-commerce**]: baja penetracion digital en la base de comercios.")
                if dx_pci_pct < 50:
                    recs.append("4. :red[**Campaña de certificacion PCI**]: alto porcentaje sin certificar.")
                if not recs:
                    recs.append(":green[**Base de comercios operando dentro de parametros optimos.**] Mantener monitoreo continuo.")
                st.markdown("\n".join(recs))

    st.divider()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="com_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        seg_sel = st.selectbox("Segmento", ["Todos"] + segmentos, key="com_seg")
    with fc3:
        adq_sel = st.selectbox("Adquirente", ["Todos"] + adquirentes, key="com_adq")
    with fc4:
        est_sel = st.selectbox("Estado", ["Todos"] + estados, key="com_est")
    with fc5:
        ciu_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="com_ciu")

    niv_sel = "Todos"

    WHERE = build_where(fecha_inicio, fecha_fin, seg_sel, adq_sel, est_sel, ciu_sel, niv_sel,
                        segmentos, adquirentes, estados, ciudades, [str(n) for n in niveles])

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(CASE WHEN ESTADO='Activo' THEN 1 ELSE 0 END) AS ACTIVOS,
            SUM(CASE WHEN ECOMMERCE_ACTIVO THEN 1 ELSE 0 END) AS ECOMMERCE_N,
            SUM(TERMINALES_ACTIVAS) AS TERMINALES,
            ROUND(AVG(MDR_PCT), 2) AS MDR_PROM,
            ROUND(AVG(TASA_APROBACION)*100, 1) AS TASA_APROB_PROM,
            ROUND(AVG(TASA_FRAUDE)*100, 2) AS TASA_FRAUDE_PROM,
            SUM(CASE WHEN PCI_CERTIFICADO THEN 1 ELSE 0 END) AS PCI_N
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL"])
        activos = int(r["ACTIVOS"])
        ecom_n = int(r["ECOMMERCE_N"])
        terminales = int(r["TERMINALES"])
        mdr_prom = float(r["MDR_PROM"])
        tasa_aprob = float(r["TASA_APROB_PROM"])
        tasa_fraude = float(r["TASA_FRAUDE_PROM"])
        pci_n = int(r["PCI_N"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total comercios", f"{total:,}")
        k2.metric("Activos", f"{activos:,}", delta=f"{round(activos/total*100,1):.1f}%")
        k3.metric("% con e-commerce", f"{round(ecom_n/total*100,1):.1f}%")
        k4.metric("Terminales desplegadas", f"{terminales:,}")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("MDR promedio", f"{mdr_prom:.2f}%")
        k6.metric("Tasa aprobacion prom", f"{tasa_aprob:.1f}%")
        k7.metric("Tasa fraude prom", f"{tasa_fraude:.2f}%", delta=f"{tasa_fraude:.2f}%", delta_color="inverse")
        k8.metric("PCI certificados", f"{pci_n:,}", delta=f"{round(pci_n/total*100,1):.1f}%")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_AFILIACION) AS MES,
                   SUM(CASE WHEN ESTADO='Activo' THEN 1 ELSE 0 END) AS ACTIVOS,
                   ROUND(SUM(VOLUMEN_MENSUAL_COP)/1e9, 2) AS VOL_B
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Comercios activos y volumen**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["ACTIVOS"].tolist(), name="Comercios activos",
                    fill="tozeroy", line=dict(color=COLORS[0], width=2),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["VOL_B"].tolist(), name="Volumen (B COP)",
                    yaxis="y2", line=dict(color=COLORS[1], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="Comercios activos"),
                    yaxis2=dict(title="Volumen (B COP)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Segmento y adquirente**")
            tree_df = run_query(f"""
                SELECT SEGMENTO, ADQUIRENTE, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
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
                    sub = tree_df[tree_df["SEGMENTO"] == seg]
                    for _, row in sub.iterrows():
                        labels.append(str(row["ADQUIRENTE"]))
                        parents.append(seg)
                        values.append(int(row["N"]))
                        colors_t.append(COLORS[i % len(COLORS)])
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    marker=dict(colors=colors_t),
                    textinfo="label+value+percent parent",
                    hovertemplate="<b>%{label}</b><br>Comercios: %{value:,}<br>%{percentParent:.1%} del padre<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("**Heatmap — Volumen promedio por segmento y ciudad**")
        heat_df = run_query(f"""
            SELECT SEGMENTO, CIUDAD, ROUND(AVG(VOLUMEN_MENSUAL_COP)/1e6, 1) AS VOL_M
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
            st.markdown("**Funnel — Base de comercios**")
            pci_n_f = int(run_query(f"SELECT SUM(CASE WHEN PCI_CERTIFICADO THEN 1 ELSE 0 END) AS N FROM {TABLE} WHERE {WHERE}")["N"].iloc[0])
            funnel_data = [
                ("Total comercios", total),
                ("Activos", activos),
                ("Con e-commerce", ecom_n),
                ("PCI certificados", pci_n_f),
            ]
            fig_funnel = go.Figure(go.Funnel(
                y=[f[0] for f in funnel_data],
                x=[f[1] for f in funnel_data],
                textinfo="value+percent initial",
                marker=dict(color=[COLORS[0], COLORS[2], COLORS[1], COLORS[3]]),
                hovertemplate="%{y}: %{x:,}<extra></extra>",
            ))
            fig_funnel.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=10, r=10, t=30, b=10),
            )
            st.plotly_chart(fig_funnel, use_container_width=True)

        with col4:
            st.markdown("**Distribucion por adquirente**")
            adq_df = run_query(f"""
                SELECT ADQUIRENTE, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if adq_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=adq_df["ADQUIRENTE"].tolist(),
                    values=adq_df["N"].tolist(),
                    hole=0.5,
                    marker=dict(colors=COLORS[:len(adq_df)]),
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
                ))
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.markdown("**Mapa — Volumen mensual por ciudad**")
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
                    caption="Comercios",
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
                        popup=folium.Popup(f"<b>{r['city']}</b><br>Comercios: {r['n']:,}<br>${r['vol_m']:.1f}M COP", max_width=200),
                        tooltip=f"{r['city']}: {r['n']:,} comercios",
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

        st.markdown("**Top 10 comercios por volumen mensual**")
        top_df = run_query(f"""
            SELECT NOMBRE_COMERCIO, SEGMENTO, ADQUIRENTE, CIUDAD,
                   VOLUMEN_MENSUAL_COP, TRANSACCIONES_MES,
                   ROUND(TASA_APROBACION*100, 1) AS TASA_APROB,
                   ROUND(MDR_PCT, 2) AS MDR
            FROM {TABLE} WHERE {WHERE}
            ORDER BY VOLUMEN_MENSUAL_COP DESC LIMIT 10
        """)
        if not top_df.empty:
            top_df["VOLUMEN_MENSUAL_COP"] = (top_df["VOLUMEN_MENSUAL_COP"] / 1e6).round(1)
            st.dataframe(top_df.rename(columns={
                "NOMBRE_COMERCIO": "Comercio", "SEGMENTO": "Segmento", "ADQUIRENTE": "Adquirente",
                "CIUDAD": "Ciudad", "VOLUMEN_MENSUAL_COP": "Volumen ($M COP)", "TRANSACCIONES_MES": "Txns/mes",
                "TASA_APROB": "% Aprobacion", "MDR": "MDR %"
            }), use_container_width=True, hide_index=True)

with tab2:
    st.markdown("**Simulador de revenue por comercios**")
    st.caption("Estime el ingreso potencial ajustando las variables clave de la base de comercios. El modelo pondera cada factor segun su impacto relativo en el revenue.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_mdr = st.slider("MDR promedio (%)", 0.5, 5.0, 2.0, step=0.1, key="com_sim_mdr",
                            help="Mayor MDR genera mas ingreso por transaccion pero puede afectar competitividad")
        sim_mix_ecom = st.slider("% Comercios con e-commerce", 0, 100, 40, key="com_sim_ecom",
                                 help="E-commerce aporta mayor volumen pero requiere inversion en seguridad")
        sim_mix_seg = st.slider("% Segmento premium (Enterprise)", 0, 100, 25, key="com_sim_seg",
                                help="Comercios premium generan mayor ticket y volumen por unidad")
        sim_riesgo = st.slider("Nivel de riesgo promedio (1=bajo, 5=alto)", 1, 5, 2, key="com_sim_riesgo",
                               help="Mayor riesgo implica mas contracargos y perdidas potenciales")
        sim_terminales = st.slider("Terminales promedio por comercio", 1, 20, 3, key="com_sim_term",
                                   help="Mas terminales incrementa capacidad de captura de transacciones")
        sim_contracargos = st.slider("Tasa de contracargos (%)", 0.0, 5.0, 0.5, step=0.1, key="com_sim_cbk",
                                     help="Contracargos reducen el ingreso neto y aumentan costos operativos")

    def calcular_revenue(mdr, ecom, seg_prem, riesgo, term, cbk):
        s_mdr = min(mdr / 5.0, 1.0)
        s_ecom = 0.5 + (ecom / 100) * 0.5
        s_seg = 0.5 + (seg_prem / 100) * 0.5
        s_riesgo = max(0.0, 1.0 - (riesgo - 1) / 4 * 0.6)
        s_term = min(term / 20, 1.0) * 0.8 + 0.2
        s_cbk = max(0.0, 1.0 - (cbk / 5.0) * 0.9)
        score = (0.25 * s_mdr + 0.18 * s_ecom + 0.15 * s_seg + 0.18 * s_riesgo + 0.12 * s_term + 0.12 * s_cbk)
        margin = 0.20 + score * 0.60
        return round(min(max(margin, 0.20), 0.80), 3)

    margin_pred = calcular_revenue(sim_mdr, sim_mix_ecom, sim_mix_seg, sim_riesgo, sim_terminales, sim_contracargos)
    margin_pct = margin_pred * 100

    with col_result:
        if margin_pct >= 65:
            nivel = "Alto"
            color_nivel = "#36B37E"
            interpretacion = "Excelente margen potencial. La combinacion de MDR, mix de segmentos y bajo riesgo genera un retorno optimo. Mantener estrategia actual."
        elif margin_pct >= 50:
            nivel = "Medio-Alto"
            color_nivel = "#29B5E8"
            interpretacion = "Margen aceptable con oportunidad de mejora. Considerar aumentar penetracion digital y optimizar pricing por segmento."
        elif margin_pct >= 35:
            nivel = "Medio"
            color_nivel = "#FFAB00"
            interpretacion = "Margen por debajo del benchmark. Revisar MDR, reducir contracargos y aumentar base de comercios premium."
        else:
            nivel = "Bajo"
            color_nivel = "#DE350B"
            interpretacion = "Margen critico. Accion inmediata: renegociar pricing, reforzar gestion de riesgo y reducir contracargos."

        st.metric("Margen estimado", f"{margin_pct:.1f}%")
        if margin_pct >= 65:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif margin_pct >= 50:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif margin_pct >= 35:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(margin_pct),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[20, 80], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[20, 30], color="#FFCDD2"),
                    dict(range=[30, 40], color="#FFEBEE"),
                    dict(range=[40, 50], color="#FFF3E0"),
                    dict(range=[50, 55], color="#FFF8E1"),
                    dict(range=[55, 65], color="#E3F2FD"),
                    dict(range=[65, 80], color="#E8F5E9"),
                ],
                threshold=dict(line=dict(color="#FF8B00", width=3), thickness=0.8, value=60),
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

    if "com_escenarios" not in st.session_state:
        st.session_state.com_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="com_save"):
        if len(st.session_state.com_escenarios) >= 3:
            st.session_state.com_escenarios.pop(0)
        st.session_state.com_escenarios.append({
            "MDR (%)": sim_mdr,
            "% E-commerce": sim_mix_ecom,
            "% Premium": sim_mix_seg,
            "Nivel riesgo": sim_riesgo,
            "Terminales/com": sim_terminales,
            "Contracargos (%)": sim_contracargos,
            "Margen estimado": f"{margin_pct:.1f}%",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.com_escenarios:
        esc_df = pd.DataFrame(st.session_state.com_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="com_clear"):
            st.session_state.com_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
