import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from app_pages.map_helper import colombia_scatter_map

TABLE = "MGG_PAGOS.LIQUIDACION_Y_COMPENSACION.LIQUIDACIONES"

COLORS = ["#29B5E8", "#FF8B00", "#36B37E", "#6554C0", "#DE350B", "#11567F", "#FFAB00", "#00A3BF"]

COORDS_CIUDAD = {
    "Bogota": (4.6097, -74.0817), "Medellin": (6.2442, -75.5812),
    "Cali": (3.4516, -76.5320), "Barranquilla": (10.9685, -74.7813),
    "Bucaramanga": (7.1254, -73.1198), "Cartagena": (10.3997, -75.5144),
    "Cucuta": (7.8939, -72.5078), "Ibague": (4.4389, -75.2322),
    "Pereira": (4.8133, -75.6961), "Manizales": (5.0689, -75.5174),
    "Villavicencio": (4.1420, -73.6266), "Santa Marta": (11.2404, -74.1990),
}


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filter_options():
    frecuencias = run_query(f"SELECT DISTINCT FRECUENCIA_LIQUIDACION FROM {TABLE} ORDER BY 1")["FRECUENCIA_LIQUIDACION"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO FROM {TABLE} ORDER BY 1")["ESTADO"].tolist()
    bancos = run_query(f"SELECT DISTINCT BANCO_DESTINO FROM {TABLE} ORDER BY 1")["BANCO_DESTINO"].tolist()
    metodos = run_query(f"SELECT DISTINCT METODO_PAGO FROM {TABLE} ORDER BY 1")["METODO_PAGO"].tolist()
    ciudades = run_query(f"SELECT DISTINCT CIUDAD FROM {TABLE} ORDER BY 1")["CIUDAD"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_CORTE) AS FMIN, MAX(FECHA_CORTE) AS FMAX FROM {TABLE}")
    return frecuencias, estados, bancos, metodos, ciudades, fechas


def build_where(fecha_ini, fecha_f, frec_s, estado_s, banco_s, metodo_s, ciudad_s,
                all_frec, all_estados, all_bancos, all_metodos, all_ciudades):
    clauses = [f"FECHA_CORTE BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if frec_s and frec_s != "Todos" and frec_s in all_frec:
        clauses.append(f"FRECUENCIA_LIQUIDACION = '{frec_s}'")
    if estado_s and estado_s != "Todos" and estado_s in all_estados:
        clauses.append(f"ESTADO = '{estado_s}'")
    if banco_s and banco_s != "Todos" and banco_s in all_bancos:
        clauses.append(f"BANCO_DESTINO = '{banco_s}'")
    if metodo_s and metodo_s != "Todos" and metodo_s in all_metodos:
        clauses.append(f"METODO_PAGO = '{metodo_s}'")
    if ciudad_s and ciudad_s != "Todos" and ciudad_s in all_ciudades:
        clauses.append(f"CIUDAD = '{ciudad_s}'")
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


st.header(":material/payments: Liquidaciones")
st.caption("Proceso de liquidacion (payout) a comercios. Monto bruto, comisiones, retenciones fiscales y monto neto depositado.")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Falta de transparencia en el proceso de liquidacion que genera reclamos por montos incorrectos o pagos tardios.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Cada liquidacion detalla: monto bruto, interchange, scheme fees, comision adquirente, retenciones fiscales y monto neto.")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Reducir reclamos con liquidaciones transparentes, cumplir tiempos de pago T+1/T+2 y optimizar flujo de fondos.")

    frecuencias, estados, bancos, metodos, ciudades, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_CORTE BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            ROUND(SUM(MONTO_BRUTO_COP)/1e9, 2) AS BRUTO_B,
            ROUND(SUM(MONTO_NETO_COP)/1e9, 2) AS NETO_B,
            ROUND(AVG(HORAS_PROCESO), 1) AS HORAS_PROM,
            SUM(CASE WHEN RETENCION_FRAUDE THEN 1 ELSE 0 END)*100.0/COUNT(*) AS PCT_FRAUDE,
            SUM(CASE WHEN NOTIFICACION_ENVIADA THEN 1 ELSE 0 END)*100.0/COUNT(*) AS PCT_NOTIF,
            ROUND(AVG(MDR_EFECTIVO_PCT), 2) AS MDR_PROM
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL"])
        dx_bruto = float(dx["BRUTO_B"])
        dx_neto = float(dx["NETO_B"])
        dx_ratio = round(dx_neto / dx_bruto * 100, 1) if dx_bruto > 0 else 0
        dx_horas = float(dx["HORAS_PROM"])
        dx_pct_fraude = float(dx["PCT_FRAUDE"])

        dx_worst_banco = run_query(f"""
            SELECT BANCO_DESTINO, ROUND(AVG(HORAS_PROCESO), 1) AS HORAS
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_top_ciudad = run_query(f"""
            SELECT CIUDAD, ROUND(SUM(MONTO_NETO_COP)/1e9, 2) AS NETO_B
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)

        if dx_ratio >= 85:
            dx_estado = f"Eficiencia neto/bruto ({color_tag(dx_ratio, 85, 75)}) :green[**saludable**]. Procesando ${dx_bruto:.1f}B COP bruto."
        elif dx_ratio >= 75:
            dx_estado = f"Eficiencia neto/bruto ({color_tag(dx_ratio, 85, 75)}) en :orange[**zona de atencion**]. Comisiones y retenciones altas."
        else:
            dx_estado = f"Eficiencia neto/bruto ({color_tag(dx_ratio, 85, 75)}) :red[**critica**]. Revisar estructura de costos."

        dx_lines = [dx_estado]
        if not dx_worst_banco.empty:
            wb = dx_worst_banco.iloc[0]
            dx_lines.append(f"- Banco con mayor demora: **{wb['BANCO_DESTINO']}** ({color_tag(float(wb['HORAS']), 24, 48, fmt='{:.1f}h', inverse=True)})")
        if not dx_top_ciudad.empty:
            tc = dx_top_ciudad.iloc[0]
            dx_lines.append(f"- Ciudad mayor volumen neto: **{tc['CIUDAD']}** (${float(tc['NETO_B']):.2f}B COP)")
        dx_lines.append(f"- Retencion por fraude: **{dx_pct_fraude:.1f}%** {'— :green[**bajo control**]' if dx_pct_fraude < 5 else '— :orange[**monitorear**]'}")
        dx_lines.append(f"- Horas proceso promedio: **{dx_horas:.1f}h** {'— :green[**dentro de SLA**]' if dx_horas < 24 else '— :red[**excede SLA**]'}")

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
                if dx_ratio < 80:
                    recs.append("1. :red[**Revisar estructura de comisiones**]: ratio neto/bruto por debajo del objetivo.")
                if dx_horas > 24:
                    recs.append("2. :orange[**Optimizar tiempos de pago**]: horas de proceso exceden SLA T+1.")
                if dx_pct_fraude > 5:
                    recs.append("3. :orange[**Evaluar retenciones de fraude**]: porcentaje alto puede afectar liquidez de comercios.")
                if float(dx["MDR_PROM"]) > 3:
                    recs.append("4. :orange[**Negociar MDR**]: tasa efectiva por encima del benchmark regional.")
                if not recs:
                    recs.append(":green[**Proceso de liquidacion operando dentro de parametros optimos.**] Mantener monitoreo continuo.")
                st.markdown("\n".join(recs))

    st.divider()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="liq_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        frec_sel = st.selectbox("Frecuencia", ["Todos"] + frecuencias, key="liq_frec")
    with fc3:
        estado_sel = st.selectbox("Estado", ["Todos"] + estados, key="liq_estado")
    with fc4:
        banco_sel = st.selectbox("Banco destino", ["Todos"] + bancos, key="liq_banco")
    with fc5:
        ciudad_sel = st.selectbox("Ciudad", ["Todos"] + ciudades, key="liq_ciudad")

    metodo_sel = "Todos"

    WHERE = build_where(fecha_inicio, fecha_fin, frec_sel, estado_sel, banco_sel, metodo_sel, ciudad_sel,
                        frecuencias, estados, bancos, metodos, ciudades)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            ROUND(SUM(MONTO_BRUTO_COP)/1e9, 2) AS BRUTO_B,
            ROUND(SUM(MONTO_NETO_COP)/1e9, 2) AS NETO_B,
            ROUND(SUM(COMISION_ADQUIRENCIA_COP + INTERCHANGE_COP + SCHEME_FEES_COP)/1e6, 1) AS COMISIONES_M,
            ROUND(SUM(INTERCHANGE_COP)/1e6, 1) AS INTERCHANGE_M,
            ROUND(AVG(HORAS_PROCESO), 1) AS HORAS_PROM,
            ROUND(SUM(CASE WHEN RETENCION_FRAUDE THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_FRAUDE,
            ROUND(SUM(CASE WHEN NOTIFICACION_ENVIADA THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_NOTIF
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total liquidaciones", f"{total:,}")
        k2.metric("Monto bruto", f"${float(r['BRUTO_B']):.2f}B COP")
        k3.metric("Monto neto", f"${float(r['NETO_B']):.2f}B COP")
        k4.metric("Comisiones totales", f"${float(r['COMISIONES_M']):,.0f}M COP")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Interchange total", f"${float(r['INTERCHANGE_M']):,.0f}M COP")
        k6.metric("Horas proceso prom", f"{float(r['HORAS_PROM']):.1f}h")
        k7.metric("% Retencion fraude", f"{float(r['PCT_FRAUDE']):.1f}%")
        k8.metric("% Notificacion enviada", f"{float(r['PCT_NOTIF']):.1f}%")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_CORTE) AS MES,
                   ROUND(SUM(MONTO_BRUTO_COP)/1e9, 2) AS BRUTO_B,
                   ROUND(SUM(MONTO_NETO_COP)/1e9, 2) AS NETO_B
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Monto bruto y neto**")
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
                    x=meses, y=trend_df["NETO_B"].tolist(), name="Monto neto (B COP)",
                    yaxis="y2", line=dict(color=COLORS[1], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="Bruto (B COP)"),
                    yaxis2=dict(title="Neto (B COP)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Frecuencia de liquidacion y estado**")
            tree_df = run_query(f"""
                SELECT FRECUENCIA_LIQUIDACION, ESTADO, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
            """)
            if tree_df.empty:
                st.info("Sin datos.")
            else:
                labels, parents, values, colors_t = [], [], [], []
                parent_totals = tree_df.groupby("FRECUENCIA_LIQUIDACION")["N"].sum().sort_values(ascending=False)
                for i, (parent, parent_total) in enumerate(parent_totals.items()):
                    labels.append(parent)
                    parents.append("")
                    values.append(int(parent_total))
                    colors_t.append(COLORS[i % len(COLORS)])
                    sub = tree_df[tree_df["FRECUENCIA_LIQUIDACION"] == parent]
                    for _, row in sub.iterrows():
                        labels.append(str(row["ESTADO"]))
                        parents.append(parent)
                        values.append(int(row["N"]))
                        colors_t.append(COLORS[i % len(COLORS)])
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    marker=dict(colors=colors_t),
                    textinfo="label+value+percent parent",
                    hovertemplate="<b>%{label}</b><br>Liquidaciones: %{value:,}<br>%{percentParent:.1%} del padre<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("**Heatmap — Monto neto promedio por banco destino y estado**")
        heat_df = run_query(f"""
            SELECT BANCO_DESTINO, ESTADO, ROUND(AVG(MONTO_NETO_COP)/1e6, 1) AS NETO_M
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="BANCO_DESTINO", columns="ESTADO", values="NETO_M", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E3F2FD"], [0.3, "#29B5E8"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"${v:.1f}M" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Banco: %{y}<br>Estado: %{x}<br>Neto prom: $%{z:.1f}M COP<extra></extra>",
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
            st.markdown("**Funnel — Cascada de liquidacion**")
            cascade_df = run_query(f"""
                SELECT
                    ROUND(SUM(MONTO_BRUTO_COP)/1e9, 2) AS BRUTO,
                    ROUND(SUM(INTERCHANGE_COP)/1e9, 2) AS INTERCHANGE,
                    ROUND(SUM(SCHEME_FEES_COP)/1e9, 2) AS SCHEME,
                    ROUND(SUM(COMISION_ADQUIRENCIA_COP)/1e9, 2) AS COMISION,
                    ROUND(SUM(IVA_COMISIONES_COP + RETEFUENTE_COP + RETEICA_COP)/1e9, 2) AS IMPUESTOS,
                    ROUND(SUM(MONTO_NETO_COP)/1e9, 2) AS NETO
                FROM {TABLE} WHERE {WHERE}
            """)
            if not cascade_df.empty:
                cd = cascade_df.iloc[0]
                funnel_data = [
                    ("Bruto", float(cd["BRUTO"])),
                    ("- Interchange", float(cd["BRUTO"]) - float(cd["INTERCHANGE"])),
                    ("- Scheme fees", float(cd["BRUTO"]) - float(cd["INTERCHANGE"]) - float(cd["SCHEME"])),
                    ("- Comision adq", float(cd["BRUTO"]) - float(cd["INTERCHANGE"]) - float(cd["SCHEME"]) - float(cd["COMISION"])),
                    ("- Impuestos", float(cd["BRUTO"]) - float(cd["INTERCHANGE"]) - float(cd["SCHEME"]) - float(cd["COMISION"]) - float(cd["IMPUESTOS"])),
                    ("Neto", float(cd["NETO"])),
                ]
                fig_funnel = go.Figure(go.Funnel(
                    y=[f[0] for f in funnel_data],
                    x=[f[1] for f in funnel_data],
                    textinfo="value+percent initial",
                    marker=dict(color=[COLORS[0], COLORS[1], COLORS[3], COLORS[5], COLORS[4], COLORS[2]]),
                    hovertemplate="%{y}: $%{x:.2f}B COP<extra></extra>",
                ))
                fig_funnel.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_funnel, use_container_width=True)

        with col4:
            st.markdown("**Distribucion por metodo de pago**")
            metodo_df = run_query(f"""
                SELECT METODO_PAGO, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if metodo_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=metodo_df["METODO_PAGO"].tolist(),
                    values=metodo_df["N"].tolist(),
                    hole=0.5,
                    marker=dict(colors=COLORS[:len(metodo_df)]),
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
                ))
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.markdown("**Mapa — Volumen de liquidaciones por ciudad**")
        map_df = run_query(f"""
            SELECT CIUDAD, COUNT(*) AS N, ROUND(SUM(MONTO_NETO_COP)/1e6, 1) AS NETO_M
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC
        """)
        if not map_df.empty:
            map_data = []
            for _, row in map_df.iterrows():
                city = str(row["CIUDAD"])
                if city in COORDS_CIUDAD:
                    lat, lon = COORDS_CIUDAD[city]
                    map_data.append({"lat": lat, "lon": lon, "city": city, "n": int(row["N"]), "neto_m": float(row["NETO_M"])})
            if map_data:
                pdf = pd.DataFrame(map_data)
                pdf["hover"] = pdf.apply(lambda r: f"{r['city']}<br>Liquidaciones: {r['n']:,}<br>${r['neto_m']:.1f}M COP neto", axis=1)
                deck = colombia_scatter_map(pdf, colorbar_title="Liquidaciones")
                st.pydeck_chart(deck, key="liq_map")

        st.markdown("**Top 10 bancos destino por volumen**")
        banco_df = run_query(f"""
            SELECT BANCO_DESTINO,
                   COUNT(*) AS TOTAL,
                   ROUND(SUM(MONTO_NETO_COP)/1e6, 1) AS NETO_M,
                   ROUND(AVG(HORAS_PROCESO), 1) AS HORAS_PROM,
                   ROUND(AVG(MDR_EFECTIVO_PCT), 2) AS MDR_PROM
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """)
        if not banco_df.empty:
            st.dataframe(banco_df.rename(columns={
                "BANCO_DESTINO": "Banco", "TOTAL": "Liquidaciones",
                "NETO_M": "Neto ($M COP)", "HORAS_PROM": "Horas prom",
                "MDR_PROM": "MDR % prom"
            }), use_container_width=True, hide_index=True)

with tab2:
    st.markdown("**Simulador de eficiencia de liquidacion**")
    st.caption("Estime el ratio neto/bruto y la eficiencia del proceso ajustando variables clave. El modelo pondera cada factor segun su impacto relativo.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_mdr = st.slider("MDR efectivo (%)", 0.5, 8.0, 2.5, step=0.1, key="liq_sim_mdr",
                            help="Mayor MDR reduce el monto neto para el comercio")
        sim_interchange = st.slider("Tasa interchange (%)", 0.5, 5.0, 1.8, step=0.1, key="liq_sim_int",
                                    help="Interchange pagado al emisor, regulado en algunos mercados")
        sim_scheme = st.slider("Scheme fee (%)", 0.05, 1.0, 0.2, step=0.05, key="liq_sim_scheme",
                               help="Fee de la red (Visa/MC) por transaccion procesada")
        sim_horas = st.slider("Horas de proceso objetivo", 4, 72, 24, step=4, key="liq_sim_horas",
                              help="Tiempo objetivo de liquidacion (T+1 = 24h)")
        sim_pct_fraude = st.slider("% Retencion por fraude", 0, 20, 3, key="liq_sim_fraude",
                                   help="Porcentaje de liquidaciones retenidas por sospecha de fraude")
        sim_mix_diaria = st.slider("% Liquidacion diaria vs semanal", 0, 100, 60, key="liq_sim_mix",
                                   help="Mayor frecuencia diaria mejora flujo pero aumenta costos operativos")

    def calcular_eficiencia_liq(mdr, interchange, scheme, horas, pct_fraude, mix_diaria):
        total_fee = mdr + interchange + scheme
        s_fee = max(0, 1.0 - total_fee / 10.0)
        s_horas = max(0, 1.0 - horas / 72.0)
        s_fraude = 1.0 - (pct_fraude / 100.0) * 2.0
        s_mix = 0.5 + (mix_diaria / 100.0) * 0.5
        ratio_neto = max(0.70, 1.0 - total_fee / 100.0 - pct_fraude / 200.0)
        eficiencia = 0.25 * s_fee + 0.25 * s_horas + 0.20 * max(0, s_fraude) + 0.15 * s_mix + 0.15 * ratio_neto
        score = 0.50 + eficiencia * 0.48
        return round(min(max(score, 0.50), 0.98), 3), round(ratio_neto * 100, 1)

    score_pred, ratio_pred = calcular_eficiencia_liq(sim_mdr, sim_interchange, sim_scheme, sim_horas, sim_pct_fraude, sim_mix_diaria)
    score_pct = score_pred * 100

    with col_result:
        if score_pct >= 92:
            nivel = "Alta"
            color_nivel = "#36B37E"
            interpretacion = "Excelente eficiencia de liquidacion. Ratio neto/bruto optimo y tiempos dentro de SLA. Mantener condiciones actuales."
        elif score_pct >= 80:
            nivel = "Media-Alta"
            color_nivel = "#29B5E8"
            interpretacion = "Eficiencia aceptable. Revisar estructura de fees o tiempos de proceso para alcanzar nivel optimo."
        elif score_pct >= 65:
            nivel = "Media"
            color_nivel = "#FFAB00"
            interpretacion = "Eficiencia por debajo del benchmark. Priorizar reduccion de comisiones y optimizacion de tiempos de pago."
        else:
            nivel = "Baja"
            color_nivel = "#DE350B"
            interpretacion = "Eficiencia critica. Accion inmediata: renegociar fees, reducir retenciones de fraude y optimizar procesos de settlement."

        st.metric("Eficiencia estimada", f"{score_pct:.1f}%")
        st.metric("Ratio neto/bruto estimado", f"{ratio_pred:.1f}%")
        if score_pct >= 92:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif score_pct >= 80:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif score_pct >= 65:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

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

    if "liq_escenarios" not in st.session_state:
        st.session_state.liq_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="liq_save"):
        if len(st.session_state.liq_escenarios) >= 3:
            st.session_state.liq_escenarios.pop(0)
        st.session_state.liq_escenarios.append({
            "MDR %": sim_mdr,
            "Interchange %": sim_interchange,
            "Scheme %": sim_scheme,
            "Horas proceso": sim_horas,
            "% Ret. fraude": sim_pct_fraude,
            "% Liq. diaria": sim_mix_diaria,
            "Eficiencia": f"{score_pct:.1f}%",
            "Ratio N/B": f"{ratio_pred:.1f}%",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.liq_escenarios:
        esc_df = pd.DataFrame(st.session_state.liq_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="liq_clear"):
            st.session_state.liq_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
