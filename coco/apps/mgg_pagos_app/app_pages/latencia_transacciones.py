import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from decimal import Decimal

TABLE = "MGG_PAGOS.DISPONIBILIDAD_Y_PERFORMANCE.LATENCIA_TRANSACCIONES"

COLORS = ["#29B5E8", "#11567F", "#71D4F0", "#0E3A53", "#A3E4F7", "#1B8BBF", "#5BC3E8", "#083248"]


from app_pages.conn_helper import run_query



def color_tag(value, good, bad, fmt="{:.1f}", inverse=False):
    v = float(value)
    if not inverse:
        if v >= good:
            return f":green[**{fmt.format(v)}**]"
        elif v >= bad:
            return f":orange[**{fmt.format(v)}**]"
        else:
            return f":red[**{fmt.format(v)}**]"
    else:
        if v <= good:
            return f":green[**{fmt.format(v)}**]"
        elif v <= bad:
            return f":orange[**{fmt.format(v)}**]"
        else:
            return f":red[**{fmt.format(v)}**]"


@st.cache_data(ttl=300, show_spinner=False)
def get_filter_options():
    redes = run_query(f"SELECT DISTINCT RED FROM {TABLE} ORDER BY 1")["RED"].tolist()
    bancos = run_query(f"SELECT DISTINCT BANCO_EMISOR FROM {TABLE} ORDER BY 1")["BANCO_EMISOR"].tolist()
    canales = run_query(f"SELECT DISTINCT CANAL FROM {TABLE} ORDER BY 1")["CANAL"].tolist()
    tipos = run_query(f"SELECT DISTINCT TIPO_TXN FROM {TABLE} ORDER BY 1")["TIPO_TXN"].tolist()
    entornos = run_query(f"SELECT DISTINCT ENTORNO FROM {TABLE} ORDER BY 1")["ENTORNO"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return redes, bancos, canales, tipos, entornos, fechas


st.header(":material/speed: Latencia de transacciones")
st.caption("Desglose de latencia por componente (switch, routing, fraude, red, emisor), percentiles, SLA y deteccion de degradacion.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Identificar cuellos de botella en el flujo de autorizacion, asegurando que cada componente del pipeline responda dentro del SLA acordado.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Cada transaccion registra latencia por componente (switch, routing, fraude, red, emisor). Se calculan percentiles P50-P999 y se detectan degradaciones vs baseline.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Reducir latencia mejora conversion, experiencia del tarjetahabiente y reduce timeouts. Cada 100ms de mejora se traduce en mayor aprobacion.")

redes, bancos, canales, tipos, entornos, fechas_df = get_filter_options()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_kpi = run_query(f"""
    SELECT
        ROUND(AVG(LATENCIA_TOTAL_MS), 1) AS LAT_PROM,
        ROUND(AVG(P50_MS), 1) AS P50_PROM,
        ROUND(AVG(P95_MS), 1) AS P95_PROM,
        ROUND(AVG(P99_MS), 1) AS P99_PROM,
        ROUND(AVG(TASA_DENTRO_SLA)*100, 1) AS SLA_PCT,
        SUM(CASE WHEN DEGRADACION_DETECTADA THEN 1 ELSE 0 END) AS DEGRADACIONES,
        COUNT(*) AS TOTAL_MED,
        ROUND(AVG(SLA_OBJETIVO_MS), 0) AS SLA_OBJ
    FROM {TABLE}
""")

if not dx_kpi.empty and dx_kpi["TOTAL_MED"].iloc[0] > 0:
    dx = dx_kpi.iloc[0]
    dx_lat = float(dx["LAT_PROM"])
    dx_sla = float(dx["SLA_PCT"])
    dx_p99 = float(dx["P99_PROM"])
    dx_degs = int(dx["DEGRADACIONES"])

    dx_worst_comp = run_query(f"SELECT COMPONENTE_MAS_LENTO, COUNT(*) AS N FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 1")
    dx_worst_red = run_query(f"SELECT RED, ROUND(AVG(LATENCIA_TOTAL_MS), 1) AS L FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 1")

    if dx_lat <= float(dx["SLA_OBJ"]):
        dx_estado = f"Latencia promedio ({color_tag(dx_lat, 0, 500, fmt='{:.0f} ms', inverse=True)}) :green[**dentro de SLA**] ({float(dx['SLA_OBJ']):.0f} ms)."
    else:
        dx_estado = f"Latencia promedio ({color_tag(dx_lat, 0, 500, fmt='{:.0f} ms', inverse=True)}) :red[**excede SLA**] ({float(dx['SLA_OBJ']):.0f} ms). P99: {dx_p99:.0f} ms."

    dx_lines = [dx_estado]
    if not dx_worst_comp.empty:
        dx_lines.append(f"- Componente cuello de botella: **{dx_worst_comp.iloc[0]['COMPONENTE_MAS_LENTO']}** ({int(dx_worst_comp.iloc[0]['N'])} veces)")
    if not dx_worst_red.empty:
        dx_lines.append(f"- Red mas lenta: **{dx_worst_red.iloc[0]['RED']}** ({float(dx_worst_red.iloc[0]['L']):.0f} ms prom)")
    dx_lines.append(f"- Degradaciones detectadas: **{dx_degs:,}** — Cumplimiento SLA: **{dx_sla:.1f}%**")

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
            if dx_lat > float(dx["SLA_OBJ"]):
                recs.append("1. :red[**Optimizar componente mas lento**]: la latencia promedio supera el SLA objetivo.")
            if dx_p99 > float(dx["SLA_OBJ"]) * 2:
                recs.append("2. :orange[**Atender cola de latencia (P99)**]: los outliers duplican el SLA, impactando experiencia.")
            if dx_degs > 100:
                recs.append("3. :orange[**Investigar degradaciones**]: alto volumen sugiere problemas sistematicos recurrentes.")
            if dx_sla < 95:
                recs.append("4. :red[**Mejorar tasa SLA**]: menos del 95% de transacciones cumplen el objetivo de latencia.")
            if not recs:
                recs.append(":green[**Latencia operando dentro de parametros optimos.**] Monitoreo continuo recomendado.")
            st.markdown("\n".join(recs))

st.divider()

fc1, fc2, fc3, fc4, fc5 = st.columns(5)
with fc1:
    fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="lat_fecha")
    if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
        fecha_inicio, fecha_fin = fecha_rng
    else:
        fecha_inicio, fecha_fin = fmin, fmax
with fc2:
    red_sel = st.selectbox("Red", ["Todos"] + redes, key="lat_red")
with fc3:
    canal_sel = st.selectbox("Canal", ["Todos"] + canales, key="lat_canal")
with fc4:
    banco_sel = st.selectbox("Banco emisor", ["Todos"] + bancos, key="lat_banco")
with fc5:
    tipo_sel = st.selectbox("Tipo txn", ["Todos"] + tipos, key="lat_tipo")

WHERE = f"FECHA_HORA::DATE BETWEEN '{fecha_inicio}' AND '{fecha_fin}'"
if red_sel != "Todos":
    WHERE += f" AND RED = '{red_sel}'"
if canal_sel != "Todos":
    WHERE += f" AND CANAL = '{canal_sel}'"
if banco_sel != "Todos":
    WHERE += f" AND BANCO_EMISOR = '{banco_sel}'"
if tipo_sel != "Todos":
    WHERE += f" AND TIPO_TXN = '{tipo_sel}'"

kpi_df = run_query(f"""
    SELECT
        ROUND(AVG(LATENCIA_TOTAL_MS), 1) AS LAT_PROM,
        ROUND(AVG(LATENCIA_SWITCH_MS), 1) AS SW,
        ROUND(AVG(LATENCIA_ROUTING_MS), 1) AS RT,
        ROUND(AVG(LATENCIA_FRAUDE_MS), 1) AS FR,
        ROUND(AVG(LATENCIA_RED_MS), 1) AS RD,
        ROUND(AVG(LATENCIA_EMISOR_MS), 1) AS EM,
        ROUND(AVG(P50_MS), 1) AS P50,
        ROUND(AVG(P95_MS), 1) AS P95,
        ROUND(AVG(P99_MS), 1) AS P99,
        ROUND(AVG(P999_MS), 1) AS P999,
        ROUND(AVG(TASA_DENTRO_SLA)*100, 1) AS SLA_PCT,
        COUNT(*) AS TOTAL_MED,
        ROUND(AVG(SLA_OBJETIVO_MS), 0) AS SLA_OBJ,
        SUM(CASE WHEN DEGRADACION_DETECTADA THEN 1 ELSE 0 END) AS DEGS,
        ROUND(AVG(VARIACION_VS_BASELINE_PCT), 1) AS VAR_BASE,
        SUM(TRANSACCIONES_MUESTRA) AS TX_MUESTRA
    FROM {TABLE}
    WHERE {WHERE}
""")

if kpi_df.empty or kpi_df["TOTAL_MED"].iloc[0] == 0:
    st.info("No hay datos para los filtros seleccionados.")
else:
    r = kpi_df.iloc[0]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Latencia total prom", f"{float(r['LAT_PROM']):.1f} ms")
    k2.metric("P50", f"{float(r['P50']):.1f} ms")
    k3.metric("P95", f"{float(r['P95']):.1f} ms")
    k4.metric("P99", f"{float(r['P99']):.1f} ms")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Tasa dentro SLA", f"{float(r['SLA_PCT']):.1f}%")
    k6.metric("SLA objetivo", f"{float(r['SLA_OBJ']):.0f} ms")
    k7.metric("Degradaciones", f"{int(r['DEGS']):,}")
    k8.metric("Var vs baseline", f"{float(r['VAR_BASE']):+.1f}%")

    st.divider()

    st.markdown("**Cascada de latencia — Desglose por componente**")
    comp_labels = ["Switch", "Routing", "Fraude", "Red", "Emisor"]
    comp_vals = [float(r["SW"]), float(r["RT"]), float(r["FR"]), float(r["RD"]), float(r["EM"])]
    fig_wf = go.Figure(go.Waterfall(
        x=comp_labels + ["Total"],
        y=comp_vals + [float(r["LAT_PROM"])],
        measure=["relative"] * 5 + ["total"],
        text=[f"{v:.1f} ms" for v in comp_vals] + [f"{float(r['LAT_PROM']):.1f} ms"],
        textposition="outside",
        connector=dict(line=dict(color="#11567F", width=1)),
        increasing=dict(marker=dict(color=COLORS[0])),
        totals=dict(marker=dict(color=COLORS[1])),
        hovertemplate="<b>%{x}</b><br>%{y:.1f} ms<extra></extra>",
    ))
    fig_wf.update_layout(
        template="plotly_white", paper_bgcolor="#FFFFFF", height=380,
        margin=dict(l=60, r=20, t=10, b=60),
        yaxis=dict(title="Latencia (ms)"),
    )
    st.plotly_chart(fig_wf, use_container_width=True, key="lat_waterfall")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Percentiles por red — Distribucion de cola**")
        pct_df = run_query(f"""
            SELECT RED,
                   ROUND(AVG(P50_MS), 1) AS P50,
                   ROUND(AVG(P95_MS), 1) AS P95,
                   ROUND(AVG(P99_MS), 1) AS P99,
                   ROUND(AVG(P999_MS), 1) AS P999
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)
        if not pct_df.empty:
            fig_pct = go.Figure()
            for j, (pname, pcol) in enumerate([("P50", "P50"), ("P95", "P95"), ("P99", "P99"), ("P999", "P999")]):
                fig_pct.add_trace(go.Bar(
                    x=pct_df["RED"].tolist(),
                    y=pct_df[pcol].tolist(),
                    name=pname,
                    marker=dict(color=COLORS[j]),
                    hovertemplate=f"<b>%{{x}}</b><br>{pname}: %{{y:.1f}} ms<extra></extra>",
                ))
            fig_pct.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=60, r=20, t=10, b=60),
                barmode="group",
                yaxis=dict(title="Latencia (ms)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_pct, use_container_width=True, key="lat_percentiles")

    with col2:
        st.markdown("**Distribucion de latencia total por canal**")
        box_df = run_query(f"""
            SELECT CANAL, LATENCIA_TOTAL_MS
            FROM {TABLE}
            WHERE {WHERE}
        """)
        if not box_df.empty:
            fig_box = go.Figure()
            canal_list = sorted(box_df["CANAL"].unique().tolist())
            for i, canal in enumerate(canal_list):
                subset = box_df[box_df["CANAL"] == canal]
                fig_box.add_trace(go.Box(
                    y=subset["LATENCIA_TOTAL_MS"].tolist(),
                    name=canal,
                    marker=dict(color=COLORS[i % len(COLORS)]),
                    boxmean="sd",
                ))
            fig_box.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=60, r=20, t=10, b=60),
                yaxis=dict(title="Latencia total (ms)"),
                showlegend=False,
            )
            st.plotly_chart(fig_box, use_container_width=True, key="lat_boxplot")

    st.markdown("**Cumplimiento SLA por red**")
    sla_df = run_query(f"""
        SELECT RED, ROUND(AVG(TASA_DENTRO_SLA)*100, 1) AS SLA_PCT
        FROM {TABLE}
        WHERE {WHERE}
        GROUP BY 1 ORDER BY 1
    """)
    if not sla_df.empty:
        n_red = len(sla_df)
        gauge_cols = st.columns(min(n_red, 6))
        for i, (_, row) in enumerate(sla_df.iterrows()):
            with gauge_cols[i % len(gauge_cols)]:
                val = float(row["SLA_PCT"])
                g_color = "#36B37E" if val >= 95 else ("#FFAB00" if val >= 90 else "#DE350B")
                fig_g = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=val,
                    title=dict(text=str(row["RED"]), font=dict(size=12)),
                    number=dict(suffix="%", font=dict(size=22)),
                    gauge=dict(
                        axis=dict(range=[80, 100], ticksuffix="%"),
                        bar=dict(color=g_color),
                        steps=[
                            dict(range=[80, 90], color="#FFEBEE"),
                            dict(range=[90, 95], color="#FFF8E1"),
                            dict(range=[95, 100], color="#E8F5E9"),
                        ],
                        threshold=dict(line=dict(color=COLORS[1], width=2), thickness=0.8, value=95),
                    ),
                ))
                fig_g.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=200, margin=dict(l=20, r=20, t=40, b=10),
                )
                st.plotly_chart(fig_g, use_container_width=True, key=f"lat_gauge_{i}")

    col3, col4 = st.columns(2)

    with col3:
        st.markdown("**Componente cuello de botella — Frecuencia**")
        comp_df = run_query(f"""
            SELECT COMPONENTE_MAS_LENTO, COUNT(*) AS N
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 2 DESC
        """)
        if not comp_df.empty:
            fig_donut = go.Figure(go.Pie(
                labels=comp_df["COMPONENTE_MAS_LENTO"].tolist(),
                values=comp_df["N"].tolist(),
                hole=0.5,
                marker=dict(colors=COLORS[:len(comp_df)]),
                textinfo="label+percent",
                hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
            ))
            fig_donut.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=10, r=10, t=10, b=10),
            )
            st.plotly_chart(fig_donut, use_container_width=True, key="lat_donut")

    with col4:
        st.markdown("**Deteccion de anomalias — Variacion vs baseline**")
        anom_df = run_query(f"""
            SELECT VARIACION_VS_BASELINE_PCT, LATENCIA_TOTAL_MS,
                   DEGRADACION_DETECTADA, TRANSACCIONES_MUESTRA, RED
            FROM {TABLE}
            WHERE {WHERE}
        """)
        if not anom_df.empty:
            fig_anom = go.Figure()
            for deg_val in [True, False]:
                sub = anom_df[anom_df["DEGRADACION_DETECTADA"] == deg_val]
                if sub.empty:
                    continue
                max_tx = max(anom_df["TRANSACCIONES_MUESTRA"].max(), 1)
                sizes = (sub["TRANSACCIONES_MUESTRA"] / max_tx * 20 + 4).tolist()
                fig_anom.add_trace(go.Scatter(
                    x=sub["VARIACION_VS_BASELINE_PCT"].tolist(),
                    y=sub["LATENCIA_TOTAL_MS"].tolist(),
                    mode="markers",
                    name="Degradacion" if deg_val else "Normal",
                    marker=dict(
                        size=sizes,
                        color="#DE350B" if deg_val else COLORS[0],
                        opacity=0.6,
                        line=dict(width=0.5, color="#11567F"),
                    ),
                    text=sub["RED"].tolist(),
                    hovertemplate="<b>%{text}</b><br>Var baseline: %{x:.1f}%<br>Latencia: %{y:.0f} ms<extra></extra>",
                ))
            fig_anom.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=60, r=20, t=10, b=60),
                xaxis=dict(title="Variacion vs baseline (%)"),
                yaxis=dict(title="Latencia total (ms)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_anom, use_container_width=True, key="lat_anomaly")

    st.markdown("**Heatmap — Latencia promedio por banco emisor y red**")
    heat_df = run_query(f"""
        SELECT BANCO_EMISOR, RED, ROUND(AVG(LATENCIA_TOTAL_MS), 1) AS LAT_PROM
        FROM {TABLE}
        WHERE {WHERE}
        GROUP BY 1, 2
    """)
    if not heat_df.empty:
        pivot = heat_df.pivot_table(index="BANCO_EMISOR", columns="RED", values="LAT_PROM", fill_value=0)
        fig_heat = go.Figure(go.Heatmap(
            z=pivot.values.tolist(),
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[[0, "#E8F5E9"], [0.4, "#FFF8E1"], [0.7, "#FFAB00"], [1, "#DE350B"]],
            text=[[f"{v:.0f}" for v in row] for row in pivot.values.tolist()],
            texttemplate="%{text} ms",
            textfont=dict(size=11),
            hovertemplate="Banco: %{y}<br>Red: %{x}<br>Latencia: %{z:.1f} ms<extra></extra>",
            colorbar=dict(title="ms"),
        ))
        fig_heat.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF",
            height=500, margin=dict(l=160, r=40, t=10, b=80),
            xaxis=dict(tickfont=dict(size=12)),
            yaxis=dict(tickfont=dict(size=11)),
        )
        st.plotly_chart(fig_heat, use_container_width=True, key="lat_heatmap")
