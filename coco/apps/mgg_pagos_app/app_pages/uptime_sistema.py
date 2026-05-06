import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from decimal import Decimal

TABLE = "MGG_PAGOS.DISPONIBILIDAD_Y_PERFORMANCE.UPTIME_SISTEMA"

COLORS = ["#29B5E8", "#11567F", "#71D4F0", "#0E3A53", "#A3E4F7", "#1B8BBF", "#5BC3E8", "#083248"]


from app_pages.conn_helper import run_query



def color_tag(value, good, bad, fmt="{:.1f}%", inverse=False):
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
    servicios = run_query(f"SELECT DISTINCT SERVICIO FROM {TABLE} ORDER BY 1")["SERVICIO"].tolist()
    datacenters = run_query(f"SELECT DISTINCT DATACENTER FROM {TABLE} ORDER BY 1")["DATACENTER"].tolist()
    causas = run_query(f"SELECT DISTINCT CAUSA_PRINCIPAL FROM {TABLE} WHERE CAUSA_PRINCIPAL IS NOT NULL ORDER BY 1")["CAUSA_PRINCIPAL"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA) AS FMIN, MAX(FECHA) AS FMAX FROM {TABLE}")
    return servicios, datacenters, causas, fechas


st.header(":material/monitor_heart: Uptime del sistema")
st.caption("Disponibilidad, SLA, incidentes, MTTR/MTBF y consumo de recursos por servicio y datacenter.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Monitoreo continuo de la disponibilidad de cada servicio critico del switch de pagos, con trazabilidad de incidentes y cumplimiento de SLA.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Se registran minutos de disponibilidad, downtime y degradacion por servicio/dia. Incluye MTTR, MTBF, failover, alertas y consumo de CPU/memoria.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Garantizar niveles de servicio (99.9%+) para procesar pagos sin interrupciones, minimizar impacto financiero y priorizar inversiones en infraestructura.")

servicios, datacenters, causas, fechas_df = get_filter_options()
fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

dx_kpi = run_query(f"""
    SELECT
        ROUND(AVG(UPTIME_PCT), 2) AS UPTIME_PROM,
        ROUND(AVG(SLA_OBJETIVO), 2) AS SLA_PROM,
        SUM(CASE WHEN SLA_CUMPLIDO THEN 1 ELSE 0 END) AS SLA_OK,
        COUNT(*) AS TOTAL_REG,
        SUM(INCIDENTES_DIA) AS TOTAL_INCIDENTES,
        ROUND(AVG(MTTR_MINUTOS), 1) AS MTTR_PROM,
        ROUND(SUM(IMPACTO_FINANCIERO_COP)/1e6, 1) AS IMPACTO_M,
        ROUND(AVG(CPU_PICO_PCT), 1) AS CPU_PROM,
        ROUND(AVG(MEMORIA_PICO_PCT), 1) AS MEM_PROM
    FROM {TABLE}
""")

if not dx_kpi.empty and dx_kpi["TOTAL_REG"].iloc[0] > 0:
    dx = dx_kpi.iloc[0]
    dx_uptime = float(dx["UPTIME_PROM"])
    dx_sla_pct = round(float(dx["SLA_OK"]) / float(dx["TOTAL_REG"]) * 100, 1)
    dx_mttr = float(dx["MTTR_PROM"])
    dx_impacto = float(dx["IMPACTO_M"])

    dx_worst = run_query(f"SELECT SERVICIO, ROUND(AVG(UPTIME_PCT), 2) AS U FROM {TABLE} GROUP BY 1 ORDER BY 2 ASC LIMIT 1")
    dx_top_causa = run_query(f"SELECT CAUSA_PRINCIPAL, COUNT(*) AS N FROM {TABLE} WHERE INCIDENTES_DIA > 0 GROUP BY 1 ORDER BY 2 DESC LIMIT 1")

    if dx_uptime >= 99.5:
        dx_estado = f"Disponibilidad promedio ({color_tag(dx_uptime, 99.5, 99)}) :green[**excelente**]. Cumplimiento SLA: {dx_sla_pct:.1f}%."
    elif dx_uptime >= 99.0:
        dx_estado = f"Disponibilidad ({color_tag(dx_uptime, 99.5, 99)}) en :orange[**zona de atencion**]. MTTR promedio: {dx_mttr:.0f} min."
    else:
        dx_estado = f"Disponibilidad ({color_tag(dx_uptime, 99.5, 99)}) :red[**critica**]. Impacto financiero acumulado: ${dx_impacto:.1f}M COP."

    dx_lines = [dx_estado]
    if not dx_worst.empty:
        dx_lines.append(f"- Servicio con menor uptime: **{dx_worst.iloc[0]['SERVICIO']}** ({color_tag(float(dx_worst.iloc[0]['U']), 99.5, 99)})")
    if not dx_top_causa.empty:
        dx_lines.append(f"- Causa principal mas frecuente: **{dx_top_causa.iloc[0]['CAUSA_PRINCIPAL']}** ({int(dx_top_causa.iloc[0]['N'])} incidentes)")
    dx_lines.append(f"- CPU pico promedio: **{float(dx['CPU_PROM']):.1f}%** — {'Memoria: ' + str(float(dx['MEM_PROM'])) + '%'}")

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
            if dx_uptime < 99.5:
                recs.append("1. :red[**Mejorar disponibilidad**]: uptime por debajo del objetivo 99.5%. Revisar infraestructura critica.")
            if dx_mttr > 30:
                recs.append("2. :orange[**Reducir MTTR**]: tiempo de recuperacion elevado. Automatizar procedimientos de failover.")
            if float(dx["CPU_PROM"]) > 70:
                recs.append("3. :orange[**Escalar recursos**]: CPU pico promedio alto. Considerar capacidad adicional.")
            if dx_impacto > 100:
                recs.append("4. :red[**Mitigar impacto financiero**]: perdidas acumuladas significativas. Priorizar causa raiz principal.")
            if not recs:
                recs.append(":green[**Sistemas operando dentro de parametros optimos.**] Mantener monitoreo preventivo.")
            st.markdown("\n".join(recs))

st.divider()

fc1, fc2, fc3, fc4 = st.columns(4)
with fc1:
    fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="upt_fecha")
    if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
        fecha_inicio, fecha_fin = fecha_rng
    else:
        fecha_inicio, fecha_fin = fmin, fmax
with fc2:
    serv_sel = st.selectbox("Servicio", ["Todos"] + servicios, key="upt_serv")
with fc3:
    dc_sel = st.selectbox("Datacenter", ["Todos"] + datacenters, key="upt_dc")
with fc4:
    causa_sel = st.selectbox("Causa principal", ["Todos"] + causas, key="upt_causa")

WHERE = f"FECHA BETWEEN '{fecha_inicio}' AND '{fecha_fin}'"
if serv_sel != "Todos":
    WHERE += f" AND SERVICIO = '{serv_sel}'"
if dc_sel != "Todos":
    WHERE += f" AND DATACENTER = '{dc_sel}'"
if causa_sel != "Todos":
    WHERE += f" AND CAUSA_PRINCIPAL = '{causa_sel}'"

kpi_df = run_query(f"""
    SELECT
        ROUND(AVG(UPTIME_PCT), 2) AS UPTIME_PROM,
        ROUND(AVG(SLA_OBJETIVO), 2) AS SLA_PROM,
        SUM(CASE WHEN SLA_CUMPLIDO THEN 1 ELSE 0 END) AS SLA_OK,
        COUNT(*) AS TOTAL_REG,
        SUM(INCIDENTES_DIA) AS TOTAL_INCIDENTES,
        ROUND(AVG(MTTR_MINUTOS), 1) AS MTTR_PROM,
        ROUND(AVG(MTBF_MINUTOS), 1) AS MTBF_PROM,
        ROUND(SUM(IMPACTO_FINANCIERO_COP)/1e6, 1) AS IMPACTO_M,
        ROUND(AVG(CPU_PICO_PCT), 1) AS CPU_PROM,
        ROUND(AVG(MEMORIA_PICO_PCT), 1) AS MEM_PROM,
        SUM(TRANSACCIONES_AFECTADAS) AS TX_AFECTADAS,
        SUM(CASE WHEN FAILOVER_ACTIVADO THEN 1 ELSE 0 END) AS FAILOVERS
    FROM {TABLE}
    WHERE {WHERE}
""")

if kpi_df.empty or kpi_df["TOTAL_REG"].iloc[0] == 0:
    st.info("No hay datos para los filtros seleccionados.")
else:
    r = kpi_df.iloc[0]
    total_reg = int(r["TOTAL_REG"])
    uptime_prom = float(r["UPTIME_PROM"])
    sla_ok = int(r["SLA_OK"])
    sla_pct = round(sla_ok / total_reg * 100, 1)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Uptime promedio", f"{uptime_prom:.2f}%")
    k2.metric("Cumplimiento SLA", f"{sla_pct:.1f}%")
    k3.metric("Incidentes totales", f"{int(r['TOTAL_INCIDENTES']):,}")
    k4.metric("Impacto financiero", f"${float(r['IMPACTO_M']):,.1f}M COP")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("MTTR promedio", f"{float(r['MTTR_PROM']):.1f} min")
    k6.metric("MTBF promedio", f"{float(r['MTBF_PROM']):.0f} min")
    k7.metric("Txns afectadas", f"{int(r['TX_AFECTADAS']):,}")
    k8.metric("Failovers activados", f"{int(r['FAILOVERS']):,}")

    st.divider()

    st.markdown("**Tablero de estado por servicio**")
    status_df = run_query(f"""
        SELECT SERVICIO,
               ROUND(AVG(UPTIME_PCT), 2) AS UPTIME_PROM,
               SUM(INCIDENTES_DIA) AS INCIDENTES,
               SUM(MINUTOS_DOWNTIME) AS DOWNTIME_TOTAL,
               SUM(MINUTOS_DEGRADADO) AS DEGRADADO_TOTAL
        FROM {TABLE}
        WHERE {WHERE}
        GROUP BY 1 ORDER BY 2 ASC
    """)
    if not status_df.empty:
        servs = status_df["SERVICIO"].tolist()
        z_vals = []
        text_vals = []
        for _, row in status_df.iterrows():
            u = float(row["UPTIME_PROM"])
            if u >= 99.5:
                z_vals.append(3)
                text_vals.append(f"{u:.2f}% ✓")
            elif u >= 99.0:
                z_vals.append(2)
                text_vals.append(f"{u:.2f}% ⚠")
            else:
                z_vals.append(1)
                text_vals.append(f"{u:.2f}% ✗")

        fig_status = go.Figure(go.Heatmap(
            z=[z_vals],
            x=servs,
            y=["Estado"],
            colorscale=[[0, "#DE350B"], [0.5, "#FFAB00"], [1, "#36B37E"]],
            zmin=1, zmax=3,
            text=[text_vals],
            texttemplate="%{text}",
            textfont=dict(size=12, color="white"),
            hovertemplate="<b>%{x}</b><br>Uptime: %{text}<extra></extra>",
            showscale=False,
        ))
        fig_status.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=140,
            margin=dict(l=80, r=20, t=10, b=60),
            xaxis=dict(tickangle=-30, tickfont=dict(size=11)),
            yaxis=dict(tickfont=dict(size=11)),
        )
        st.plotly_chart(fig_status, use_container_width=True, key="upt_status_board")

    st.markdown("**Calendario de disponibilidad — Uptime por servicio y fecha**")
    cal_df = run_query(f"""
        SELECT FECHA, SERVICIO, ROUND(UPTIME_PCT, 2) AS UPTIME_PCT
        FROM {TABLE}
        WHERE {WHERE}
        ORDER BY FECHA, SERVICIO
    """)
    if not cal_df.empty:
        pivot_cal = cal_df.pivot_table(index="SERVICIO", columns="FECHA", values="UPTIME_PCT", aggfunc="mean")
        pivot_cal = pivot_cal.fillna(0)
        fechas_str = [str(d)[:10] for d in pivot_cal.columns.tolist()]
        fig_cal = go.Figure(go.Heatmap(
            z=pivot_cal.values.tolist(),
            x=fechas_str,
            y=pivot_cal.index.tolist(),
            colorscale=[[0, "#DE350B"], [0.5, "#FFAB00"], [0.95, "#E8F5E9"], [1, "#36B37E"]],
            zmin=95, zmax=100,
            hovertemplate="Servicio: %{y}<br>Fecha: %{x}<br>Uptime: %{z:.2f}%<extra></extra>",
            colorbar=dict(title="Uptime %"),
        ))
        fig_cal.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=450,
            margin=dict(l=140, r=40, t=10, b=80),
            xaxis=dict(tickangle=-45, tickfont=dict(size=9), nticks=30),
            yaxis=dict(tickfont=dict(size=11)),
        )
        st.plotly_chart(fig_cal, use_container_width=True, key="upt_calendar")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Bullet chart — Uptime vs SLA objetivo por servicio**")
        bullet_df = run_query(f"""
            SELECT SERVICIO,
                   ROUND(AVG(UPTIME_PCT), 2) AS UPTIME_PROM,
                   ROUND(AVG(SLA_OBJETIVO), 2) AS SLA_OBJ
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 2
        """)
        if not bullet_df.empty:
            fig_bullet = go.Figure()
            for _, row in bullet_df.iterrows():
                sla = float(row["SLA_OBJ"])
                actual = float(row["UPTIME_PROM"])
                fig_bullet.add_trace(go.Bar(
                    y=[row["SERVICIO"]], x=[100],
                    orientation="h", marker=dict(color="#E0E0E0"),
                    showlegend=False, hoverinfo="skip", width=0.6,
                ))
                fig_bullet.add_trace(go.Bar(
                    y=[row["SERVICIO"]], x=[sla],
                    orientation="h", marker=dict(color="#BDBDBD"),
                    showlegend=False, hoverinfo="skip", width=0.6,
                ))
                bar_color = "#36B37E" if actual >= sla else "#DE350B"
                fig_bullet.add_trace(go.Bar(
                    y=[row["SERVICIO"]], x=[actual],
                    orientation="h", marker=dict(color=bar_color),
                    showlegend=False, width=0.3,
                    hovertemplate=f"<b>{row['SERVICIO']}</b><br>Uptime: {actual:.2f}%<br>SLA: {sla:.2f}%<extra></extra>",
                ))
            fig_bullet.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=140, r=40, t=10, b=40),
                barmode="overlay",
                xaxis=dict(range=[95, 100.5], title="Uptime %", ticksuffix="%"),
                yaxis=dict(tickfont=dict(size=11)),
            )
            st.plotly_chart(fig_bullet, use_container_width=True, key="upt_bullet")

    with col2:
        st.markdown("**MTTR vs MTBF — Analisis por cuadrante**")
        scatter_df = run_query(f"""
            SELECT SERVICIO,
                   ROUND(AVG(MTTR_MINUTOS), 1) AS MTTR,
                   ROUND(AVG(MTBF_MINUTOS), 1) AS MTBF,
                   SUM(INCIDENTES_DIA) AS INCIDENTES
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1
        """)
        if not scatter_df.empty:
            fig_scatter = go.Figure()
            max_inc = max(scatter_df["INCIDENTES"].max(), 1)
            for i, (_, row) in enumerate(scatter_df.iterrows()):
                sz = max(10, (float(row["INCIDENTES"]) / max_inc) * 50)
                fig_scatter.add_trace(go.Scatter(
                    x=[float(row["MTTR"])], y=[float(row["MTBF"])],
                    mode="markers+text", text=[row["SERVICIO"]],
                    textposition="top center", textfont=dict(size=10),
                    marker=dict(size=sz, color=COLORS[i % len(COLORS)], opacity=0.7,
                                line=dict(width=1, color="#11567F")),
                    showlegend=False,
                    hovertemplate=f"<b>{row['SERVICIO']}</b><br>MTTR: {float(row['MTTR']):.1f} min<br>MTBF: {float(row['MTBF']):.0f} min<br>Incidentes: {int(row['INCIDENTES'])}<extra></extra>",
                ))
            mttr_med = scatter_df["MTTR"].median()
            mtbf_med = scatter_df["MTBF"].median()
            fig_scatter.add_hline(y=mtbf_med, line_dash="dash", line_color="#999", opacity=0.5)
            fig_scatter.add_vline(x=mttr_med, line_dash="dash", line_color="#999", opacity=0.5)
            fig_scatter.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=60, r=20, t=10, b=60),
                xaxis=dict(title="MTTR (min) — menor es mejor"),
                yaxis=dict(title="MTBF (min) — mayor es mejor"),
            )
            st.plotly_chart(fig_scatter, use_container_width=True, key="upt_scatter")

    st.markdown("**Impacto financiero acumulado por causa principal**")
    wf_df = run_query(f"""
        SELECT CAUSA_PRINCIPAL,
               ROUND(SUM(IMPACTO_FINANCIERO_COP)/1e6, 1) AS IMPACTO_M
        FROM {TABLE}
        WHERE {WHERE} AND INCIDENTES_DIA > 0
        GROUP BY 1
        ORDER BY 2 DESC
    """)
    if not wf_df.empty:
        fig_wf = go.Figure(go.Waterfall(
            x=wf_df["CAUSA_PRINCIPAL"].tolist(),
            y=wf_df["IMPACTO_M"].tolist(),
            measure=["relative"] * len(wf_df),
            text=[f"${v:.1f}M" for v in wf_df["IMPACTO_M"].tolist()],
            textposition="outside",
            connector=dict(line=dict(color="#11567F", width=1)),
            increasing=dict(marker=dict(color="#DE350B")),
            decreasing=dict(marker=dict(color="#36B37E")),
            totals=dict(marker=dict(color=COLORS[1])),
            hovertemplate="<b>%{x}</b><br>Impacto: $%{y:.1f}M COP<extra></extra>",
        ))
        fig_wf.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=420,
            margin=dict(l=60, r=20, t=10, b=100),
            xaxis=dict(tickangle=-30, tickfont=dict(size=11)),
            yaxis=dict(title="Impacto ($M COP)"),
        )
        st.plotly_chart(fig_wf, use_container_width=True, key="upt_waterfall")

    st.markdown("**Consumo de recursos por datacenter — CPU y memoria pico**")
    gauge_df = run_query(f"""
        SELECT DATACENTER,
               ROUND(AVG(CPU_PICO_PCT), 1) AS CPU_PROM,
               ROUND(AVG(MEMORIA_PICO_PCT), 1) AS MEM_PROM
        FROM {TABLE}
        WHERE {WHERE}
        GROUP BY 1 ORDER BY 1
    """)
    if not gauge_df.empty:
        n_dc = len(gauge_df)
        gauge_cols = st.columns(n_dc)
        for i, (_, row) in enumerate(gauge_df.iterrows()):
            with gauge_cols[i]:
                st.markdown(f"**{row['DATACENTER']}**")
                cpu_val = float(row["CPU_PROM"])
                mem_val = float(row["MEM_PROM"])
                cpu_color = "#36B37E" if cpu_val < 70 else ("#FFAB00" if cpu_val < 85 else "#DE350B")
                mem_color = "#36B37E" if mem_val < 70 else ("#FFAB00" if mem_val < 85 else "#DE350B")

                fig_g = go.Figure()
                fig_g.add_trace(go.Indicator(
                    mode="gauge+number",
                    value=cpu_val,
                    title=dict(text="CPU %", font=dict(size=13)),
                    number=dict(suffix="%", font=dict(size=20)),
                    gauge=dict(
                        axis=dict(range=[0, 100], ticksuffix="%"),
                        bar=dict(color=cpu_color),
                        steps=[
                            dict(range=[0, 70], color="#E8F5E9"),
                            dict(range=[70, 85], color="#FFF8E1"),
                            dict(range=[85, 100], color="#FFEBEE"),
                        ],
                    ),
                    domain=dict(x=[0, 0.45], y=[0, 1]),
                ))
                fig_g.add_trace(go.Indicator(
                    mode="gauge+number",
                    value=mem_val,
                    title=dict(text="Memoria %", font=dict(size=13)),
                    number=dict(suffix="%", font=dict(size=20)),
                    gauge=dict(
                        axis=dict(range=[0, 100], ticksuffix="%"),
                        bar=dict(color=mem_color),
                        steps=[
                            dict(range=[0, 70], color="#E8F5E9"),
                            dict(range=[70, 85], color="#FFF8E1"),
                            dict(range=[85, 100], color="#FFEBEE"),
                        ],
                    ),
                    domain=dict(x=[0.55, 1], y=[0, 1]),
                ))
                fig_g.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=220, margin=dict(l=20, r=20, t=40, b=10),
                )
                st.plotly_chart(fig_g, use_container_width=True, key=f"upt_gauge_{i}")
