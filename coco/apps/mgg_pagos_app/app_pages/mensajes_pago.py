import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.PROCESAMIENTO_TRANSACCIONES.MENSAJES_PAGO"

COLORS = ["#29B5E8", "#FF8B00", "#36B37E", "#6554C0", "#DE350B", "#11567F", "#FFAB00", "#00A3BF"]


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filter_options():
    tipos = run_query(f"SELECT DISTINCT TIPO_MENSAJE FROM {TABLE} ORDER BY 1")["TIPO_MENSAJE"].tolist()
    origenes = run_query(f"SELECT DISTINCT ORIGEN FROM {TABLE} ORDER BY 1")["ORIGEN"].tolist()
    destinos = run_query(f"SELECT DISTINCT DESTINO FROM {TABLE} ORDER BY 1")["DESTINO"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO FROM {TABLE} ORDER BY 1")["ESTADO"].tolist()
    formatos = run_query(f"SELECT DISTINCT FORMATO FROM {TABLE} ORDER BY 1")["FORMATO"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_HORA)::DATE AS FMIN, MAX(FECHA_HORA)::DATE AS FMAX FROM {TABLE}")
    return tipos, origenes, destinos, estados, formatos, fechas


def build_where(fecha_ini, fecha_f, tipo_s, origen_s, destino_s, estado_s, formato_s,
                all_tipos, all_orig, all_dest, all_est, all_fmt):
    clauses = [f"FECHA_HORA::DATE BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if tipo_s and tipo_s != "Todos" and tipo_s in all_tipos:
        clauses.append(f"TIPO_MENSAJE = '{tipo_s}'")
    if origen_s and origen_s != "Todos" and origen_s in all_orig:
        clauses.append(f"ORIGEN = '{origen_s}'")
    if destino_s and destino_s != "Todos" and destino_s in all_dest:
        clauses.append(f"DESTINO = '{destino_s}'")
    if estado_s and estado_s != "Todos" and estado_s in all_est:
        clauses.append(f"ESTADO = '{estado_s}'")
    if formato_s and formato_s != "Todos" and formato_s in all_fmt:
        clauses.append(f"FORMATO = '{formato_s}'")
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


st.header(":material/mail: Mensajes de pago")
st.caption("Mensajeria financiera ISO8583/ISO20022 intercambiada entre entidades del ecosistema de pagos. Salud de comunicaciones interbancarias y errores de formato.")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Dificultad para detectar y diagnosticar errores en la mensajeria financiera entre switch, redes, emisores y adquirentes que causan transacciones fallidas.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Cada mensaje ISO8583/ISO20022 se registra con tipo, origen, destino, estado, validacion MAC y tiempo de respuesta. Se detectan errores de parseo y timeouts.")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Reducir transacciones fallidas por errores de comunicacion, cumplir con estandares de mensajeria y diagnosticar problemas de conectividad antes del impacto.")

    tipos, origenes, destinos, estados, formatos, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_HORA::DATE BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL_MSG,
            SUM(CASE WHEN ESTADO='Procesado' THEN 1 ELSE 0 END) AS PROCESADOS,
            SUM(CASE WHEN ERROR_PARSEO THEN 1 ELSE 0 END) AS ERRORES_PARSEO,
            SUM(CASE WHEN ERROR_VALIDACION THEN 1 ELSE 0 END) AS ERRORES_VALID,
            SUM(CASE WHEN ESTADO='Timeout' THEN 1 ELSE 0 END) AS TIMEOUTS,
            ROUND(AVG(TIEMPO_PROCESAMIENTO_MS), 0) AS TIEMPO_PROM,
            ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS SLA_PCT
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL_MSG"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL_MSG"])
        dx_errores = int(dx["ERRORES_PARSEO"]) + int(dx["ERRORES_VALID"])
        dx_tasa_error = round(dx_errores / dx_total * 100, 2)
        dx_sla = float(dx["SLA_PCT"])
        dx_timeouts = int(dx["TIMEOUTS"])

        dx_worst_origen = run_query(f"""
            SELECT ORIGEN, ROUND(SUM(CASE WHEN ERROR_PARSEO OR ERROR_VALIDACION THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) AS TASA_ERR
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_worst_destino = run_query(f"""
            SELECT DESTINO, COUNT(*) AS N
            FROM {TABLE} WHERE {dx_where} AND ESTADO='Timeout'
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)

        if dx_tasa_error <= 2:
            dx_estado = f"Tasa de error en mensajeria ({color_tag(dx_tasa_error, 2, 5, fmt='{:.2f}%', inverse=True)}) :green[**saludable**]. SLA: {dx_sla:.1f}%."
        elif dx_tasa_error <= 5:
            dx_estado = f"Tasa de error ({color_tag(dx_tasa_error, 2, 5, fmt='{:.2f}%', inverse=True)}) en :orange[**zona de atencion**]. SLA: {dx_sla:.1f}%."
        else:
            dx_estado = f"Tasa de error ({color_tag(dx_tasa_error, 2, 5, fmt='{:.2f}%', inverse=True)}) :red[**critica**]. Impacto directo en transacciones."

        dx_lines = [dx_estado]
        if not dx_worst_origen.empty:
            wo = dx_worst_origen.iloc[0]
            dx_lines.append(f"- Origen mas errores: **{wo['ORIGEN']}** ({color_tag(float(wo['TASA_ERR']), 2, 5, fmt='{:.2f}%', inverse=True)})")
        if not dx_worst_destino.empty:
            wd = dx_worst_destino.iloc[0]
            dx_lines.append(f"- Destino mas timeouts: **{wd['DESTINO']}** ({int(wd['N']):,} timeouts)")
        dx_lines.append(f"- Timeouts totales: **{dx_timeouts:,}** {'— :green[**bajo**]' if dx_timeouts < dx_total*0.02 else '— :red[**elevado**]'}")
        dx_lines.append(f"- Cumplimiento SLA: {color_tag(dx_sla, 95, 85)}")

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
                if dx_tasa_error > 5:
                    recs.append("1. :red[**Revisar parseo de mensajes**]: tasa de error critica afecta transacciones.")
                if dx_sla < 90:
                    recs.append("2. :red[**Mejorar SLA**]: cumplimiento por debajo del objetivo impacta confiabilidad.")
                if dx_timeouts > dx_total * 0.03:
                    recs.append("3. :orange[**Investigar timeouts**]: porcentaje elevado indica problemas de conectividad.")
                if not dx_worst_origen.empty and float(dx_worst_origen.iloc[0]["TASA_ERR"]) > 5:
                    recs.append(f"4. :orange[**Revisar {dx_worst_origen.iloc[0]['ORIGEN']}**]: origen con mayor tasa de error.")
                if not recs:
                    recs.append(":green[**Mensajeria financiera operando dentro de parametros optimos.**] Mantener monitoreo de SLA.")
                st.markdown("\n".join(recs))

    st.divider()

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fecha_rng = st.date_input("Periodo", value=(fmin, fmax), min_value=fmin, max_value=fmax, key="msg_fecha")
        if isinstance(fecha_rng, (list, tuple)) and len(fecha_rng) == 2:
            fecha_inicio, fecha_fin = fecha_rng
        else:
            fecha_inicio, fecha_fin = fmin, fmax
    with fc2:
        tipo_sel = st.selectbox("Tipo mensaje", ["Todos"] + tipos, key="msg_tipo")
    with fc3:
        origen_sel = st.selectbox("Origen", ["Todos"] + origenes, key="msg_origen")
    with fc4:
        destino_sel = st.selectbox("Destino", ["Todos"] + destinos, key="msg_destino")
    with fc5:
        estado_sel = st.selectbox("Estado", ["Todos"] + estados, key="msg_estado")

    formato_sel = "Todos"

    WHERE = build_where(fecha_inicio, fecha_fin, tipo_sel, origen_sel, destino_sel, estado_sel, formato_sel,
                        tipos, origenes, destinos, estados, formatos)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL_MSG,
            SUM(CASE WHEN ESTADO='Procesado' THEN 1 ELSE 0 END) AS PROCESADOS,
            SUM(CASE WHEN ERROR_PARSEO THEN 1 ELSE 0 END) AS ERRORES_PARSEO,
            SUM(CASE WHEN ERROR_VALIDACION THEN 1 ELSE 0 END) AS ERRORES_VALID,
            SUM(CASE WHEN ESTADO='Timeout' THEN 1 ELSE 0 END) AS TIMEOUTS,
            ROUND(AVG(TIEMPO_PROCESAMIENTO_MS), 0) AS TIEMPO_PROM,
            ROUND(AVG(TAMANO_BYTES), 0) AS TAMANO_PROM,
            ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS SLA_PCT
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL_MSG"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL_MSG"])
        procesados = int(r["PROCESADOS"])
        errores_p = int(r["ERRORES_PARSEO"])
        errores_v = int(r["ERRORES_VALID"])
        timeouts = int(r["TIMEOUTS"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Mensajes totales", f"{total:,}")
        k2.metric("Procesados", f"{procesados:,}")
        k3.metric("Cumplimiento SLA", f"{float(r['SLA_PCT']):.1f}%")
        k4.metric("Tiempo procesamiento", f"{float(r['TIEMPO_PROM']):.0f} ms")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Errores parseo", f"{errores_p:,}", delta=f"{round(errores_p/total*100,2):.2f}%", delta_color="inverse")
        k6.metric("Errores validacion", f"{errores_v:,}", delta=f"{round(errores_v/total*100,2):.2f}%", delta_color="inverse")
        k7.metric("Timeouts", f"{timeouts:,}", delta=f"{round(timeouts/total*100,2):.2f}%", delta_color="inverse")
        k8.metric("Tamano prom", f"{float(r['TAMANO_PROM']):.0f} bytes")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_HORA) AS MES,
                   COUNT(*) AS TOTAL,
                   SUM(CASE WHEN ERROR_PARSEO OR ERROR_VALIDACION THEN 1 ELSE 0 END) AS ERRORES,
                   SUM(CASE WHEN ESTADO='Timeout' THEN 1 ELSE 0 END) AS TIMEOUTS
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Volumen y errores**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                err_pct = [round(float(e)/float(t)*100, 2) if float(t) > 0 else 0
                           for e, t in zip(trend_df["ERRORES"], trend_df["TOTAL"])]
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["TOTAL"].tolist(), name="Mensajes",
                    fill="tozeroy", line=dict(color=COLORS[0], width=2),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=err_pct, name="% Error",
                    yaxis="y2", line=dict(color=COLORS[4], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="Mensajes"),
                    yaxis2=dict(title="% Error", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Mensajes por origen y tipo**")
            tree_df = run_query(f"""
                SELECT ORIGEN, TIPO_MENSAJE, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
            """)
            if tree_df.empty:
                st.info("Sin datos.")
            else:
                labels, parents, values, colors_t = [], [], [], []
                orig_totals = tree_df.groupby("ORIGEN")["N"].sum().sort_values(ascending=False)
                for i, (origen, orig_total) in enumerate(orig_totals.items()):
                    labels.append(str(origen))
                    parents.append("")
                    values.append(int(orig_total))
                    colors_t.append(COLORS[i % len(COLORS)])
                    sub = tree_df[tree_df["ORIGEN"] == origen]
                    for _, row in sub.iterrows():
                        labels.append(str(row["TIPO_MENSAJE"]))
                        parents.append(str(origen))
                        values.append(int(row["N"]))
                        colors_t.append(COLORS[i % len(COLORS)])
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    marker=dict(colors=colors_t),
                    textinfo="label+value+percent parent",
                    hovertemplate="<b>%{label}</b><br>Mensajes: %{value:,}<br>%{percentParent:.1%} del padre<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("**Heatmap — Estado de mensajes por origen y destino**")
        heat_df = run_query(f"""
            SELECT ORIGEN, DESTINO, COUNT(*) AS N
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="ORIGEN", columns="DESTINO", values="N", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E3F2FD"], [0.3, "#29B5E8"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"{int(v)}" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Origen: %{y}<br>Destino: %{x}<br>Mensajes: %{z:,}<extra></extra>",
                colorbar=dict(title="Mensajes"),
            ))
            fig_heat.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF",
                height=500, margin=dict(l=180, r=40, t=30, b=120),
                xaxis=dict(tickangle=-45, tickfont=dict(size=11)),
                yaxis=dict(tickfont=dict(size=11)),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        col3, col4 = st.columns(2)

        with col3:
            st.markdown("**Funnel — Estado de procesamiento**")
            recibidos = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND ESTADO='Recibido'")
            enviados = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND ESTADO='Enviado'")
            reenviados = run_query(f"SELECT COUNT(*) AS N FROM {TABLE} WHERE {WHERE} AND ESTADO='Reenviado'")
            funnel_data = [
                ("Total mensajes", total),
                ("Procesados", procesados),
                ("Recibidos", int(recibidos.iloc[0]["N"]) if not recibidos.empty else 0),
                ("Enviados", int(enviados.iloc[0]["N"]) if not enviados.empty else 0),
                ("Error/Timeout", errores_p + errores_v + timeouts),
            ]
            fig_funnel = go.Figure(go.Funnel(
                y=[f[0] for f in funnel_data],
                x=[f[1] for f in funnel_data],
                textinfo="value+percent initial",
                marker=dict(color=[COLORS[0], COLORS[2], COLORS[5], COLORS[3], COLORS[4]]),
                hovertemplate="%{y}: %{x:,}<extra></extra>",
            ))
            fig_funnel.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=10, r=10, t=30, b=10),
            )
            st.plotly_chart(fig_funnel, use_container_width=True)

        with col4:
            st.markdown("**Distribucion por formato**")
            fmt_df = run_query(f"""
                SELECT FORMATO, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if fmt_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=fmt_df["FORMATO"].tolist(),
                    values=fmt_df["N"].tolist(),
                    hole=0.5,
                    marker=dict(colors=COLORS[:len(fmt_df)]),
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
                ))
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.markdown("**Radar — Salud por protocolo**")
        proto_df = run_query(f"""
            SELECT PROTOCOLO,
                   ROUND(SUM(CASE WHEN DENTRO_SLA THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS SLA_PCT,
                   ROUND(AVG(TIEMPO_PROCESAMIENTO_MS), 0) AS TIEMPO_PROM,
                   ROUND(SUM(CASE WHEN MAC_VALIDADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS MAC_PCT,
                   ROUND(SUM(CASE WHEN ERROR_PARSEO OR ERROR_VALIDACION THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS ERROR_PCT,
                   ROUND(AVG(CAMPOS_PRESENTES), 0) AS CAMPOS_PROM
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1
        """)
        if not proto_df.empty:
            categories = ["SLA %", "MAC valido %", "Error %", "Campos prom", "Tiempo prom"]
            fig_radar = go.Figure()
            for i, (_, row) in enumerate(proto_df.iterrows()):
                vals = [float(row["SLA_PCT"]), float(row["MAC_PCT"]), float(row["ERROR_PCT"]),
                        float(row["CAMPOS_PROM"]), float(row["TIEMPO_PROM"])]
                fig_radar.add_trace(go.Scatterpolar(
                    r=vals + [vals[0]],
                    theta=categories + [categories[0]],
                    fill="toself",
                    name=str(row["PROTOCOLO"]),
                    line=dict(color=COLORS[i % len(COLORS)]),
                ))
            fig_radar.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=420,
                margin=dict(l=60, r=60, t=30, b=30),
                polar=dict(radialaxis=dict(visible=True)),
                legend=dict(orientation="h", yanchor="bottom", y=-0.15),
            )
            st.plotly_chart(fig_radar, use_container_width=True)

with tab2:
    st.markdown("**Simulador de salud de mensajeria**")
    st.caption("Estime la tasa de error y el cumplimiento de SLA ajustando parametros de la infraestructura de mensajeria financiera.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_volumen = st.slider("Volumen de mensajes/hora (miles)", 1, 100, 15, key="msg_sim_vol",
                                help="Mayor volumen puede saturar parsers y aumentar errores")
        sim_tamano = st.slider("Tamano promedio de mensaje (bytes)", 100, 5000, 800, step=100, key="msg_sim_tam",
                               help="Mensajes mas grandes tardan mas en procesar y validar")
        sim_campos = st.slider("Campos presentes promedio", 10, 128, 65, key="msg_sim_campos",
                               help="Menos campos = menos validaciones, pero mas rechazos por datos faltantes")
        sim_mac = st.slider("% Mensajes con MAC valido", 50, 100, 92, key="msg_sim_mac",
                            help="MAC invalido genera errores de validacion y rechazos de seguridad")
        sim_timeout_sla = st.slider("SLA de timeout (ms)", 100, 5000, 1000, step=100, key="msg_sim_sla",
                                    help="SLA mas estricto genera mas timeouts pero mejor experiencia")
        sim_conexiones = st.slider("Conexiones concurrentes activas", 1, 50, 10, key="msg_sim_conn",
                                   help="Mas conexiones = mas capacidad pero mayor riesgo de colision")

    def calcular_salud_msg(volumen, tamano, campos, mac, sla_ms, conn):
        s_vol = max(0.0, 1.0 - (volumen / 100) * 0.6)
        s_tam = max(0.0, 1.0 - (tamano / 5000) * 0.5)
        s_campos = min(1.0, campos / 128)
        s_mac = mac / 100
        s_sla = min(1.0, sla_ms / 5000)
        s_conn = max(0.0, 1.0 - (conn / 50) * 0.4)
        score = (0.20 * s_vol + 0.12 * s_tam + 0.13 * s_campos + 0.22 * s_mac + 0.18 * s_sla + 0.15 * s_conn)
        tasa_error = max(0.5, (1 - score) * 15)
        sla_pct = min(99.5, 70 + score * 29)
        return round(tasa_error, 2), round(sla_pct, 1)

    err_pred, sla_pred = calcular_salud_msg(sim_volumen, sim_tamano, sim_campos, sim_mac, sim_timeout_sla, sim_conexiones)

    with col_result:
        if err_pred <= 2:
            nivel = "Saludable"
            color_nivel = "#36B37E"
            interpretacion = "Mensajeria en estado optimo. Tasa de error baja y SLA cumplido. Mantener parametros actuales y monitoreo continuo."
        elif err_pred <= 5:
            nivel = "Atencion"
            color_nivel = "#29B5E8"
            interpretacion = "Mensajeria funcional con tasa de error moderada. Revisar volumen de mensajes y validaciones MAC para optimizar."
        elif err_pred <= 8:
            nivel = "Riesgo"
            color_nivel = "#FFAB00"
            interpretacion = "Tasa de error elevada. Posible saturacion o problemas de parseo. Reducir volumen, optimizar tamano de mensajes o ampliar SLA."
        else:
            nivel = "Critico"
            color_nivel = "#DE350B"
            interpretacion = "Infraestructura de mensajeria en estado critico. Accion inmediata: reducir carga, validar MAC, revisar conexiones y parsers."

        st.metric("Tasa de error estimada", f"{err_pred:.2f}%")
        st.metric("Cumplimiento SLA estimado", f"{sla_pred:.1f}%")
        if err_pred <= 2:
            st.markdown(f"**Nivel:** :green[**{nivel}**]")
        elif err_pred <= 5:
            st.markdown(f"**Nivel:** :blue[**{nivel}**]")
        elif err_pred <= 8:
            st.markdown(f"**Nivel:** :orange[**{nivel}**]")
        else:
            st.markdown(f"**Nivel:** :red[**{nivel}**]")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(err_pred),
            number=dict(suffix="%", font=dict(size=36)),
            gauge=dict(
                axis=dict(range=[0, 15], ticksuffix="%"),
                bar=dict(color=color_nivel),
                steps=[
                    dict(range=[0, 2], color="#E8F5E9"),
                    dict(range=[2, 5], color="#E3F2FD"),
                    dict(range=[5, 8], color="#FFF3E0"),
                    dict(range=[8, 15], color="#FFEBEE"),
                ],
                threshold=dict(line=dict(color="#DE350B", width=3), thickness=0.8, value=5),
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

    if "msg_escenarios" not in st.session_state:
        st.session_state.msg_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="msg_save"):
        if len(st.session_state.msg_escenarios) >= 3:
            st.session_state.msg_escenarios.pop(0)
        st.session_state.msg_escenarios.append({
            "Vol (K msg/h)": sim_volumen,
            "Tamano (bytes)": sim_tamano,
            "Campos prom": sim_campos,
            "% MAC valido": sim_mac,
            "SLA timeout (ms)": sim_timeout_sla,
            "Conexiones": sim_conexiones,
            "Tasa error": f"{err_pred:.2f}%",
            "SLA estimado": f"{sla_pred:.1f}%",
            "Nivel": nivel,
        })
        st.rerun()

    if st.session_state.msg_escenarios:
        esc_df = pd.DataFrame(st.session_state.msg_escenarios)
        esc_df.index = [f"Escenario {i+1}" for i in range(len(esc_df))]
        st.dataframe(esc_df.T, use_container_width=True)

        if st.button("Limpiar escenarios", key="msg_clear"):
            st.session_state.msg_escenarios = []
            st.rerun()
    else:
        st.caption("Aun no hay escenarios guardados. Ajuste los sliders y presione 'Guardar escenario actual'.")
