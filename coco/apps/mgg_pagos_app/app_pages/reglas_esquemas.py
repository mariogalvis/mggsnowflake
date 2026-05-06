import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from decimal import Decimal

TABLE = "MGG_PAGOS.CUMPLIMIENTO_Y_ESQUEMAS.REGLAS_ESQUEMAS"

COLORS = ["#29B5E8", "#11567F", "#71D4F0", "#0E3A53", "#A3E4F7", "#1B8BBF", "#5BC3E8", "#083248"]


from app_pages.conn_helper import run_query



@st.cache_data(ttl=300, show_spinner=False)
def get_filters():
    redes = run_query(f"SELECT DISTINCT RED_ESQUEMA FROM {TABLE} ORDER BY 1")["RED_ESQUEMA"].tolist()
    tipos = run_query(f"SELECT DISTINCT TIPO_REGLA FROM {TABLE} ORDER BY 1")["TIPO_REGLA"].tolist()
    estados = run_query(f"SELECT DISTINCT ESTADO_CUMPLIMIENTO FROM {TABLE} ORDER BY 1")["ESTADO_CUMPLIMIENTO"].tolist()
    impactos = run_query(f"SELECT DISTINCT IMPACTO_NEGOCIO FROM {TABLE} ORDER BY 1")["IMPACTO_NEGOCIO"].tolist()
    marcos = run_query(f"SELECT DISTINCT MARCO_NORMATIVO FROM {TABLE} ORDER BY 1")["MARCO_NORMATIVO"].tolist()
    return redes, tipos, estados, impactos, marcos


def build_where(red_s, tipo_s, estado_s, impacto_s, marco_s, all_r, all_t, all_e, all_i, all_m):
    clauses = ["1=1"]
    if red_s and red_s != "Todos" and red_s in all_r:
        clauses.append(f"RED_ESQUEMA = '{red_s}'")
    if tipo_s and tipo_s != "Todos" and tipo_s in all_t:
        clauses.append(f"TIPO_REGLA = '{tipo_s}'")
    if estado_s and estado_s != "Todos" and estado_s in all_e:
        clauses.append(f"ESTADO_CUMPLIMIENTO = '{estado_s}'")
    if impacto_s and impacto_s != "Todos" and impacto_s in all_i:
        clauses.append(f"IMPACTO_NEGOCIO = '{impacto_s}'")
    if marco_s and marco_s != "Todos" and marco_s in all_m:
        clauses.append(f"MARCO_NORMATIVO = '{marco_s}'")
    return " AND ".join(clauses)


def color_tag(value, good, bad, fmt="{:.1f}%", inverse=False):
    v = float(value)
    if not inverse:
        return f":green[**{fmt.format(v)}**]" if v >= good else f":orange[**{fmt.format(v)}**]" if v >= bad else f":red[**{fmt.format(v)}**]"
    else:
        return f":green[**{fmt.format(v)}**]" if v <= good else f":orange[**{fmt.format(v)}**]" if v <= bad else f":red[**{fmt.format(v)}**]"


st.header(":material/gavel: Reglas de esquemas")
st.caption("Monitoreo de cumplimiento de reglas de esquemas de pago (Visa, Mastercard, Redeban, Credibanco). Excepciones, multas potenciales y estado de cumplimiento.")

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/lightbulb: Que resuelve**")
        st.markdown("Riesgo de sanciones y multas por incumplimiento de reglas de los esquemas de pago. Visibilidad centralizada del estado de cada regla.")
with c2:
    with st.container(border=True):
        st.markdown("**:material/settings: Como funciona**")
        st.markdown("Cada regla de esquema se monitorea con tasa de cumplimiento, excepciones detectadas, resueltas, multa potencial y fechas de vigencia. Genera alertas por reglas proximas a vencer.")
with c3:
    with st.container(border=True):
        st.markdown("**:material/trending_up: Valor de negocio**")
        st.markdown("Evitar multas millonarias por incumplimiento, reducir excepciones pendientes y mantener certificaciones al dia con los esquemas.")

redes, tipos, estados, impactos, marcos = get_filters()

dx_kpi = run_query(f"""
    SELECT ROUND(AVG(TASA_CUMPLIMIENTO)*100,1) AS TASA_CUMPL,
           SUM(MULTA_POTENCIAL_COP) AS MULTA_TOTAL,
           SUM(EXCEPCIONES_DETECTADAS) AS EXCEP_DET,
           SUM(EXCEPCIONES_RESUELTAS) AS EXCEP_RES,
           COUNT(*) AS TOTAL
    FROM {TABLE}
""")

if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
    d = dx_kpi.iloc[0]
    dx_tasa = float(d["TASA_CUMPL"])
    dx_multa_b = float(d["MULTA_TOTAL"]) / 1e9
    dx_excep_det = int(d["EXCEP_DET"])
    dx_excep_res = int(d["EXCEP_RES"])
    dx_excep_pend = dx_excep_det - dx_excep_res

    dx_worst_red = run_query(f"""
        SELECT RED_ESQUEMA, ROUND(AVG(TASA_CUMPLIMIENTO)*100,1) AS TASA
        FROM {TABLE} GROUP BY 1 ORDER BY 2 ASC LIMIT 1
    """)
    dx_top_multa = run_query(f"""
        SELECT DESCRIPCION_REGLA, MULTA_POTENCIAL_COP/1e6 AS MULTA_MM
        FROM {TABLE} ORDER BY 2 DESC LIMIT 1
    """)

    dx_lines = []
    if dx_tasa >= 95:
        dx_lines.append(f"La tasa de cumplimiento general es {color_tag(dx_tasa, 95, 85)}, :green[**excelente**] para el portafolio de reglas.")
    elif dx_tasa >= 85:
        dx_lines.append(f"La tasa de cumplimiento general es {color_tag(dx_tasa, 95, 85)}, :orange[**requiere atencion**] en algunas reglas.")
    else:
        dx_lines.append(f"La tasa de cumplimiento general es {color_tag(dx_tasa, 95, 85)}, :red[**critica**]. Riesgo de sanciones inminente.")
    dx_lines.append(f"- Multa potencial acumulada: :red[**${dx_multa_b:.1f}B COP**]")
    dx_lines.append(f"- Excepciones pendientes: :orange[**{dx_excep_pend:,}**] de {dx_excep_det:,} detectadas")
    if not dx_worst_red.empty:
        dx_lines.append(f"- Red con menor cumplimiento: :red[**{dx_worst_red.iloc[0]['RED_ESQUEMA']}**] ({float(dx_worst_red.iloc[0]['TASA']):.1f}%)")
    if not dx_top_multa.empty:
        dx_lines.append(f"- Regla de mayor riesgo: :red[**{dx_top_multa.iloc[0]['DESCRIPCION_REGLA'][:50]}...**] (${float(dx_top_multa.iloc[0]['MULTA_MM']):,.0f}M)")

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
            if dx_tasa < 95:
                recs.append("1. :red[**Cerrar excepciones pendientes**] priorizando reglas con mayor multa potencial.")
            if not dx_worst_red.empty and float(dx_worst_red.iloc[0]['TASA']) < 90:
                recs.append(f"2. :orange[**Focus en {dx_worst_red.iloc[0]['RED_ESQUEMA']}**]: plan de remediacion para mejorar cumplimiento.")
            recs.append("3. :blue[**Automatizar validaciones**] para reducir excepciones recurrentes.")
            recs.append("4. :green[**Revisar reglas por vencer**] y planificar renovacion de certificaciones.")
            st.markdown("\n".join(recs))

st.divider()

fc1, fc2, fc3, fc4, fc5 = st.columns(5)
with fc1:
    red_sel = st.selectbox("Red esquema", ["Todos"] + redes, key="reg_red")
with fc2:
    tipo_sel = st.selectbox("Tipo regla", ["Todos"] + tipos, key="reg_tipo")
with fc3:
    estado_sel = st.selectbox("Estado", ["Todos"] + estados, key="reg_estado")
with fc4:
    impacto_sel = st.selectbox("Impacto", ["Todos"] + impactos, key="reg_impacto")
with fc5:
    marco_sel = st.selectbox("Marco normativo", ["Todos"] + marcos, key="reg_marco")

WHERE = build_where(red_sel, tipo_sel, estado_sel, impacto_sel, marco_sel,
                    redes, tipos, estados, impactos, marcos)

kpi_df = run_query(f"""
    SELECT COUNT(*) AS TOTAL_REGLAS,
           ROUND(AVG(TASA_CUMPLIMIENTO)*100,1) AS TASA_CUMPL,
           SUM(MULTA_POTENCIAL_COP) AS MULTA_TOTAL,
           SUM(EXCEPCIONES_DETECTADAS) AS EXCEP_DET,
           SUM(EXCEPCIONES_RESUELTAS) AS EXCEP_RES,
           SUM(TRANSACCIONES_EXCEPCION_DIA) AS TXN_EXCEP,
           SUM(MONTO_EXCEPCION_DIA_COP) AS MONTO_EXCEP,
           SUM(CASE WHEN REQUIERE_CERTIFICACION THEN 1 ELSE 0 END) AS CERT_REQ
    FROM {TABLE} WHERE {WHERE}
""")

if kpi_df.empty or kpi_df["TOTAL_REGLAS"].iloc[0] == 0:
    st.info("No hay datos para los filtros seleccionados.")
else:
    r = kpi_df.iloc[0]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total reglas", f"{int(r['TOTAL_REGLAS']):,}")
    k2.metric("Tasa cumplimiento", f"{float(r['TASA_CUMPL']):.1f}%")
    k3.metric("Multa potencial", f"${float(r['MULTA_TOTAL'])/1e9:.1f}B COP")
    k4.metric("Excepciones detectadas", f"{int(r['EXCEP_DET']):,}")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Excepciones resueltas", f"{int(r['EXCEP_RES']):,}")
    k6.metric("Excepciones pendientes", f"{int(r['EXCEP_DET']) - int(r['EXCEP_RES']):,}")
    k7.metric("Txns en excepcion/dia", f"{int(r['TXN_EXCEP']):,}")
    k8.metric("Requieren certificacion", f"{int(r['CERT_REQ']):,}")

    st.divider()

    st.markdown("**Gauges de Cumplimiento por Red Esquema**")
    gauge_df = run_query(f"""
        SELECT RED_ESQUEMA, ROUND(AVG(TASA_CUMPLIMIENTO)*100,1) AS TASA
        FROM {TABLE} WHERE {WHERE}
        GROUP BY 1 ORDER BY 1
    """)
    if not gauge_df.empty:
        n_gauges = len(gauge_df)
        cols = st.columns(min(n_gauges, 6))
        for i, (_, row) in enumerate(gauge_df.iterrows()):
            with cols[i % len(cols)]:
                tasa_v = float(row["TASA"])
                bar_color = "#36B37E" if tasa_v >= 95 else "#FFAB00" if tasa_v >= 85 else "#DE350B"
                fig_g = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=tasa_v,
                    number=dict(suffix="%", font=dict(size=24)),
                    title=dict(text=str(row["RED_ESQUEMA"]), font=dict(size=12)),
                    gauge=dict(
                        axis=dict(range=[0, 100], ticksuffix="%"),
                        bar=dict(color=bar_color),
                        steps=[
                            dict(range=[0, 85], color="#FFCDD2"),
                            dict(range=[85, 95], color="#FFF3E0"),
                            dict(range=[95, 100], color="#E8F5E9"),
                        ],
                        threshold=dict(line=dict(color="#DE350B", width=2), thickness=0.8, value=95),
                    ),
                ))
                fig_g.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF",
                    height=200, margin=dict(l=20, r=20, t=40, b=10),
                )
                st.plotly_chart(fig_g, use_container_width=True, key=f"reg_gauge_{i}")

    st.divider()

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Exposicion de Riesgo — Sunburst**")
        sun_df = run_query(f"""
            SELECT RED_ESQUEMA, TIPO_REGLA, IMPACTO_NEGOCIO,
                   SUM(MULTA_POTENCIAL_COP)/1e6 AS MULTA_MM
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1,2,3 ORDER BY 4 DESC
        """)
        if not sun_df.empty:
            labels = []
            parents = []
            values = []
            red_totals = sun_df.groupby("RED_ESQUEMA")["MULTA_MM"].sum().to_dict()
            tipo_by_red = sun_df.groupby(["RED_ESQUEMA", "TIPO_REGLA"])["MULTA_MM"].sum().to_dict()
            for red in red_totals:
                labels.append(str(red))
                parents.append("")
                values.append(float(red_totals[red]))
            for (red, tipo), val in tipo_by_red.items():
                labels.append(str(tipo))
                parents.append(str(red))
                values.append(float(val))
            for _, row in sun_df.iterrows():
                labels.append(str(row["IMPACTO_NEGOCIO"]))
                parents.append(str(row["TIPO_REGLA"]))
                values.append(float(row["MULTA_MM"]))
            fig_sun = go.Figure(go.Sunburst(
                labels=labels, parents=parents, values=values,
                branchvalues="total",
                marker=dict(colorscale=[[0, "#A3E4F7"], [0.5, "#FFAB00"], [1, "#DE350B"]],
                            line=dict(width=2, color="white")),
                hovertemplate="<b>%{label}</b><br>Multa: $%{value:,.0f}M COP<br>%{percentRoot:.1%}<extra></extra>",
            ))
            fig_sun.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=450,
                margin=dict(l=5, r=5, t=30, b=5),
            )
            st.plotly_chart(fig_sun, use_container_width=True, key="reg_sunburst")

    with col_b:
        st.markdown("**Funnel — Resolucion de Excepciones**")
        funnel_data = run_query(f"""
            SELECT SUM(EXCEPCIONES_DETECTADAS) AS DETECTADAS,
                   SUM(EXCEPCIONES_RESUELTAS) AS RESUELTAS,
                   SUM(EXCEPCIONES_DETECTADAS) - SUM(EXCEPCIONES_RESUELTAS) AS PENDIENTES,
                   SUM(CASE WHEN ESTADO_CUMPLIMIENTO = 'Cumple' THEN EXCEPCIONES_DETECTADAS ELSE 0 END) AS EN_CUMPLE,
                   SUM(CASE WHEN ESTADO_CUMPLIMIENTO = 'No cumple' THEN EXCEPCIONES_DETECTADAS ELSE 0 END) AS NO_CUMPLE
            FROM {TABLE} WHERE {WHERE}
        """)
        if not funnel_data.empty:
            fd = funnel_data.iloc[0]
            steps = ["Excepciones detectadas", "En revision", "Resueltas", "Pendientes"]
            vals = [int(fd["DETECTADAS"]),
                    max(0, int(fd["DETECTADAS"]) - int(fd["RESUELTAS"]) // 2),
                    int(fd["RESUELTAS"]),
                    max(0, int(fd["PENDIENTES"]))]
            colors = [COLORS[0], "#FFAB00", "#36B37E", "#DE350B"]
            fig_funnel = go.Figure(go.Funnel(
                y=steps, x=vals,
                textinfo="value+percent initial",
                texttemplate="%{value:,} (%{percentInitial:.1%})",
                marker=dict(color=colors, line=dict(width=1, color="white")),
                connector=dict(line=dict(color="#DFE1E6", width=1)),
            ))
            fig_funnel.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=450,
                margin=dict(l=180, r=20, t=30, b=40),
            )
            st.plotly_chart(fig_funnel, use_container_width=True, key="reg_funnel")

    st.divider()

    st.markdown("**Timeline de Vigencia de Reglas**")
    tl_df = run_query(f"""
        SELECT DESCRIPCION_REGLA, RED_ESQUEMA, FECHA_EFECTIVA, FECHA_VENCIMIENTO,
               ESTADO_CUMPLIMIENTO, PRIORIDAD_IMPLEMENTACION
        FROM {TABLE} WHERE {WHERE}
        ORDER BY FECHA_VENCIMIENTO ASC
        LIMIT 20
    """)
    if not tl_df.empty:
        est_colors = {"Cumple": "#36B37E", "No cumple": "#DE350B", "En proceso": "#FFAB00",
                      "Parcial": "#FF8B00", "Exento": "#29B5E8"}
        fig_tl = go.Figure()
        for i, (_, row) in enumerate(tl_df.iterrows()):
            desc = str(row["DESCRIPCION_REGLA"])[:40] + "..."
            f_eff = pd.to_datetime(row["FECHA_EFECTIVA"])
            f_ven = pd.to_datetime(row["FECHA_VENCIMIENTO"])
            estado = str(row["ESTADO_CUMPLIMIENTO"])
            fig_tl.add_trace(go.Bar(
                x=[(f_ven - f_eff).days],
                y=[desc],
                base=[f_eff],
                orientation="h",
                marker=dict(color=est_colors.get(estado, COLORS[0])),
                name=estado,
                showlegend=i < 5,
                hovertemplate=f"<b>{desc}</b><br>Red: {row['RED_ESQUEMA']}<br>Desde: {f_eff.strftime('%Y-%m-%d')}<br>Hasta: {f_ven.strftime('%Y-%m-%d')}<br>Estado: {estado}<extra></extra>",
            ))
        fig_tl.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=500,
            margin=dict(l=250, r=20, t=30, b=50),
            xaxis=dict(title="Fecha", type="date"),
            barmode="overlay",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_tl, use_container_width=True, key="reg_timeline")

    st.divider()

    st.markdown("**Heatmap — Tipo Regla vs Estado de Cumplimiento**")
    heat_df = run_query(f"""
        SELECT TIPO_REGLA, ESTADO_CUMPLIMIENTO, COUNT(*) AS CNT
        FROM {TABLE} WHERE {WHERE}
        GROUP BY 1,2
    """)
    if not heat_df.empty:
        piv = heat_df.pivot_table(index="TIPO_REGLA", columns="ESTADO_CUMPLIMIENTO", values="CNT", fill_value=0)
        fig_heat = go.Figure(go.Heatmap(
            z=piv.values.tolist(),
            x=piv.columns.tolist(),
            y=piv.index.tolist(),
            colorscale=[[0, "#E8F5E9"], [0.5, "#FFF3E0"], [1, "#FFCDD2"]],
            text=piv.values.tolist(),
            texttemplate="%{text:,}",
            textfont=dict(size=13),
            hovertemplate="Tipo: %{y}<br>Estado: %{x}<br>Reglas: %{z:,}<extra></extra>",
        ))
        fig_heat.update_layout(
            template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
            margin=dict(l=150, r=20, t=30, b=50),
        )
        st.plotly_chart(fig_heat, use_container_width=True, key="reg_heatmap")

    st.divider()

    st.markdown("**Top 15 reglas con mayor exposicion financiera**")
    top_df = run_query(f"""
        SELECT RED_ESQUEMA, TIPO_REGLA, DESCRIPCION_REGLA, ESTADO_CUMPLIMIENTO,
               IMPACTO_NEGOCIO, ROUND(TASA_CUMPLIMIENTO*100,1) AS TASA_CUMPL,
               MULTA_POTENCIAL_COP, EXCEPCIONES_DETECTADAS, EXCEPCIONES_RESUELTAS,
               PRIORIDAD_IMPLEMENTACION
        FROM {TABLE} WHERE {WHERE}
        ORDER BY MULTA_POTENCIAL_COP DESC
        LIMIT 15
    """)
    if not top_df.empty:
        st.dataframe(top_df, use_container_width=True)
