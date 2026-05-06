import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

TABLE = "MGG_PAGOS.LIQUIDACION_Y_COMPENSACION.COMISIONES"

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
    tipos = run_query(f"SELECT DISTINCT TIPO_FEE FROM {TABLE} ORDER BY 1")["TIPO_FEE"].tolist()
    redes = run_query(f"SELECT DISTINCT RED FROM {TABLE} ORDER BY 1")["RED"].tolist()
    productos = run_query(f"SELECT DISTINCT PRODUCTO FROM {TABLE} ORDER BY 1")["PRODUCTO"].tolist()
    estructuras = run_query(f"SELECT DISTINCT ESTRUCTURA_FEE FROM {TABLE} ORDER BY 1")["ESTRUCTURA_FEE"].tolist()
    entornos = run_query(f"SELECT DISTINCT ENTORNO FROM {TABLE} ORDER BY 1")["ENTORNO"].tolist()
    fechas = run_query(f"SELECT MIN(FECHA_APLICACION) AS FMIN, MAX(FECHA_APLICACION) AS FMAX FROM {TABLE}")
    return tipos, redes, productos, estructuras, entornos, fechas


def build_where(fecha_ini, fecha_f, tipo_s, red_s, producto_s, estructura_s, entorno_s,
                all_tipos, all_redes, all_productos, all_estructuras, all_entornos):
    clauses = [f"FECHA_APLICACION BETWEEN '{fecha_ini}' AND '{fecha_f}'"]
    if tipo_s and tipo_s != "Todos" and tipo_s in all_tipos:
        clauses.append(f"TIPO_FEE = '{tipo_s}'")
    if red_s and red_s != "Todos" and red_s in all_redes:
        clauses.append(f"RED = '{red_s}'")
    if producto_s and producto_s != "Todos" and producto_s in all_productos:
        clauses.append(f"PRODUCTO = '{producto_s}'")
    if estructura_s and estructura_s != "Todos" and estructura_s in all_estructuras:
        clauses.append(f"ESTRUCTURA_FEE = '{estructura_s}'")
    if entorno_s and entorno_s != "Todos" and entorno_s in all_entornos:
        clauses.append(f"ENTORNO = '{entorno_s}'")
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


st.header(":material/price_change: Comisiones")
st.caption("Estructura de fees por transaccion: MDR, interchange, scheme fee, processing fee. Tipo, porcentaje, monto, pagador y receptor.")

tab1, tab2 = st.tabs(["Dashboard Ejecutivo", "Simulador Predictivo"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**:material/lightbulb: Que resuelve**")
            st.markdown("Complejidad para modelar rentabilidad por transaccion debido a la multiplicidad de fees y variacion por segmento.")
    with c2:
        with st.container(border=True):
            st.markdown("**:material/settings: Como funciona**")
            st.markdown("Cada transaccion desglosa: MDR total, interchange, scheme fee, processing fee y margen adquirente. Registra vigencia y condiciones.")
    with c3:
        with st.container(border=True):
            st.markdown("**:material/trending_up: Valor de negocio**")
            st.markdown("Optimizar pricing por segmento, negociar tarifas con redes y entender rentabilidad real por tipo de transaccion.")

    tipos, redes, productos, estructuras, entornos, fechas_df = get_filter_options()
    fmin = pd.to_datetime(fechas_df["FMIN"].iloc[0]).date()
    fmax = pd.to_datetime(fechas_df["FMAX"].iloc[0]).date()

    dx_where = f"FECHA_APLICACION BETWEEN '{fmin}' AND '{fmax}'"

    dx_kpi = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            ROUND(SUM(FEE_TOTAL_COP)/1e9, 2) AS FEE_TOTAL_B,
            ROUND(AVG(FEE_TOTAL_COP), 0) AS FEE_PROM,
            ROUND(SUM(CASE WHEN ES_REGULADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_REG,
            ROUND(SUM(CASE WHEN APLICA_IVA THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_IVA,
            ROUND(SUM(CASE WHEN FEE_ESPECIAL_NEGOCIADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_ESP,
            ROUND(SUM(MONTO_BASE_COP)/1e9, 2) AS BASE_B,
            ROUND(AVG(PORCENTAJE_FEE), 2) AS PCT_FEE_PROM
        FROM {TABLE}
        WHERE {dx_where}
    """)

    if not dx_kpi.empty and dx_kpi["TOTAL"].iloc[0] > 0:
        dx = dx_kpi.iloc[0]
        dx_total = int(dx["TOTAL"])
        dx_fee_prom = float(dx["FEE_PROM"])
        dx_pct_reg = float(dx["PCT_REG"])
        dx_pct_esp = float(dx["PCT_ESP"])
        dx_pct_fee = float(dx["PCT_FEE_PROM"])

        dx_top_tipo = run_query(f"""
            SELECT TIPO_FEE, ROUND(SUM(FEE_TOTAL_COP)/1e6, 1) AS FEE_M
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)
        dx_top_red = run_query(f"""
            SELECT RED, ROUND(AVG(PORCENTAJE_FEE), 2) AS PCT
            FROM {TABLE} WHERE {dx_where}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 1
        """)

        if dx_pct_fee <= 2.5:
            dx_estado = f"Fee promedio ({color_tag(dx_pct_fee, 2.5, 4.0, fmt='{:.2f}%', inverse=True)}) :green[**competitivo**]. Recaudo: ${float(dx['FEE_TOTAL_B']):.2f}B COP."
        elif dx_pct_fee <= 4.0:
            dx_estado = f"Fee promedio ({color_tag(dx_pct_fee, 2.5, 4.0, fmt='{:.2f}%', inverse=True)}) en :orange[**rango medio**]. Oportunidad de optimizacion."
        else:
            dx_estado = f"Fee promedio ({color_tag(dx_pct_fee, 2.5, 4.0, fmt='{:.2f}%', inverse=True)}) :red[**elevado**]. Revisar estructura de pricing."

        dx_lines = [dx_estado]
        if not dx_top_tipo.empty:
            tt = dx_top_tipo.iloc[0]
            dx_lines.append(f"- Mayor recaudo: **{tt['TIPO_FEE']}** (${float(tt['FEE_M']):.0f}M COP)")
        if not dx_top_red.empty:
            tr = dx_top_red.iloc[0]
            dx_lines.append(f"- Red con mayor fee %: **{tr['RED']}** ({float(tr['PCT']):.2f}%)")
        dx_lines.append(f"- Regulados: **{dx_pct_reg:.1f}%** — {'gran parte del portafolio' if dx_pct_reg > 50 else 'espacio para pricing diferenciado'}")
        dx_lines.append(f"- Fee especial negociado: **{dx_pct_esp:.1f}%** {'— :green[**buena cobertura**]' if dx_pct_esp > 20 else '— :orange[**oportunidad de negociacion**]'}")

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
                if dx_pct_fee > 3.5:
                    recs.append("1. :red[**Renegociar tarifas**]: fee promedio por encima del benchmark regional.")
                if dx_pct_reg < 30:
                    recs.append("2. :orange[**Revisar cumplimiento regulatorio**]: bajo porcentaje de fees regulados.")
                if dx_pct_esp < 15:
                    recs.append("3. :orange[**Ampliar negociacion de fees especiales**]: bajo porcentaje de acuerdos negociados.")
                if dx_fee_prom > 5000:
                    recs.append("4. :orange[**Optimizar fee fijo**]: monto promedio de fee alto, impacta microtransacciones.")
                if not recs:
                    recs.append(":green[**Estructura de comisiones operando dentro de parametros optimos.**] Mantener monitoreo continuo.")
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
        tipo_sel = st.selectbox("Tipo fee", ["Todos"] + tipos, key="com_tipo")
    with fc3:
        red_sel = st.selectbox("Red", ["Todos"] + redes, key="com_red")
    with fc4:
        producto_sel = st.selectbox("Producto", ["Todos"] + productos, key="com_producto")
    with fc5:
        entorno_sel = st.selectbox("Entorno", ["Todos"] + entornos, key="com_entorno")

    estructura_sel = "Todos"

    WHERE = build_where(fecha_inicio, fecha_fin, tipo_sel, red_sel, producto_sel, estructura_sel, entorno_sel,
                        tipos, redes, productos, estructuras, entornos)

    kpi_df = run_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            ROUND(SUM(FEE_TOTAL_COP)/1e9, 2) AS FEE_TOTAL_B,
            ROUND(AVG(FEE_TOTAL_COP), 0) AS FEE_PROM,
            ROUND(SUM(CASE WHEN ES_REGULADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_REG,
            ROUND(SUM(CASE WHEN APLICA_IVA THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_IVA,
            ROUND(SUM(CASE WHEN FEE_ESPECIAL_NEGOCIADO THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS PCT_ESP,
            ROUND(SUM(MONTO_BASE_COP)/1e9, 2) AS BASE_B,
            ROUND(AVG(FEE_FIJO_COP), 0) AS FIJO_PROM
        FROM {TABLE}
        WHERE {WHERE}
    """)

    if kpi_df.empty or kpi_df["TOTAL"].iloc[0] == 0:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        r = kpi_df.iloc[0]
        total = int(r["TOTAL"])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total comisiones", f"{total:,}")
        k2.metric("Fee total recaudado", f"${float(r['FEE_TOTAL_B']):.2f}B COP")
        k3.metric("Fee promedio", f"${float(r['FEE_PROM']):,.0f} COP")
        k4.metric("% Regulado", f"{float(r['PCT_REG']):.1f}%")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("% Con IVA", f"{float(r['PCT_IVA']):.1f}%")
        k6.metric("% Fee especial negociado", f"{float(r['PCT_ESP']):.1f}%")
        k7.metric("Monto base total", f"${float(r['BASE_B']):.2f}B COP")
        k8.metric("Fee fijo promedio", f"${float(r['FIJO_PROM']):,.0f} COP")

        st.divider()

        trend_df = run_query(f"""
            SELECT DATE_TRUNC('MONTH', FECHA_APLICACION) AS MES,
                   ROUND(SUM(FEE_TOTAL_COP)/1e9, 2) AS FEE_B,
                   ROUND(SUM(MONTO_BASE_COP)/1e9, 2) AS BASE_B
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1 ORDER BY 1
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Evolucion mensual — Fee total y monto base**")
            if trend_df.empty:
                st.info("Sin datos de tendencia.")
            else:
                meses = trend_df["MES"].tolist()
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["FEE_B"].tolist(), name="Fee total (B COP)",
                    fill="tozeroy", line=dict(color=COLORS[0], width=2),
                ))
                fig_trend.add_trace(go.Scatter(
                    x=meses, y=trend_df["BASE_B"].tolist(), name="Monto base (B COP)",
                    yaxis="y2", line=dict(color=COLORS[1], width=2, dash="dot"),
                ))
                fig_trend.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=60, r=60, t=30, b=60),
                    yaxis=dict(title="Fee (B COP)"),
                    yaxis2=dict(title="Base (B COP)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_trend, use_container_width=True)

        with col2:
            st.markdown("**Treemap — Tipo de fee y red**")
            tree_df = run_query(f"""
                SELECT TIPO_FEE, RED, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1, 2 ORDER BY 3 DESC
            """)
            if tree_df.empty:
                st.info("Sin datos.")
            else:
                labels, parents, values, colors_t = [], [], [], []
                parent_totals = tree_df.groupby("TIPO_FEE")["N"].sum().sort_values(ascending=False)
                for i, (parent, parent_total) in enumerate(parent_totals.items()):
                    labels.append(parent)
                    parents.append("")
                    values.append(int(parent_total))
                    colors_t.append(COLORS[i % len(COLORS)])
                    sub = tree_df[tree_df["TIPO_FEE"] == parent]
                    for _, row in sub.iterrows():
                        labels.append(str(row["RED"]))
                        parents.append(parent)
                        values.append(int(row["N"]))
                        colors_t.append(COLORS[i % len(COLORS)])
                fig_tree = go.Figure(go.Treemap(
                    labels=labels, parents=parents, values=values,
                    marker=dict(colors=colors_t),
                    textinfo="label+value+percent parent",
                    hovertemplate="<b>%{label}</b><br>Comisiones: %{value:,}<br>%{percentParent:.1%} del padre<extra></extra>",
                ))
                fig_tree.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("**Heatmap — Fee promedio por tipo de fee y producto**")
        heat_df = run_query(f"""
            SELECT TIPO_FEE, PRODUCTO, ROUND(AVG(FEE_TOTAL_COP)/1000, 1) AS FEE_K
            FROM {TABLE}
            WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        if heat_df.empty:
            st.info("Sin datos para heatmap.")
        else:
            pivot = heat_df.pivot_table(index="TIPO_FEE", columns="PRODUCTO", values="FEE_K", fill_value=0)
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0, "#E3F2FD"], [0.3, "#29B5E8"], [0.6, "#FF8B00"], [1, "#DE350B"]],
                text=[[f"${v:.1f}K" for v in row] for row in pivot.values.tolist()],
                texttemplate="%{text}",
                textfont=dict(size=13),
                hovertemplate="Tipo: %{y}<br>Producto: %{x}<br>Fee prom: $%{z:.1f}K COP<extra></extra>",
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
            st.markdown("**Funnel — Cascada de fees**")
            cascade_df = run_query(f"""
                SELECT
                    ROUND(SUM(MONTO_BASE_COP)/1e9, 2) AS BASE,
                    ROUND(SUM(FEE_TOTAL_COP)/1e9, 2) AS FEE_TOTAL,
                    ROUND(SUM(MONTO_FEE_COP)/1e9, 2) AS MONTO_FEE,
                    ROUND(SUM(FEE_FIJO_COP)/1e9, 2) AS FEE_FIJO,
                    ROUND(SUM(IVA_FEE_COP)/1e9, 2) AS IVA
                FROM {TABLE} WHERE {WHERE}
            """)
            if not cascade_df.empty:
                cd = cascade_df.iloc[0]
                funnel_data = [
                    ("Monto base", float(cd["BASE"])),
                    ("Fee total", float(cd["FEE_TOTAL"])),
                    ("Monto fee", float(cd["MONTO_FEE"])),
                    ("Fee fijo", float(cd["FEE_FIJO"])),
                    ("IVA fee", float(cd["IVA"])),
                ]
                fig_funnel = go.Figure(go.Funnel(
                    y=[f[0] for f in funnel_data],
                    x=[f[1] for f in funnel_data],
                    textinfo="value+percent initial",
                    marker=dict(color=[COLORS[0], COLORS[1], COLORS[3], COLORS[5], COLORS[4]]),
                    hovertemplate="%{y}: $%{x:.2f}B COP<extra></extra>",
                ))
                fig_funnel.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_funnel, use_container_width=True)

        with col4:
            st.markdown("**Distribucion por estructura de fee**")
            estr_df = run_query(f"""
                SELECT ESTRUCTURA_FEE, COUNT(*) AS N
                FROM {TABLE} WHERE {WHERE}
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if estr_df.empty:
                st.info("Sin datos.")
            else:
                fig_donut = go.Figure(go.Pie(
                    labels=estr_df["ESTRUCTURA_FEE"].tolist(),
                    values=estr_df["N"].tolist(),
                    hole=0.5,
                    marker=dict(colors=COLORS[:len(estr_df)]),
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
                ))
                fig_donut.update_layout(
                    template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_donut, use_container_width=True)

        st.markdown("**Top 10 MCC por fee total**")
        mcc_df = run_query(f"""
            SELECT MCC,
                   COUNT(*) AS TOTAL,
                   ROUND(SUM(FEE_TOTAL_COP)/1e6, 1) AS FEE_M,
                   ROUND(AVG(PORCENTAJE_FEE), 2) AS PCT_PROM
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1 ORDER BY 3 DESC LIMIT 10
        """)
        if not mcc_df.empty:
            fig_mcc = go.Figure(go.Bar(
                y=mcc_df["MCC"].tolist()[::-1],
                x=mcc_df["FEE_M"].tolist()[::-1],
                orientation="h",
                marker=dict(color=COLORS[0]),
                text=[f"${v:.1f}M" for v in mcc_df["FEE_M"].tolist()[::-1]],
                textposition="outside",
                hovertemplate="MCC: %{y}<br>Fee total: $%{x:.1f}M COP<extra></extra>",
            ))
            fig_mcc.update_layout(
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=120, r=60, t=30, b=40),
                xaxis=dict(title="Fee total ($M COP)"),
                yaxis=dict(title=""),
            )
            st.plotly_chart(fig_mcc, use_container_width=True)

        st.markdown("**Distribucion pagador vs receptor**")
        pag_rec_df = run_query(f"""
            SELECT PAGADO_POR, RECIBIDO_POR, COUNT(*) AS N, ROUND(SUM(FEE_TOTAL_COP)/1e6, 1) AS FEE_M
            FROM {TABLE} WHERE {WHERE}
            GROUP BY 1, 2 ORDER BY 3 DESC
        """)
        if not pag_rec_df.empty:
            pagadores = pag_rec_df["PAGADO_POR"].unique().tolist()
            receptores = pag_rec_df["RECIBIDO_POR"].unique().tolist()
            fig_stacked = go.Figure()
            for i, receptor in enumerate(receptores):
                sub = pag_rec_df[pag_rec_df["RECIBIDO_POR"] == receptor]
                vals = []
                for pag in pagadores:
                    match = sub[sub["PAGADO_POR"] == pag]
                    vals.append(float(match["FEE_M"].iloc[0]) if not match.empty else 0)
                fig_stacked.add_trace(go.Bar(
                    name=receptor, x=pagadores, y=vals,
                    marker=dict(color=COLORS[i % len(COLORS)]),
                    hovertemplate=f"Receptor: {receptor}<br>Pagador: %{{x}}<br>Fee: $%{{y:.1f}}M COP<extra></extra>",
                ))
            fig_stacked.update_layout(
                barmode="stack",
                template="plotly_white", paper_bgcolor="#FFFFFF", height=400,
                margin=dict(l=60, r=40, t=30, b=60),
                xaxis=dict(title="Pagado por"),
                yaxis=dict(title="Fee ($M COP)"),
                legend=dict(title="Recibido por", orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_stacked, use_container_width=True)

with tab2:
    st.markdown("**Simulador de revenue por comisiones**")
    st.caption("Estime el revenue por fees y el margen ajustando variables clave de pricing. El modelo pondera cada factor segun su impacto relativo.")

    col_sliders, col_result = st.columns([3, 2])

    with col_sliders:
        sim_interchange = st.slider("% Interchange promedio", 0.5, 5.0, 1.8, step=0.1, key="com_sim_int",
                                    help="Tasa pagada al banco emisor por cada transaccion")
        sim_scheme = st.slider("% Scheme fee promedio", 0.05, 1.0, 0.2, step=0.05, key="com_sim_scheme",
                               help="Fee de la red de tarjetas (Visa, Mastercard)")
        sim_mdr = st.slider("% MDR total promedio", 1.0, 8.0, 2.8, step=0.1, key="com_sim_mdr",
                            help="Merchant Discount Rate total cobrado al comercio")
        sim_pct_regulado = st.slider("% Fees regulados", 0, 100, 40, key="com_sim_reg",
                                     help="Porcentaje de fees sujetos a regulacion (limita pricing)")
        sim_mix_producto = st.slider("% Credito vs debito", 0, 100, 55, key="com_sim_mix",
                                     help="Mayor % credito genera mayor interchange y MDR")
        sim_pct_especial = st.slider("% Fee especial negociado", 0, 50, 10, key="com_sim_esp",
                                     help="Porcentaje de comercios con tarifas negociadas especiales")

    def calcular_revenue_com(interchange, scheme, mdr, pct_regulado, mix_credito, pct_especial):
        margen_adq = max(0, mdr - interchange - scheme)
        s_margen = min(1.0, margen_adq / 3.0)
        s_regulado = 1.0 - (pct_regulado / 100.0) * 0.4
        s_mix = 0.5 + (mix_credito / 100.0) * 0.5
        s_especial = 1.0 - (pct_especial / 100.0) * 0.6
        revenue_score = 0.30 * s_margen + 0.20 * s_regulado + 0.25 * s_mix + 0.25 * s_especial
        score = 0.50 + revenue_score * 0.48
        return round(min(max(score, 0.50), 0.98), 3), round(margen_adq, 2)

    score_pred, margen_pred = calcular_revenue_com(sim_interchange, sim_scheme, sim_mdr, sim_pct_regulado, sim_mix_producto, sim_pct_especial)
    score_pct = score_pred * 100

    with col_result:
        if score_pct >= 92:
            nivel = "Alta"
            color_nivel = "#36B37E"
            interpretacion = "Excelente revenue por comisiones. Margen adquirente optimo y mix de producto favorable. Mantener estructura actual."
        elif score_pct >= 80:
            nivel = "Media-Alta"
            color_nivel = "#29B5E8"
            interpretacion = "Revenue aceptable. Explorar oportunidades de negociacion de interchange o ajuste de MDR en segmentos clave."
        elif score_pct >= 65:
            nivel = "Media"
            color_nivel = "#FFAB00"
            interpretacion = "Revenue por debajo del benchmark. Priorizar revision de pricing por segmento y negociacion con redes."
        else:
            nivel = "Baja"
            color_nivel = "#DE350B"
            interpretacion = "Revenue critico. Accion inmediata: revisar MDR vs costos, renegociar interchange y optimizar mix de producto."

        st.metric("Revenue score estimado", f"{score_pct:.1f}%")
        st.metric("Margen adquirente estimado", f"{margen_pred:.2f}%")
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

    if "com_escenarios" not in st.session_state:
        st.session_state.com_escenarios = []

    if st.button("Guardar escenario actual", type="primary", key="com_save"):
        if len(st.session_state.com_escenarios) >= 3:
            st.session_state.com_escenarios.pop(0)
        st.session_state.com_escenarios.append({
            "% Interchange": sim_interchange,
            "% Scheme": sim_scheme,
            "% MDR": sim_mdr,
            "% Regulado": sim_pct_regulado,
            "% Credito": sim_mix_producto,
            "% Fee especial": sim_pct_especial,
            "Revenue score": f"{score_pct:.1f}%",
            "Margen adq": f"{margen_pred:.2f}%",
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
