# =============================================================================
# PANEL DE PROVISIÓN Y SOLVENCIA - RIESGO DE CRÉDITO
# Área de Riesgos | Streamlit in Snowflake
# Tablero Genérico para Entidades Financieras
# =============================================================================

import streamlit as st
import pandas as pd
import numpy as np

# -----------------------------------------------------------------------------
# CONFIGURACIÓN DE PÁGINA
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Provisión & Solvencia | Riesgos",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS - Paleta Profesional Neutra (Azul, Blanco, Gris)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    * { font-family: 'Inter', sans-serif; }
    
    .stApp { background: #f8fafc; }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #ffffff 0%, #f1f5f9 100%);
        border-right: 2px solid #e2e8f0;
    }
    
    [data-testid="stSidebar"] .stMarkdown h1,
    [data-testid="stSidebar"] .stMarkdown h2,
    [data-testid="stSidebar"] .stMarkdown h3 {
        color: #1e40af !important;
        font-weight: 600;
    }
    
    [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] label {
        color: #475569 !important;
    }
    
    [data-testid="stSidebar"] hr {
        border-color: #e2e8f0;
    }
    
    /* Header */
    .header-main {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%);
        padding: 25px 35px;
        border-radius: 16px;
        margin-bottom: 25px;
        box-shadow: 0 4px 20px rgba(30, 64, 175, 0.15);
    }
    
    .header-title {
        color: white;
        font-size: 1.9rem;
        font-weight: 700;
        margin: 0;
    }
    
    .header-subtitle {
        color: rgba(255,255,255,0.85);
        font-size: 1rem;
        margin: 8px 0 0 0;
    }
    
    /* Métricas */
    [data-testid="stMetric"] {
        background: #ffffff;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        border-left: 4px solid #3b82f6;
    }
    
    [data-testid="stMetricLabel"] {
        color: #64748b !important;
        font-weight: 500;
        font-size: 0.85rem !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    [data-testid="stMetricValue"] {
        color: #1e293b !important;
        font-size: 1.6rem !important;
        font-weight: 700 !important;
    }
    
    /* Headers de sección */
    .section-header {
        color: #1e40af;
        font-size: 1.1rem;
        font-weight: 600;
        padding: 12px 0;
        margin: 0 0 20px 0;
        border-bottom: 2px solid #3b82f6;
    }
    
    /* Tarjetas */
    .info-card {
        background: #ffffff;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        border-left: 4px solid #3b82f6;
        margin-bottom: 15px;
    }
    
    .info-card h4 {
        color: #1e40af;
        margin: 0 0 10px 0;
        font-weight: 600;
    }
    
    .info-card p {
        color: #475569;
        margin: 5px 0;
        font-size: 0.9rem;
    }
    
    .info-card.success { border-left-color: #10b981; }
    .info-card.success h4 { color: #059669; }
    
    .info-card.warning { border-left-color: #f59e0b; }
    .info-card.warning h4 { color: #d97706; }
    
    .info-card.danger { border-left-color: #ef4444; }
    .info-card.danger h4 { color: #dc2626; }
    
    .info-card.neutral { border-left-color: #6366f1; }
    .info-card.neutral h4 { color: #4f46e5; }
    
    /* Badges de escenario */
    .scenario-badge {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 12px 24px;
        border-radius: 30px;
        font-weight: 600;
        font-size: 0.95rem;
    }
    
    .scenario-base {
        background: #ecfdf5;
        color: #065f46;
        border: 1px solid #a7f3d0;
    }
    
    .scenario-leve {
        background: #fef3c7;
        color: #92400e;
        border: 1px solid #fcd34d;
    }
    
    .scenario-moderado {
        background: #ffedd5;
        color: #c2410c;
        border: 1px solid #fdba74;
    }
    
    .scenario-severo {
        background: #fee2e2;
        color: #991b1b;
        border: 1px solid #fca5a5;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
        background: #f1f5f9;
        padding: 4px;
        border-radius: 12px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border-radius: 10px;
        color: #64748b;
        font-weight: 500;
        padding: 12px 24px;
    }
    
    .stTabs [aria-selected="true"] {
        background: #1e40af !important;
        color: white !important;
    }
    
    /* DataFrames */
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
        border: 1px solid #e2e8f0;
    }
    
    /* Expanders */
    .streamlit-expanderHeader {
        background: #f8fafc;
        border-radius: 10px;
        font-weight: 500;
        color: #475569;
        border: 1px solid #e2e8f0;
    }
    
    /* Footer */
    .footer-corp {
        background: #ffffff;
        padding: 25px;
        border-radius: 12px;
        margin-top: 40px;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        border-top: 3px solid #3b82f6;
    }
    
    .footer-corp p {
        margin: 5px 0;
        color: #64748b;
    }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# HEADER
# -----------------------------------------------------------------------------
st.markdown("""
<div class="header-main" style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:20px;">
    <img src="https://mgg.com.co/wp-content/uploads/images/logo.png" alt="Logo" style="height:50px;">
    <div style="text-align:right;">
        <h1 class="header-title">📊 Panel de Provisión y Solvencia</h1>
        <p class="header-subtitle">Área de Riesgos | Simulador What-If Macroeconómico | NIIF 9</p>
    </div>
</div>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# GENERACIÓN DE DATOS SINTÉTICOS
# -----------------------------------------------------------------------------
@st.cache_data
def generar_datos_cartera(n_creditos: int = 1000, seed: int = 42) -> pd.DataFrame:
    """Genera un DataFrame sintético con información de créditos."""
    np.random.seed(seed)
    
    segmentos = ['Vivienda', 'Consumo', 'Comercial', 'Libranza']
    pesos_segmento = [0.25, 0.35, 0.25, 0.15]
    
    etapas_dist = {
        'Vivienda': [0.85, 0.10, 0.05],
        'Consumo': [0.75, 0.15, 0.10],
        'Comercial': [0.80, 0.12, 0.08],
        'Libranza': [0.90, 0.07, 0.03]
    }
    
    pd_base = {1: (0.5, 2.0), 2: (5.0, 15.0), 3: (25.0, 60.0)}
    lgd_params = {'Vivienda': (15, 35), 'Consumo': (45, 70), 'Comercial': (30, 55), 'Libranza': (35, 55)}
    saldo_params = {'Vivienda': (50e6, 500e6), 'Consumo': (5e6, 80e6), 'Comercial': (100e6, 500e6), 'Libranza': (5e6, 60e6)}
    score_params = {1: (650, 850), 2: (500, 650), 3: (300, 500)}
    
    regiones = ['Norte', 'Centro', 'Sur', 'Oriente', 'Occidente', 'Costa', 'Otras']
    
    data = []
    for i in range(n_creditos):
        seg = np.random.choice(segmentos, p=pesos_segmento)
        etapa = np.random.choice([1, 2, 3], p=etapas_dist[seg])
        saldo = np.random.uniform(*saldo_params[seg])
        pd_val = np.random.uniform(*pd_base[etapa]) / 100
        lgd_val = np.random.uniform(*lgd_params[seg]) / 100
        score = int(np.random.uniform(*score_params[etapa]))
        
        if etapa == 1:
            dias_mora = int(np.random.choice([0]*4 + [15, 25], p=[0.7, 0.1, 0.1, 0.05, 0.03, 0.02]))
        elif etapa == 2:
            dias_mora = int(np.random.uniform(31, 90))
        else:
            dias_mora = int(np.random.uniform(91, 365))
        
        region = np.random.choice(regiones, p=[0.2, 0.25, 0.15, 0.12, 0.13, 0.1, 0.05])
        
        data.append({
            'ID_Credito': f'CR-{i+1:06d}',
            'Segmento': seg,
            'Region': region,
            'Saldo_Capital': round(saldo, 0),
            'Etapa': etapa,
            'PD': round(pd_val, 4),
            'LGD': round(lgd_val, 4),
            'Score_Crediticio': score,
            'Dias_Mora': dias_mora
        })
    
    df = pd.DataFrame(data)
    df['Provision_Base'] = df['Saldo_Capital'] * df['PD'] * df['LGD']
    return df

# -----------------------------------------------------------------------------
# FUNCIONES DE CÁLCULO
# -----------------------------------------------------------------------------
def calcular_factor_estres(desempleo, inflacion, pib, tasa_ref, devaluacion, petroleo):
    factores = {
        'Desempleo': 1 + max(0, (desempleo - 10) * 0.06),
        'Inflación': 1 + max(0, (inflacion - 6) * 0.04),
        'PIB': 1 + max(0, (2 - pib) * 0.05),
        'Tasa Ref.': 1 + max(0, (tasa_ref - 8) * 0.03),
        'Devaluación': 1 + max(0, (devaluacion - 10) * 0.02),
        'Petróleo': 1 + max(0, (60 - petroleo) * 0.01)
    }
    total = sum([v * w for v, w in zip(factores.values(), [0.25, 0.20, 0.20, 0.15, 0.10, 0.10])])
    return {'individuales': factores, 'total': total}

def aplicar_estres(df, factores, sensibilidades):
    df_e = df.copy()
    for seg in df_e['Segmento'].unique():
        mask = df_e['Segmento'] == seg
        factor = 1 + (factores['total'] - 1) * sensibilidades.get(seg, 1.0)
        df_e.loc[mask, 'PD_Estresada'] = np.minimum(df_e.loc[mask, 'PD'] * factor, 1.0)
        df_e.loc[mask, 'LGD_Estresada'] = df_e.loc[mask, 'LGD'] * (1 + max(0, factores['total'] - 1.3) * 0.1)
        df_e.loc[mask, 'LGD_Estresada'] = np.minimum(df_e.loc[mask, 'LGD_Estresada'], 1.0)
    df_e['Provision_Estresada'] = df_e['Saldo_Capital'] * df_e['PD_Estresada'] * df_e['LGD_Estresada']
    return df_e

def calcular_solvencia(prov_base, prov_estres, tier1, tier2, apr):
    patrimonio = tier1 + tier2
    adicional = max(0, prov_estres - prov_base)
    pat_adj = patrimonio - adicional * 0.5
    t1_adj = tier1 - adicional * 0.3
    return {
        'patrimonio': patrimonio,
        'patrimonio_adj': pat_adj,
        'tier1_adj': t1_adj,
        'ratio_total': (pat_adj / apr) * 100,
        'ratio_tier1': (t1_adj / apr) * 100,
        'buffer': (pat_adj / apr) * 100 - 9.0
    }

def calcular_migracion(df, factor):
    p12 = min(0.05 * (factor - 1) * 10, 0.15) if factor > 1 else 0
    p23 = min(0.08 * (factor - 1) * 10, 0.25) if factor > 1 else 0
    n1, n2, n3 = len(df[df['Etapa']==1]), len(df[df['Etapa']==2]), len(df[df['Etapa']==3])
    m12, m23 = int(n1 * p12), int(n2 * p23)
    return {
        'actual': [n1, n2, n3],
        'proyectado': [n1 - m12, n2 + m12 - m23, n3 + m23],
        'migraciones': [m12, m23],
        'probabilidades': [p12 * 100, p23 * 100]
    }

def fmt(v, dec=2):
    if abs(v) >= 1e12: return f"${v/1e12:,.{dec}f} B"
    elif abs(v) >= 1e9: return f"${v/1e9:,.{dec}f} MM"
    elif abs(v) >= 1e6: return f"${v/1e6:,.{dec}f} M"
    return f"${v:,.0f}"

# -----------------------------------------------------------------------------
# DATOS
# -----------------------------------------------------------------------------
df_cartera = generar_datos_cartera()

# -----------------------------------------------------------------------------
# SIDEBAR
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## 🎛️ Panel de Control")
    st.markdown("---")
    
    st.markdown("### 📈 Variables Macroeconómicas")
    
    with st.expander("💼 Mercado Laboral", expanded=True):
        desempleo = st.slider("Tasa de Desempleo (%)", 5.0, 25.0, 10.0, 0.5)
    
    with st.expander("💰 Precios y Tasas", expanded=True):
        inflacion = st.slider("Inflación Anual (%)", 2.0, 18.0, 6.0, 0.5)
        tasa_ref = st.slider("Tasa de Referencia (%)", 4.0, 16.0, 8.0, 0.25)
    
    with st.expander("🌍 Sector Externo", expanded=False):
        devaluacion = st.slider("Devaluación Anual (%)", -10.0, 30.0, 5.0, 1.0)
        petroleo = st.slider("Precio Petróleo (USD)", 30, 120, 70, 5)
    
    with st.expander("📊 Actividad Económica", expanded=False):
        pib = st.slider("Crecimiento PIB (%)", -5.0, 8.0, 3.0, 0.5)
    
    st.markdown("---")
    st.markdown("### 🏛️ Capital Regulatorio")
    
    with st.expander("💎 Patrimonio", expanded=True):
        tier1 = st.number_input("Tier 1 (Billones)", 5.0, 30.0, 12.0, 0.5) * 1e12
        tier2 = st.number_input("Tier 2 (Billones)", 1.0, 10.0, 3.0, 0.5) * 1e12
        apr = st.number_input("APR (Billones)", 50.0, 200.0, 120.0, 5.0) * 1e12
    
    st.markdown("---")
    st.markdown("### 🎯 Sensibilidades")
    
    with st.expander("⚙️ Por Segmento", expanded=False):
        s_viv = st.slider("Vivienda", 0.5, 2.0, 0.8, 0.1)
        s_con = st.slider("Consumo", 0.5, 2.0, 1.3, 0.1)
        s_com = st.slider("Comercial", 0.5, 2.0, 1.0, 0.1)
        s_lib = st.slider("Libranza", 0.5, 2.0, 0.7, 0.1)
    
    sensibilidades = {'Vivienda': s_viv, 'Consumo': s_con, 'Comercial': s_com, 'Libranza': s_lib}
    
    st.markdown("---")
    st.markdown("### 🔍 Filtros")
    
    seg_sel = st.multiselect("Segmentos", df_cartera['Segmento'].unique().tolist(), default=df_cartera['Segmento'].unique().tolist())
    etapa_sel = st.multiselect("Etapas", [1, 2, 3], default=[1, 2, 3], format_func=lambda x: f"Etapa {x}")
    rango = st.slider("Saldo (Millones)", 0, 500, (0, 500), 10)

# -----------------------------------------------------------------------------
# CÁLCULOS
# -----------------------------------------------------------------------------
factores = calcular_factor_estres(desempleo, inflacion, pib, tasa_ref, devaluacion, petroleo)

df_filt = df_cartera[
    (df_cartera['Segmento'].isin(seg_sel)) &
    (df_cartera['Etapa'].isin(etapa_sel)) &
    (df_cartera['Saldo_Capital'] >= rango[0] * 1e6) &
    (df_cartera['Saldo_Capital'] <= rango[1] * 1e6)
].copy()

df_stress = aplicar_estres(df_filt, factores, sensibilidades)

factor_total = factores['total']
prov_base = df_stress['Provision_Base'].sum()
prov_stress = df_stress['Provision_Estresada'].sum()
saldo_total = df_stress['Saldo_Capital'].sum()
impacto = prov_stress - prov_base

solv = calcular_solvencia(prov_base, prov_stress, tier1, tier2, apr)
mig = calcular_migracion(df_stress, factor_total)

# -----------------------------------------------------------------------------
# INDICADOR DE ESCENARIO
# -----------------------------------------------------------------------------
col_e1, col_e2, col_e3 = st.columns([1, 2, 1])

with col_e2:
    if factor_total < 1.05:
        badge = f'<span class="scenario-badge scenario-base">✅ ESCENARIO BASE | Factor: {factor_total:.2f}x</span>'
    elif factor_total < 1.20:
        badge = f'<span class="scenario-badge scenario-leve">⚠️ ADVERSO LEVE | Factor: {factor_total:.2f}x</span>'
    elif factor_total < 1.40:
        badge = f'<span class="scenario-badge scenario-moderado">🔶 ADVERSO MODERADO | Factor: {factor_total:.2f}x</span>'
    else:
        badge = f'<span class="scenario-badge scenario-severo">🚨 SEVERAMENTE ADVERSO | Factor: {factor_total:.2f}x</span>'
    st.markdown(f'<div style="text-align:center;">{badge}</div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# MÉTRICAS - RIESGO DE CRÉDITO
# -----------------------------------------------------------------------------
st.markdown('<div class="section-header">📊 Indicadores de Riesgo de Crédito</div>', unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("💰 Provisión Base", fmt(prov_base))
with c2:
    delta = f"+{fmt(impacto)}" if impacto > 0 else None
    st.metric("📉 Provisión Estrés", fmt(prov_stress), delta=delta, delta_color="inverse")
with c3:
    cob = (prov_stress / saldo_total * 100) if saldo_total > 0 else 0
    st.metric("🛡️ Cobertura", f"{cob:.2f}%")
with c4:
    st.metric("⚡ Impacto P&G", fmt(-impacto))

# MÉTRICAS - SOLVENCIA
st.markdown('<div class="section-header">🏛️ Indicadores de Solvencia</div>', unsafe_allow_html=True)

c5, c6, c7, c8 = st.columns(4)
with c5:
    buf = f"+{solv['buffer']:.1f} pp" if solv['buffer'] > 0 else "⚠️ Bajo mínimo"
    st.metric("📊 Ratio Solvencia", f"{solv['ratio_total']:.2f}%", delta=buf)
with c6:
    st.metric("💎 Ratio Tier 1", f"{solv['ratio_tier1']:.2f}%")
with c7:
    st.metric("🏦 Patrimonio Adj.", fmt(solv['patrimonio_adj']))
with c8:
    pd_p = df_stress['PD_Estresada'].mean() * 100
    pd_d = pd_p - df_stress['PD'].mean() * 100
    st.metric("📈 PD Promedio", f"{pd_p:.2f}%", delta=f"+{pd_d:.2f} pp" if pd_d > 0 else None, delta_color="inverse")

# -----------------------------------------------------------------------------
# TABS
# -----------------------------------------------------------------------------
st.markdown("<br>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs(["📊 Por Segmento", "📈 Migración Etapas", "🔴 Mayor Riesgo", "📉 Sensibilidad"])

with tab1:
    c_a, c_b = st.columns([3, 2])
    
    with c_a:
        st.markdown("#### Comparación de Provisiones")
        df_seg = df_stress.groupby('Segmento').agg({
            'Provision_Base': 'sum', 'Provision_Estresada': 'sum', 'Saldo_Capital': 'sum', 'ID_Credito': 'count'
        }).reset_index()
        df_seg['Base (MM)'] = df_seg['Provision_Base'] / 1e6
        df_seg['Estrés (MM)'] = df_seg['Provision_Estresada'] / 1e6
        chart = df_seg[['Segmento', 'Base (MM)', 'Estrés (MM)']].set_index('Segmento')
        st.bar_chart(chart, color=["#94a3b8", "#3b82f6"], height=350)
    
    with c_b:
        st.markdown("#### Detalle por Segmento")
        for _, r in df_seg.iterrows():
            d = ((r['Provision_Estresada'] - r['Provision_Base']) / r['Provision_Base'] * 100) if r['Provision_Base'] > 0 else 0
            st.markdown(f"""
            <div class="info-card">
                <h4>{r['Segmento']}</h4>
                <p><strong>{int(r['ID_Credito']):,}</strong> créditos | Saldo: <strong>{fmt(r['Saldo_Capital'])}</strong></p>
                <p>Base: {fmt(r['Provision_Base'])} → Estrés: {fmt(r['Provision_Estresada'])}</p>
                <p style="color:#1e40af; font-weight:600;">Δ {d:+.1f}%</p>
            </div>
            """, unsafe_allow_html=True)

with tab2:
    c_m1, c_m2 = st.columns(2)
    
    with c_m1:
        st.markdown("#### Distribución Actual vs Proyectada")
        df_m = pd.DataFrame({
            'Etapa': ['Etapa 1', 'Etapa 2', 'Etapa 3'],
            'Actual': mig['actual'],
            'Proyectado': mig['proyectado']
        })
        st.dataframe(df_m, hide_index=True, use_container_width=True)
        st.bar_chart(df_m.set_index('Etapa'), color=["#10b981", "#ef4444"], height=280)
    
    with c_m2:
        st.markdown("#### Flujos de Migración")
        st.markdown(f"""
        <div class="info-card warning">
            <h4>Etapa 1 → Etapa 2</h4>
            <p style="font-size:1.8rem; color:#d97706; font-weight:700;">{mig['migraciones'][0]:,} créditos</p>
            <p>Probabilidad: {mig['probabilidades'][0]:.1f}%</p>
        </div>
        <div class="info-card danger">
            <h4>Etapa 2 → Etapa 3</h4>
            <p style="font-size:1.8rem; color:#dc2626; font-weight:700;">{mig['migraciones'][1]:,} créditos</p>
            <p>Probabilidad: {mig['probabilidades'][1]:.1f}%</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("#### Provisión por Etapa")
        for e in [1, 2, 3]:
            df_e = df_stress[df_stress['Etapa'] == e]
            if len(df_e) > 0:
                ic = {1: "🟢", 2: "🟡", 3: "🔴"}
                st.markdown(f"{ic[e]} **Etapa {e}**: {fmt(df_e['Provision_Estresada'].sum())}")

with tab3:
    st.markdown("#### Top 15 Créditos de Mayor Provisión")
    
    df_stress['Score_Riesgo'] = (
        df_stress['PD_Estresada'] * 40 +
        df_stress['LGD_Estresada'] * 30 +
        (1 - df_stress['Score_Crediticio'] / 850) * 20 +
        (df_stress['Dias_Mora'] / 365) * 10
    ) * 100
    
    df_top = df_stress.nlargest(15, 'Provision_Estresada')[
        ['ID_Credito', 'Segmento', 'Region', 'Saldo_Capital', 'Etapa', 
         'PD_Estresada', 'LGD_Estresada', 'Score_Crediticio', 'Dias_Mora', 'Provision_Estresada', 'Score_Riesgo']
    ].copy()
    
    df_disp = df_top.copy()
    df_disp['Saldo_Capital'] = df_disp['Saldo_Capital'].apply(lambda x: fmt(x, 0))
    df_disp['PD_Estresada'] = df_disp['PD_Estresada'].apply(lambda x: f"{x*100:.2f}%")
    df_disp['LGD_Estresada'] = df_disp['LGD_Estresada'].apply(lambda x: f"{x*100:.1f}%")
    df_disp['Provision_Estresada'] = df_disp['Provision_Estresada'].apply(lambda x: fmt(x, 0))
    df_disp['Score_Riesgo'] = df_disp['Score_Riesgo'].apply(lambda x: f"{x:.0f}")
    df_disp.columns = ['ID', 'Segmento', 'Región', 'Saldo', 'Etapa', 'PD', 'LGD', 'Score', 'Días Mora', 'Provisión', 'Riesgo']
    
    st.dataframe(df_disp, hide_index=True, use_container_width=True)
    
    cr1, cr2, cr3 = st.columns(3)
    pt = df_top['Provision_Estresada'].sum()
    pc = (pt / prov_stress * 100) if prov_stress > 0 else 0
    with cr1:
        st.metric("Provisión Top 15", fmt(pt), f"{pc:.1f}% del total")
    with cr2:
        st.metric("Score Promedio", f"{df_top['Score_Crediticio'].mean():.0f}")
    with cr3:
        st.metric("Días Mora Prom.", f"{df_top['Dias_Mora'].mean():.0f}")

with tab4:
    cs1, cs2 = st.columns(2)
    
    with cs1:
        st.markdown("#### Impacto por Factor")
        df_f = pd.DataFrame({
            'Factor': list(factores['individuales'].keys()),
            'Multiplicador': list(factores['individuales'].values()),
            'Impacto %': [(f - 1) * 100 for f in factores['individuales'].values()]
        }).sort_values('Impacto %', ascending=False)
        st.dataframe(df_f, hide_index=True, use_container_width=True)
    
    with cs2:
        st.markdown("#### Matriz de Escenarios")
        esc = [
            ('Base', 10, 6, prov_base),
            ('Adverso Leve', 12, 8, prov_base * 1.15),
            ('Adverso Moderado', 15, 10, prov_base * 1.35),
            ('Adverso Severo', 18, 12, prov_base * 1.60),
            ('Simulación Actual', desempleo, inflacion, prov_stress)
        ]
        df_esc = pd.DataFrame(esc, columns=['Escenario', 'Desempleo', 'Inflación', 'Provisión'])
        df_esc['Provisión'] = df_esc['Provisión'].apply(fmt)
        st.dataframe(df_esc, hide_index=True, use_container_width=True)

# -----------------------------------------------------------------------------
# RESUMEN EJECUTIVO
# -----------------------------------------------------------------------------
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="section-header">📝 Resumen Ejecutivo</div>', unsafe_allow_html=True)

r1, r2, r3 = st.columns(3)

with r1:
    st.markdown('<div class="info-card neutral"><h4>📊 Cartera Analizada</h4></div>', unsafe_allow_html=True)
    st.markdown(f"""
    - **Créditos:** {len(df_stress):,}
    - **Saldo Total:** {fmt(saldo_total)}
    - **Saldo Promedio:** {fmt(saldo_total/len(df_stress) if len(df_stress) > 0 else 0)}
    - **Score Promedio:** {df_stress['Score_Crediticio'].mean():.0f}
    """)

with r2:
    st.markdown('<div class="info-card warning"><h4>⚠️ Riesgo de Crédito</h4></div>', unsafe_allow_html=True)
    st.markdown(f"""
    - **Provisión Base:** {fmt(prov_base)}
    - **Provisión Estrés:** {fmt(prov_stress)}
    - **Incremento:** {fmt(impacto)} ({(impacto/prov_base*100) if prov_base > 0 else 0:.1f}%)
    - **PD Promedio:** {df_stress['PD_Estresada'].mean()*100:.2f}%
    """)

with r3:
    estado = "✅ Cumple" if solv['ratio_total'] >= 9 else "❌ No Cumple"
    st.markdown('<div class="info-card success"><h4>🏛️ Solvencia</h4></div>', unsafe_allow_html=True)
    st.markdown(f"""
    - **Ratio Solvencia:** {solv['ratio_total']:.2f}%
    - **Ratio Tier 1:** {solv['ratio_tier1']:.2f}%
    - **Buffer:** {solv['buffer']:.2f} pp
    - **Estado:** {estado}
    """)

# Alertas
if solv['ratio_total'] < 9:
    st.error("🚨 **ALERTA**: Ratio de solvencia por debajo del mínimo regulatorio (9%).")
elif solv['buffer'] < 2:
    st.warning("⚠️ **ATENCIÓN**: Buffer de capital reducido. Monitoreo recomendado.")

# -----------------------------------------------------------------------------
# FOOTER
# -----------------------------------------------------------------------------
st.markdown("""
<div class="footer-corp">
    <img src="https://mgg.com.co/wp-content/uploads/images/logo.png" alt="Logo" style="height:35px; margin-bottom:15px;">
    <p style="font-weight:600; color:#1e40af;">Panel de Provisión y Solvencia</p>
    <p style="font-size:0.85rem;">Modelo de simulación con datos sintéticos | NIIF 9 - ECL</p>
    <p style="font-size:0.8rem; color:#94a3b8;">Streamlit in Snowflake</p>
</div>
""", unsafe_allow_html=True)
