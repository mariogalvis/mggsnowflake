import streamlit as st

st.set_page_config(page_title="MGG Fintech — Centro de Control", page_icon=":material/account_balance_wallet:", layout="wide")

st.logo("https://mgg.com.co/wp-content/uploads/images/logo.png")

st.markdown("""
<style>
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0F2B46 0%, #11354A 100%);
    }
    [data-testid="stSidebar"] * {
        color: #CBD5E1 !important;
    }
    [data-testid="stSidebar"] [data-testid="stMarkdown"] p {
        color: #94A3B8 !important;
        font-weight: 500;
        text-transform: uppercase;
        font-size: 0.7rem;
        letter-spacing: 0.05em;
    }
    [data-testid="stSidebar"] .stPageLink p, [data-testid="stSidebar"] a span {
        color: #E2E8F0 !important;
        text-transform: none;
        font-size: 0.85rem;
    }
    [data-testid="stSidebar"] [aria-selected="true"] {
        background: rgba(41, 181, 232, 0.15) !important;
        border-left: 3px solid #29B5E8;
    }
    [data-testid="stMetric"] {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 12px 16px;
        border-left: 4px solid #29B5E8;
    }
    [data-testid="stMetricLabel"] {
        color: #475569 !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.03em;
    }
    [data-testid="stMetricValue"] {
        color: #0F172A !important;
        font-weight: 700 !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        border-bottom: 2px solid #E2E8F0;
    }
    .stTabs [data-baseweb="tab"] {
        color: #64748B;
        font-weight: 600;
        padding: 8px 24px;
        border-bottom: 3px solid transparent;
    }
    .stTabs [aria-selected="true"] {
        color: #0F2B46 !important;
        border-bottom: 3px solid #29B5E8 !important;
        background: transparent;
    }
    h1, h2, h3 {
        color: #0F2B46 !important;
    }
    [data-testid="stHeader"] {
        background: linear-gradient(90deg, #0F2B46 0%, #164E6B 100%);
    }
    div[data-testid="stContainer"] {
        border-color: #E2E8F0 !important;
    }
    .stDivider {
        border-color: #E2E8F0 !important;
    }
</style>
""", unsafe_allow_html=True)

orquestacion_pagos = st.Page("app_pages/orquestacion_pagos.py", title="Orquestacion Pagos", icon=":material/payments:")
smart_routing = st.Page("app_pages/smart_routing.py", title="Smart Routing", icon=":material/alt_route:")
wallets_digitales = st.Page("app_pages/wallets_digitales.py", title="Wallets Digitales", icon=":material/account_balance_wallet:")
pagos_embebidos = st.Page("app_pages/pagos_embebidos.py", title="Pagos Embebidos", icon=":material/integration_instructions:")

agregacion_cuentas = st.Page("app_pages/agregacion_cuentas.py", title="Agregacion Cuentas", icon=":material/link:")
enriquecimiento_datos = st.Page("app_pages/enriquecimiento_datos.py", title="Enriquecimiento Datos", icon=":material/auto_fix_high:")
scoring_datos_externos = st.Page("app_pages/scoring_datos_externos.py", title="Scoring Alternativo", icon=":material/score:")

experimentacion_ab = st.Page("app_pages/experimentacion_ab.py", title="Experimentacion A/B", icon=":material/science:")
optimizacion_onboarding = st.Page("app_pages/optimizacion_onboarding.py", title="Optimizacion Onboarding", icon=":material/person_add:")
growth_referidos = st.Page("app_pages/growth_referidos.py", title="Growth Referidos", icon=":material/share:")

interchange_optimization = st.Page("app_pages/interchange_optimization.py", title="Interchange Optimization", icon=":material/currency_exchange:")
suscripciones_financieras = st.Page("app_pages/suscripciones_financieras.py", title="Suscripciones", icon=":material/card_membership:")
cashback_rewards = st.Page("app_pages/cashback_rewards.py", title="Cashback & Rewards", icon=":material/redeem:")

banking_as_a_service = st.Page("app_pages/banking_as_a_service.py", title="Banking as a Service", icon=":material/api:")
gestion_partners = st.Page("app_pages/gestion_partners.py", title="Gestion Partners", icon=":material/handshake:")
revenue_sharing = st.Page("app_pages/revenue_sharing.py", title="Revenue Sharing", icon=":material/pie_chart:")

riesgo_transaccional_rt = st.Page("app_pages/riesgo_transaccional_rt.py", title="Riesgo Transaccional RT", icon=":material/shield:")
behavioral_scoring = st.Page("app_pages/behavioral_scoring.py", title="Behavioral Scoring", icon=":material/psychology:")
deteccion_abuso_producto = st.Page("app_pages/deteccion_abuso_producto.py", title="Deteccion Abuso", icon=":material/block:")

observabilidad_datos = st.Page("app_pages/observabilidad_datos.py", title="Observabilidad Datos", icon=":material/monitoring:")
feature_store = st.Page("app_pages/feature_store.py", title="Feature Store", icon=":material/storage:")
gestion_eventos = st.Page("app_pages/gestion_eventos.py", title="Gestion Eventos", icon=":material/bolt:")

insights_financieros = st.Page("app_pages/insights_financieros.py", title="Insights Financieros", icon=":material/lightbulb:")
alertas_proactivas = st.Page("app_pages/alertas_proactivas.py", title="Alertas Proactivas", icon=":material/notifications_active:")
recomendaciones_ahorro = st.Page("app_pages/recomendaciones_ahorro.py", title="Ahorro e Inversion", icon=":material/savings:")

pg = st.navigation({
    "Pagos y Ecosistema": [orquestacion_pagos, smart_routing, wallets_digitales, pagos_embebidos],
    "Open Banking": [agregacion_cuentas, enriquecimiento_datos, scoring_datos_externos],
    "Producto y Crecimiento": [experimentacion_ab, optimizacion_onboarding, growth_referidos],
    "Monetizacion": [interchange_optimization, suscripciones_financieras, cashback_rewards],
    "Partners y BaaS": [banking_as_a_service, gestion_partners, revenue_sharing],
    "Riesgo Ampliado": [riesgo_transaccional_rt, behavioral_scoring, deteccion_abuso_producto],
    "Operacion Cloud Native": [observabilidad_datos, feature_store, gestion_eventos],
    "Soporte Financiero Inteligente": [insights_financieros, alertas_proactivas, recomendaciones_ahorro],
})

pg.run()
