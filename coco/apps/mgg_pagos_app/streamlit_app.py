import streamlit as st

st.set_page_config(
    page_title="MGG Pagos",
    page_icon=":material/payments:",
    layout="wide",
)

LOGO_URL = "https://mgg.com.co/wp-content/uploads/images/logo.png"
if hasattr(st, "logo"):
    st.logo(LOGO_URL, size="large")
st.sidebar.image(LOGO_URL, use_container_width=True)

AREAS = {
    "Procesamiento de transacciones": {
        "icon": ":material/swap_horiz:",
        "pages": [
            st.Page("app_pages/transacciones_pago.py", title="Transacciones de pago", icon=":material/receipt_long:"),
            st.Page("app_pages/autorizaciones.py", title="Autorizaciones", icon=":material/verified:"),
            st.Page("app_pages/routing_transacciones.py", title="Routing de transacciones", icon=":material/route:"),
            st.Page("app_pages/mensajes_pago.py", title="Mensajes de pago", icon=":material/mail:"),
        ],
    },
    "Fraude y seguridad": {
        "icon": ":material/shield:",
        "pages": [
            st.Page("app_pages/fraude_tiempo_real.py", title="Fraude en tiempo real", icon=":material/security:"),
            st.Page("app_pages/dispositivos.py", title="Dispositivos", icon=":material/devices:"),
            st.Page("app_pages/tokenizacion.py", title="Tokenizacion", icon=":material/token:"),
            st.Page("app_pages/alertas_fraude.py", title="Alertas de fraude", icon=":material/notification_important:"),
        ],
    },
    "Comercios y adquirencia": {
        "icon": ":material/storefront:",
        "pages": [
            st.Page("app_pages/comercios.py", title="Comercios", icon=":material/store:"),
            st.Page("app_pages/afiliacion_comercios.py", title="Afiliacion de comercios", icon=":material/person_add:"),
            st.Page("app_pages/terminales_pos.py", title="Terminales POS", icon=":material/point_of_sale:"),
            st.Page("app_pages/performance_comercios.py", title="Performance de comercios", icon=":material/trending_up:"),
        ],
    },
    "Liquidacion y compensacion": {
        "icon": ":material/account_balance:",
        "pages": [
            st.Page("app_pages/liquidaciones.py", title="Liquidaciones", icon=":material/payments:"),
            st.Page("app_pages/comisiones.py", title="Comisiones", icon=":material/price_change:"),
            st.Page("app_pages/compensacion.py", title="Compensacion", icon=":material/balance:"),
        ],
    },
    "Disponibilidad y performance": {
        "icon": ":material/speed:",
        "pages": [
            st.Page("app_pages/uptime_sistema.py", title="Uptime del sistema", icon=":material/monitor_heart:"),
            st.Page("app_pages/latencia_transacciones.py", title="Latencia de transacciones", icon=":material/timer:"),
            st.Page("app_pages/errores_transaccionales.py", title="Errores transaccionales", icon=":material/error:"),
        ],
    },
    "Analitica de pagos": {
        "icon": ":material/analytics:",
        "pages": [
            st.Page("app_pages/volumen_transaccional.py", title="Volumen transaccional", icon=":material/bar_chart:"),
            st.Page("app_pages/tendencias_consumo.py", title="Tendencias de consumo", icon=":material/show_chart:"),
            st.Page("app_pages/analisis_mcc.py", title="Analisis MCC", icon=":material/category:"),
        ],
    },
    "Experiencia y autorizacion": {
        "icon": ":material/thumb_up:",
        "pages": [
            st.Page("app_pages/tasa_aprobacion.py", title="Tasa de aprobacion", icon=":material/check_circle:"),
            st.Page("app_pages/friccion_pagos.py", title="Friccion de pagos", icon=":material/block:"),
            st.Page("app_pages/autenticacion_3ds.py", title="Autenticacion 3DS", icon=":material/lock:"),
        ],
    },
    "Cumplimiento y esquemas": {
        "icon": ":material/gavel:",
        "pages": [
            st.Page("app_pages/reglas_esquemas.py", title="Reglas de esquemas", icon=":material/rule:"),
            st.Page("app_pages/monitoreo_cumplimiento.py", title="Monitoreo de cumplimiento", icon=":material/policy:"),
            st.Page("app_pages/auditoria_pagos.py", title="Auditoria de pagos", icon=":material/find_in_page:"),
        ],
    },
}

nav_dict = {}
for area_name, area_info in AREAS.items():
    nav_dict[area_name] = area_info["pages"]

page = st.navigation(nav_dict, position="sidebar")
page.run()
