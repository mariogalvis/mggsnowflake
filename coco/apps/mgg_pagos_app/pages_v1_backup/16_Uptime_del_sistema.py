from use_case_layout import render_use_case

render_use_case(
    title="Uptime del sistema",
    description="Disponibilidad de cada componente critico del ecosistema de pagos (switch, gateway, HSM, motor fraude). Registra minutos disponibles, downtime, MTTR y MTBF.",
    resuelve="Falta de visibilidad sobre la disponibilidad real de cada componente del sistema de pagos y su impacto en el SLA comprometido (99.99%).",
    como_funciona="Cada componente se monitorea con minutos disponibles vs totales, eventos de downtime con causa raiz, tiempo medio de recuperacion (MTTR) y tiempo medio entre fallas (MTBF).",
    valor_negocio="Cumplir con SLAs de disponibilidad del 99.99%, priorizar inversiones en resiliencia en los componentes mas criticos y reducir el impacto financiero de cada minuto de caida.",
)
