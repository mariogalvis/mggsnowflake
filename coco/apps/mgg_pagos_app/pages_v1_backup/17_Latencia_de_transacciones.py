from use_case_layout import render_use_case

render_use_case(
    title="Latencia de transacciones",
    description="Tiempos de respuesta desglosados por componente (switch, routing, fraude, red, emisor). Incluye percentiles (p50, p95, p99) y deteccion de degradaciones.",
    resuelve="Dificultad para identificar cual componente del flujo de autorizacion esta causando lentitud y afectando la experiencia del usuario al pagar.",
    como_funciona="Cada transaccion registra el tiempo consumido en cada etapa: procesamiento del switch, evaluacion de fraude, enrutamiento, comunicacion con la red y respuesta del emisor. Se calculan percentiles para detectar degradaciones.",
    valor_negocio="Optimizar la experiencia de pago reduciendo la latencia total, identificar componentes que necesitan escalamiento y cumplir con los SLAs de tiempo de respuesta de las redes.",
)
