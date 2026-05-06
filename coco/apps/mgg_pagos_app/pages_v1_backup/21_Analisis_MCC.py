from use_case_layout import render_use_case

render_use_case(
    title="Analisis MCC",
    description="Analisis detallado por categoria comercial (MCC) que mide volumen, ticket promedio, tasa de aprobacion, fraude, contracargos e interchange.",
    resuelve="Falta de entendimiento de la composicion del portafolio por industria para identificar MCCs de alto riesgo y oportunidades de crecimiento por vertical.",
    como_funciona="Cada MCC se analiza con volumen total, numero de transacciones, ticket promedio, tasa de aprobacion, tasa de fraude, ratio de contracargos y costo promedio de interchange. Se compara con benchmarks de industria.",
    valor_negocio="Identificar las verticales mas rentables para enfocar la adquisicion de comercios, detectar MCCs con fraude elevado para ajustar reglas y optimizar el pricing por categoria.",
)
