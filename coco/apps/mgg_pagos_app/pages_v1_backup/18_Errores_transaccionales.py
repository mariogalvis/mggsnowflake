from use_case_layout import render_use_case

render_use_case(
    title="Errores transaccionales",
    description="Catalogo de errores en el procesamiento de transacciones. Registra tipo error, componente origen, frecuencia, impacto financiero y analisis de causa raiz.",
    resuelve="Errores recurrentes en el procesamiento que no se detectan a tiempo, generan transacciones fallidas y afectan la confiabilidad del sistema.",
    como_funciona="Cada error se clasifica por tipo (timeout, formato, conexion, criptografico), componente de origen, frecuencia, impacto financiero estimado y se documenta el analisis de causa raiz y la resolucion aplicada.",
    valor_negocio="Reducir la tasa de error del sistema priorizando los problemas de mayor impacto, mejorar la estabilidad del procesamiento y disminuir las transacciones fallidas que afectan la experiencia del usuario.",
)
