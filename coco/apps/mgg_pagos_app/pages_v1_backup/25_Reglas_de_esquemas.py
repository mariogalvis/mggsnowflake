from use_case_layout import render_use_case

render_use_case(
    title="Reglas de esquemas",
    description="Reglas y mandatos de las redes de pago (Visa, Mastercard) y reguladores (SFC, PCI Council). Registra tipo regla, estado de cumplimiento y riesgo de sancion.",
    resuelve="Riesgo de multas millonarias por incumplimiento de mandatos de redes y reguladores que cambian frecuentemente y son dificiles de rastrear.",
    como_funciona="Cada mandato se registra con la red/regulador emisor, fecha efectiva, tipo de regla, estado de cumplimiento (cumple, en progreso, excepcion, incumple), plan de accion y estimacion del riesgo de sancion.",
    valor_negocio="Evitar multas que pueden alcanzar millones de dolares, mantener la licencia de procesamiento activa y demostrar a los reguladores un control proactivo del cumplimiento.",
)
