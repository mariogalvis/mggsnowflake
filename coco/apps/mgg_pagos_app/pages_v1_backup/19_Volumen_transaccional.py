from use_case_layout import render_use_case

render_use_case(
    title="Volumen transaccional",
    description="Agregados diarios de volumen transaccional segmentados por canal, red, tipo tarjeta, ciudad y tipo transaccion. Permite analizar tendencias de crecimiento.",
    resuelve="Necesidad de entender la evolucion del negocio de pagos dia a dia, comparar canales y regiones, y detectar cambios en el comportamiento transaccional.",
    como_funciona="Se agregan las transacciones diariamente por multiples dimensiones: canal (POS, e-commerce, QR, ATM), red (Visa, MC, Redeban), tipo tarjeta (credito, debito), ciudad y tipo de transaccion.",
    valor_negocio="Tomar decisiones estrategicas basadas en tendencias reales de volumen, identificar canales en crecimiento para invertir y detectar caidas anomalas que requieran atencion inmediata.",
)
