from use_case_layout import render_use_case

render_use_case(
    title="Friccion de pagos",
    description="Puntos de friccion que experimenta el usuario al intentar pagar. Registra reintentos, fallos de autenticacion, abandonos, tiempos excesivos y revenue perdido.",
    resuelve="Transacciones abandonadas por una experiencia de pago frustrante: demasiados reintentos, autenticaciones fallidas, tiempos de respuesta lentos o errores no claros.",
    como_funciona="Se rastrean los puntos de friccion en el journey de pago: reintentos del usuario, fallos en autenticacion (3DS, OTP), abandonos en el checkout, tiempos de respuesta que superan umbrales y el revenue estimado perdido por cada tipo de friccion.",
    valor_negocio="Reducir el abandono en el checkout mejorando los puntos de mayor friccion. Un 1% de mejora en conversion puede representar millones en ventas recuperadas para los comercios y comisiones para el procesador.",
)
