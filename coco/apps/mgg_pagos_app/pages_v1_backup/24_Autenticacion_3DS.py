from use_case_layout import render_use_case

render_use_case(
    title="Autenticacion 3DS",
    description="Flujo de autenticacion 3D Secure (Verified by Visa, Mastercard Identity Check). Registra version, tipo desafio, resultado, tasa frictionless y abandono.",
    resuelve="Configuracion suboptima de 3D Secure que genera demasiada friccion al usuario (abandonos) o demasiado poco (fraude en ecommerce).",
    como_funciona="Se registra cada flujo 3DS con version (1.0/2.x), tipo de desafio (OTP, biometrico, frictionless), resultado (exito, fallo, abandono), tasa de frictionless y su impacto en la autorizacion posterior.",
    valor_negocio="Optimizar la configuracion de 3DS para maximizar la seguridad con minima friccion: aumentar la tasa frictionless donde sea seguro y aplicar desafio solo cuando el riesgo lo justifique.",
)
