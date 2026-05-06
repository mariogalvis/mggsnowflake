from use_case_layout import render_use_case

render_use_case(
    title="Auditoria de pagos",
    description="Trazabilidad inmutable de cada evento critico en el ecosistema de pagos. Registra quien hizo que, cuando, desde donde y con que resultado.",
    resuelve="Falta de trazabilidad sobre las acciones realizadas en el sistema de pagos que dificulta investigaciones de fraude, auditorias y cumplimiento de PCI DSS requisito 10.",
    como_funciona="Cada evento critico se registra de forma inmutable: usuario, accion, timestamp, IP de origen, componente afectado, datos antes/despues del cambio y si involucro datos sensibles (PAN, claves).",
    valor_negocio="Cumplir con PCI DSS (requisito 10 de trazabilidad), facilitar investigaciones de fraude interno, responder auditorias regulatorias con evidencia solida y detectar accesos no autorizados a datos sensibles.",
)
