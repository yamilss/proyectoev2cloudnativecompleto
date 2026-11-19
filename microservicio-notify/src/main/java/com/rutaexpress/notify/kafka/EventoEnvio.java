package com.rutaexpress.notify.kafka;

public record EventoEnvio(String eventId, String eventType, String occurredAt, Long envioId,
                          String codigoSeguimiento, String estadoAnterior, String estado) {}
