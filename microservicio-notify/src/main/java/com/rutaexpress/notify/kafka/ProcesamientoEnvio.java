package com.rutaexpress.notify.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.*;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;

@Service
public class ProcesamientoEnvio {
    private static final Logger log = LoggerFactory.getLogger(ProcesamientoEnvio.class);
    private static final Set<String> ESTADOS = Set.of("CREADO","ACEPTADO","EN_BODEGA","EN_RUTA","ENTREGADO","CANCELADO");
    private final ObjectMapper json;
    public ProcesamientoEnvio(ObjectMapper json) { this.json = json; }

    public void process(ConsumerRecord<String,String> record, boolean audit) throws IOException {
        EventoEnvio event;
        try {
            event = json.readValue(record.value(), EventoEnvio.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON inválido");
        }
        validate(event, record.key(), audit);
        Path dir = audit ? Path.of("resultados/kafka/auditoria") : Path.of("resultados/kafka/reporteria", event.estado());
        Files.createDirectories(dir);
        Path dest = dir.resolve(event.eventId() + ".json");
        Path tmp = Files.createTempFile(dir, "evento-", ".tmp");
        try {
            Files.writeString(tmp, json.writeValueAsString(event));
            Files.move(tmp, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally { Files.deleteIfExists(tmp); }
        log.info("Procesado topic={} particion={} offset={} tipo={} rex={} resultado={}", record.topic(),
                record.partition(), record.offset(), event.eventType(), record.key(), dest);
    }

    private void validate(EventoEnvio e, String key, boolean audit) {
        if (e == null || e.eventId() == null || e.occurredAt() == null || e.envioId() == null || e.envioId() < 1 ||
                key == null || !key.matches("REX-[A-Za-z0-9-]+") || !key.equals(e.codigoSeguimiento()) ||
                e.estado() == null || !ESTADOS.contains(e.estado()))
            throw new IllegalArgumentException("Evento sin identificación o estado válido");
        UUID.fromString(e.eventId());
        try { Instant.parse(e.occurredAt()); } catch (DateTimeParseException ex) { throw new IllegalArgumentException("Fecha inválida"); }
        boolean alta = e.estadoAnterior() == null;
        String expected = (audit ? "AUDITORIA_" : "") + (alta ? "ENVIO_REGISTRADO" : "ESTADO_ACTUALIZADO");
        if (!expected.equals(e.eventType()) || (alta && !"CREADO".equals(e.estado())) ||
                (!alta && (!ESTADOS.contains(e.estadoAnterior()) || e.estadoAnterior().equals(e.estado()))))
            throw new IllegalArgumentException("Tipo o transición inválida: " + e.eventType());
    }
}
