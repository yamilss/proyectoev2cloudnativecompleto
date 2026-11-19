package com.rutaexpress.shipments.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rutaexpress.shipments.entity.Envio;
import com.rutaexpress.shipments.entity.EstadoEnvio;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

@Component
public class EventPublisher implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
    private final ObjectMapper json = new ObjectMapper();
    private final KafkaProducer<String,String> producer;
    private final String negocio;
    private final String auditoria;

    public EventPublisher(@Value("${spring.kafka.bootstrap-servers}") String brokers,
                          @Value("${ruta.kafka.eventos}") String negocio,
                          @Value("${ruta.kafka.auditoria}") String auditoria) {
        this.negocio = negocio;
        this.auditoria = auditoria;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("enable.idempotence", true);
        props.put("max.block.ms", 3000);
        props.put("delivery.timeout.ms", 15000);
        props.put("request.timeout.ms", 5000);
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    public void publish(Envio envio, EstadoEnvio anterior) {
        String id = UUID.randomUUID().toString();
        String time = Instant.now().toString();
        String action = anterior == null ? "ENVIO_REGISTRADO" : "ESTADO_ACTUALIZADO";
        for (String topic : new String[]{negocio, auditoria}) {
            String type = topic.equals(auditoria) ? "AUDITORIA_" + action : action;
            var event = new EventoEnvio(id, type, time, envio.getId(), envio.getCodigoSeguimiento(),
                    anterior == null ? null : anterior.name(), envio.getEstado().name());
            try {
                producer.send(new ProducerRecord<>(topic, event.codigoSeguimiento(), json.writeValueAsString(event)),
                        (meta, error) -> {
                            if (error != null) log.error("Error publicando topic={} eventId={} rex={}", topic, id, event.codigoSeguimiento(), error);
                            else log.info("Publicado topic={} partition={} offset={} eventId={} rex={}", topic, meta.partition(), meta.offset(), id, event.codigoSeguimiento());
                        });
            } catch (Exception error) {
                log.error("Error preparando publicación topic={} eventId={} rex={}", topic, id, event.codigoSeguimiento(), error);
            }
        }
    }
    @Override public void close() { producer.close(); }
}
