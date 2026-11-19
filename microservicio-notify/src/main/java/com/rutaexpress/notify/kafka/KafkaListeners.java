package com.rutaexpress.notify.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.*;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import java.io.IOException;

@Configuration
public class KafkaListeners {
    private static final Logger log = LoggerFactory.getLogger(KafkaListeners.class);
    private final ProcesamientoEnvio procesamiento;
    public KafkaListeners(ProcesamientoEnvio procesamiento) { this.procesamiento = procesamiento; }

    @Bean
    DefaultErrorHandler kafkaErrors() {
        var handler = new DefaultErrorHandler((record, error) ->
                log.error("Descartado topic={} particion={} offset={} rex={} causa={}", record.topic(),
                        record.partition(), record.offset(), record.key(), (error.getCause() == null ? error : error.getCause()).getClass().getSimpleName()),
                new FixedBackOff(1000, 2));
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }
    @KafkaListener(topics = "${ruta.kafka.eventos}", groupId = "ruta-reporteria", concurrency = "2")
    public void negocio(ConsumerRecord<String,String> record) throws IOException { procesamiento.process(record, false); }

    @KafkaListener(topics = "${ruta.kafka.auditoria}", groupId = "ruta-auditoria")
    public void auditoria(ConsumerRecord<String,String> record) throws IOException { procesamiento.process(record, true); }
}
