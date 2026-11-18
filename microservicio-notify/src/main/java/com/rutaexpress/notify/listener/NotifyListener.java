package com.rutaexpress.notify.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.slf4j.*;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.nio.file.*;

@Component
public class NotifyListener {
    private static final Logger log = LoggerFactory.getLogger(NotifyListener.class);
    private final ObjectMapper json;
    public NotifyListener(ObjectMapper json) { this.json = json; }

    @RabbitListener(queues = "#{emailQueue.name}", ackMode = "MANUAL")
    public void email(Message m, Channel ch) throws IOException {
        handle(m, ch, "email", data -> "Aviso de creación para " + required(data, "clienteEmail"));
    }

    @RabbitListener(queues = "#{warehouseQueue.name}", ackMode = "MANUAL")
    public void warehouse(Message m, Channel ch) throws IOException {
        handle(m, ch, "warehouse", data -> "Ticket de picking para servicio " + required(data, "servicioId"));
    }

    @RabbitListener(queues = "#{labelQueue.name}", ackMode = "MANUAL")
    public void label(Message m, Channel ch) throws IOException {
        handle(m, ch, "label", data -> required(data, "codigoSeguimiento"));
    }

    private interface Processor { String process(JsonNode data); }

    private static String required(JsonNode data, String field) {
        String value = data.path(field).asText("");
        if (value.isBlank()) throw new IllegalArgumentException("Falta " + field);
        return value;
    }

    private void handle(Message m, Channel ch, String domain, Processor processor) throws IOException {
        var props = m.getMessageProperties();
        long tag = props.getDeliveryTag();
        String code = "?";
        Path result;
        try {
            JsonNode data = json.readTree(m.getBody());
            code = required(data, "codigoSeguimiento");
            if (!code.matches("[A-Za-z0-9_-]{1,100}")) throw new IllegalArgumentException("Código inválido");
            if (data.path("simularError").asBoolean(false)) throw new IllegalArgumentException("Error simulado");
            String content = processor.process(data);
            Path directory = Files.createDirectories(Path.of("resultados", domain));
            boolean pdf = domain.equals("label");
            result = directory.resolve(code + (pdf ? ".pdf" : ".txt"));
            Path temporary = Files.createTempFile(directory, "cmd-", ".tmp");
            try {
                if (pdf) writePdf(temporary, code);
                else Files.writeString(temporary, code + " " + content);
                Files.move(temporary, result, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } finally {
                Files.deleteIfExists(temporary);
            }
        } catch (Exception error) {
            log.error("Fallo cola={} codigo={} tag={}", props.getConsumerQueue(), code, tag, error);
            ch.basicNack(tag, false, false);
            return;
        }
        ch.basicAck(tag, false);
        log.info("ACK cola={} codigo={} resultado={}", props.getConsumerQueue(), code, result);
    }

    private void writePdf(Path path, String code) throws IOException {
        try (var doc = new PDDocument()) {
            var page = new PDPage();
            doc.addPage(page);
            try (var content = new PDPageContentStream(doc, page)) {
                content.beginText();
                content.setFont(PDType1Font.HELVETICA, 16);
                content.newLineAtOffset(48, 720);
                content.showText("RutaExpress - Guia de despacho " + code);
                content.endText();
            }
            doc.save(path.toFile());
        }
    }
}
