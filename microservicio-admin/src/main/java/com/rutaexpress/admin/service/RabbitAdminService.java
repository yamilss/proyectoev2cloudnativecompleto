package com.rutaexpress.admin.service;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

@Service
public class RabbitAdminService {

    private final RabbitAdmin rabbitAdmin;

    private final org.springframework.web.client.RestClient management;

    public RabbitAdminService(RabbitAdmin rabbitAdmin,
            @org.springframework.beans.factory.annotation.Value("${rabbit.management.url:http://${spring.rabbitmq.host}:15672}") String url,
            @org.springframework.beans.factory.annotation.Value("${spring.rabbitmq.username}") String user,
            @org.springframework.beans.factory.annotation.Value("${spring.rabbitmq.password}") String password) {
        management = org.springframework.web.client.RestClient.builder().baseUrl(url)
                .defaultHeaders(headers -> headers.setBasicAuth(user, password)).build();
        this.rabbitAdmin = rabbitAdmin;
    }

    public void createQueue(String queueName) {
        rabbitAdmin.declareQueue(new Queue(queueName, true));
    }

    public boolean deleteQueue(String queueName) {
        return rabbitAdmin.deleteQueue(queueName);
    }

    public void createExchange(String exchangeName, String type) {
        Exchange exchange = switch (type.toLowerCase()) {
            case "topic" -> new TopicExchange(exchangeName);
            case "fanout" -> new FanoutExchange(exchangeName);
            case "direct" -> new DirectExchange(exchangeName);
            default -> throw new IllegalArgumentException("Tipo permitido: direct, topic o fanout");
        };
        rabbitAdmin.declareExchange(exchange);
    }

    public boolean deleteExchange(String exchangeName) {
        return rabbitAdmin.deleteExchange(exchangeName);
    }

    public void createBinding(String queueName, String exchangeName, String routingKey) {
        rabbitAdmin.declareBinding(binding(queueName, exchangeName, routingKey));
    }

    public void deleteBinding(String queueName, String exchangeName, String routingKey) {
        rabbitAdmin.removeBinding(binding(queueName, exchangeName, routingKey));
    }

    private Binding binding(String queue, String exchange, String routingKey) {
        var info = management.get().uri("/api/exchanges/{vhost}/{exchange}", "/", exchange)
                .retrieve().onStatus(status -> status.value() == 404, (req, res) -> {
                    throw new IllegalArgumentException("Exchange inexistente");
                }).body(com.fasterxml.jackson.databind.JsonNode.class);
        if (rabbitAdmin.getQueueInfo(queue) == null) throw new IllegalArgumentException("Cola inexistente");
        String key = routingKey == null ? "" : routingKey;
        String type = info.path("type").asText();
        switch (type) {
            case "fanout" -> {
                if (!key.isEmpty()) throw new IllegalArgumentException("Fanout requiere routing key vacía");
            }
            case "direct", "topic" -> {
                if (key.isBlank()) throw new IllegalArgumentException("Falta routing key");
                if (type.equals("topic") && !key.matches("([^.*#\\s]+|[*#])(\\.([^.*#\\s]+|[*#]))*"))
                    throw new IllegalArgumentException("Patrón topic inválido");
            }
            default -> throw new IllegalArgumentException("Tipo no soportado");
        }
        return new Binding(queue, Binding.DestinationType.QUEUE, exchange, key, null);
    }
}
