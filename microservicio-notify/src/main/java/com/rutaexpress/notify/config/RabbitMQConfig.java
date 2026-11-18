package com.rutaexpress.notify.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@org.springframework.context.annotation.Import(com.rutaexpress.messaging.CommandTopology.class)
@Configuration
public class RabbitMQConfig {

    @Bean
    public SimpleMessageConverter messageConverter() {
        return new SimpleMessageConverter();
    }
}
