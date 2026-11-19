package com.rutaexpress.kafkaadmin;

import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaAdminApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    Admin admin(@Value("${spring.kafka.bootstrap-servers}") String brokers) {
        return Admin.create(Map.of("bootstrap.servers", brokers, "default.api.timeout.ms", "10000"));
    }
}
