package com.rutaexpress.kafkaadmin;

import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/kafka/topics")
public class TopicController {
    private final Admin admin;
    public TopicController(Admin admin) { this.admin = admin; }

    public record Create(String name, int partitions, short replicationFactor) {}
    public record Settings(Long retentionMs, String cleanupPolicy) {}
    public record Partition(int partition, int leader, List<Integer> replicas, List<Integer> isr) {}

    private ConfigResource resource(String name) {
        if (name == null || !name.matches("[a-zA-Z0-9._-]{1,249}") ||
                name.startsWith("__") || name.equals(".") || name.equals(".."))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Nombre de tópico inválido");
        return new ConfigResource(ConfigResource.Type.TOPIC, name);
    }

    @GetMapping
    public Set<String> list() throws Exception { return admin.listTopics().names().get(); }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public void create(@RequestBody Create body) throws Exception {
        resource(body.name());
        if (body.partitions() < 1 || body.replicationFactor() < 1 || body.replicationFactor() > 3)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Particiones o réplicas inválidas");
        admin.createTopics(List.of(new NewTopic(body.name(), body.partitions(), body.replicationFactor()))).all().get();
    }

    @GetMapping("/{name}")
    public List<Partition> describe(@PathVariable String name) throws Exception {
        resource(name);
        var topic = admin.describeTopics(List.of(name)).allTopicNames().get().get(name);
        return topic.partitions().stream().map(p -> new Partition(p.partition(),
                p.leader() == null ? -1 : p.leader().id(), p.replicas().stream().map(n -> n.id()).toList(),
                p.isr().stream().map(n -> n.id()).toList())).toList();
    }

    @GetMapping("/{name}/config")
    public Map<String,String> config(@PathVariable String name) throws Exception {
        var ref = resource(name);
        var cfg = admin.describeConfigs(List.of(ref)).all().get().get(ref);
        return Map.of("retention.ms", cfg.get("retention.ms").value(), "cleanup.policy", cfg.get("cleanup.policy").value());
    }

    @PatchMapping("/{name}/config")
    public Map<String,String> update(@PathVariable String name, @RequestBody Settings body) throws Exception {
        var ref = resource(name);
        List<AlterConfigOp> ops = new ArrayList<>();
        if (body.retentionMs() != null && body.retentionMs() >= -1)
            ops.add(set("retention.ms", body.retentionMs().toString()));
        else if (body.retentionMs() != null)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Retención inválida");
        if (body.cleanupPolicy() != null) {
            if (!Set.of("delete","compact","compact,delete","delete,compact").contains(body.cleanupPolicy()))
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Política inválida");
            ops.add(set("cleanup.policy", body.cleanupPolicy()));
        }
        if (ops.isEmpty()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Indica al menos una propiedad");
        admin.incrementalAlterConfigs(Map.of(ref, ops)).all().get();
        return config(name);
    }

    private AlterConfigOp set(String key, String value) {
        return new AlterConfigOp(new ConfigEntry(key,value), AlterConfigOp.OpType.SET);
    }

    @DeleteMapping("/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String name) throws Exception {
        resource(name);
        admin.deleteTopics(List.of(name)).all().get();
    }

    @ExceptionHandler(ExecutionException.class)
    public ResponseEntity<String> failure(ExecutionException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof UnknownTopicOrPartitionException) return ResponseEntity.status(404).body("Tópico inexistente");
        if (cause instanceof TopicExistsException) return ResponseEntity.status(409).body("El tópico ya existe");
        if (cause instanceof InvalidConfigurationException || cause instanceof InvalidReplicationFactorException)
            return ResponseEntity.badRequest().body("Configuración incompatible");
        return ResponseEntity.status(503).body("Kafka no completó la operación: " + cause.getClass().getSimpleName());
    }
}
