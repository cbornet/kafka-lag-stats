package com.mycompany.myapp.web.rest;

import com.mycompany.myapp.config.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/kafka-kafka")
public class KafkaKafkaResource {

    private final Logger log = LoggerFactory.getLogger(KafkaKafkaResource.class);

    private final KafkaProperties kafkaProperties;
    private KafkaProducer<String, String> producer;
    private Map<String, SafeKafkaConsumer> consumers = new ConcurrentHashMap<>();

    public KafkaKafkaResource(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.producer = new KafkaProducer<>(kafkaProperties.getProducerProps());
    }

    @PostMapping(value = "/publish/{topic}")
    public PublishResult publish(@PathVariable String topic, @RequestParam String message, @RequestParam(required = false) String key) throws ExecutionException, InterruptedException {
        log.debug("REST request to send to Kafka topic {} with key {} the message : {}", topic, key, message);
        RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, message)).get();
        return new PublishResult(metadata.topic(), metadata.partition(), metadata.offset(), Instant.ofEpochMilli(metadata.timestamp()));
    }

    @PostMapping("/consumers")
    public void createConsumer(@RequestParam String name, @RequestParam("topic") List<String> topics, @RequestParam Map<String, String> params) {
        log.debug("REST request to create a Kafka consumer {} of Kafka topics {}", name, topics);
        Map<String, Object> consumerProps = kafkaProperties.getConsumerProps();
        consumerProps.putAll(params);
        consumerProps.remove("topic");
        consumerProps.remove("name");
        SafeKafkaConsumer consumer = new SafeKafkaConsumer(consumerProps);
        consumer.subscribe(topics);
        consumers.put(name, consumer);
    }

    @GetMapping("/consumers/{name}/records")
    public List<String> pollConsumer(@PathVariable String name, @RequestParam(defaultValue = "1000") int durationMs) {
        log.debug("REST request to get records of Kafka consumer {}", name);
        List<String> records = new ArrayList<>();
        SafeKafkaConsumer consumer = consumers.get(name);
        if (consumer == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Consumer not found!");
        }
        try {
            consumer.poll(Duration.ofMillis(durationMs)).forEach(record -> records.add(record.value()));
        } catch (WakeupException e) {
            log.trace("Waken up while polling", e);
        }
        return records;
    }

    @DeleteMapping("consumers/{name}")
    public void deleteConsumer(@PathVariable String name) {
        SafeKafkaConsumer consumer = consumers.get(name);
        if (consumer == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Consumer not found!");
        }
        consumer.wakeup();
        consumer.close();
        consumers.remove(name);
    }

    static class SafeKafkaConsumer extends KafkaConsumer<String, String> {

        public SafeKafkaConsumer(Map<String, Object> configs) {
            super(configs);
        }

        @Override
        public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
            return super.poll(timeout);
        }

        @Override
        public synchronized void subscribe(Collection<String> topics) {
            super.subscribe(topics);
        }

        @Override
        public synchronized void close(Duration timeout) {
            super.wakeup();
            super.close(timeout);
        }
    }

    private static class PublishResult {
        public final String topic;
        public final int partition;
        public final long offset;
        public final Instant timestamp;

        private PublishResult(String topic, int partition, long offset, Instant timestamp) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.timestamp = timestamp;
        }
    }
}
