package com.mycompany.myapp.web.rest;

import com.mycompany.myapp.service.KafkaKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka-kafka")
public class KafkaKafkaResource {

    private final Logger log = LoggerFactory.getLogger(KafkaKafkaResource.class);

    private KafkaKafkaProducer kafkaProducer;

    public KafkaKafkaResource(KafkaKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping("/publish")
    public void sendMessageToKafkaTopic(@RequestParam("message") String message) {
        log.debug("REST request to send to Kafka topic the message : {}", message);
        this.kafkaProducer.send(message);
    }
}
