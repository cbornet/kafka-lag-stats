package com.mycompany.myapp.web.rest;

import com.mycompany.myapp.config.KafkaProperties;
import com.mycompany.myapp.service.KafkaKafkaProducer;
import com.mycompany.myapp.service.lag.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/api/kafka-kafka")
@Profile("!test")
public class KafkaKafkaResource {

    private static final int NUMBER_OF_SAMPLING_INSTANTS = 10;
    private final Logger log = LoggerFactory.getLogger(KafkaKafkaResource.class);

    private KafkaKafkaProducer kafkaProducer;

    private final KafkaLagService lagService;

    private final Clock clock;

    public KafkaKafkaResource(KafkaKafkaProducer kafkaProducer, KafkaLagService lagService, Clock clock) {
        this.kafkaProducer = kafkaProducer;
        this.lagService = lagService;
        this.clock = clock;
    }

    @PostMapping("/publish")
    public void sendMessageToKafkaTopic(@RequestParam("message") String message, @RequestParam(value = "key", required = false) String key) {
        log.debug("REST request to send to Kafka topic the message : {}", message);
        if(key == null) {
            this.kafkaProducer.send(message);
        } else {
            kafkaProducer.send(message, key);
        }
    }

    @GetMapping("/partition")
    public int getPartition(@RequestParam("topic") String topic, @RequestParam("key") String key) {
        return lagService.getPartition(topic, key);
    }

    @GetMapping("/lags")
    public List<MessageLag> getLags(
            @RequestParam String group,
            @RequestParam String topic,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) String key) {
        List<Instant> samplingInstants = getSamplingInstantsFromNow(NUMBER_OF_SAMPLING_INSTANTS);
        return lagService.getConsumerLags(group,  getPartitionFromParams(topic, partition, key), samplingInstants);
    }

    @GetMapping("/speeds")
    public List<MessageSpeed> getSpeeds(
            @RequestParam String group,
            @RequestParam String topic,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) String key) {
        List<Instant> samplingInstants = getSamplingInstantsFromNow(NUMBER_OF_SAMPLING_INSTANTS);
        return lagService.getConsumerSpeeds(group,  getPartitionFromParams(topic, partition, key), samplingInstants);
    }

    @GetMapping("/speed-stats")
    public SpeedStats getSpeedStats(
            @RequestParam String group,
            @RequestParam String topic,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) String key) {
        List<Instant> samplingInstants = getSamplingInstantsFromNow(NUMBER_OF_SAMPLING_INSTANTS);
        return lagService.getSpeedStats(group, getPartitionFromParams(topic, partition, key), samplingInstants);
    }

    @GetMapping("/messages-remaining")
    public MessageLag getMessagesToPublishTimestamp(
            @RequestParam String group,
            @RequestParam String topic,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) String key,
            @RequestParam(required = false) String publishTimestamp)
            throws ExecutionException, InterruptedException {
        return lagService.getMessagesToPublishTimestamp(group, getPartitionFromParams(topic, partition, key), publishTimestamp);
    }

    @GetMapping("/time-remaining")
    public TimeRemaining getTimeRemaining(
            @RequestParam String group,
            @RequestParam String topic,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) String key,
            @RequestParam(required = false) String publishTimestamp) throws ExecutionException, InterruptedException {
        List<Instant> samplingInstants = getSamplingInstantsFromNow(NUMBER_OF_SAMPLING_INSTANTS);
        return lagService.getTimeRemaining(group, getPartitionFromParams(topic, partition, key), publishTimestamp, samplingInstants);
    }

    @GetMapping("/time-remaining-stats")
    public TimeRemainingStats getTimeRemainingStats(
            @RequestParam String group,
            @RequestParam String topic,
            @RequestParam(required = false) String publishTimestamp) {
        List<Instant> samplingInstants = getSamplingInstantsFromNow(NUMBER_OF_SAMPLING_INSTANTS);
        return lagService.getTimeRemainingStats(group, topic, publishTimestamp, samplingInstants);
    }

    private TopicPartition getPartitionFromParams(String topic, Integer partition, String key) {
        if (partition == null && key == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Either partition or key must be specified");
        }
        if (partition != null && key != null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Either partition or key must be specified, not both");
        }
        return new TopicPartition(topic, key != null ? lagService.getPartition(topic, key) : partition);
    }

    private List<Instant> getSamplingInstantsFromNow(int numberOfInstants) {
        // Start 2 seconds ago to have high probability that the consumer offset has been read.
        Instant now = clock.instant().minusSeconds(2);
        return IntStream.range(0, numberOfInstants)
                .mapToObj(i -> now.minusSeconds(i * 60L))
                .collect(Collectors.toList());
    }

}
