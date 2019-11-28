package com.mycompany.myapp.web.rest;

import com.mycompany.myapp.service.lag.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/api/kafka-lag")
@Profile("!test")
public class KafkaLagController {
    private static final int NUMBER_OF_SAMPLING_INSTANTS = 10;

    private final KafkaLagService lagService;
    private final Clock clock;

    public KafkaLagController(KafkaLagService lagService, Clock clock) {
        this.lagService = lagService;
        this.clock = clock;
    }

    /**
     * {@code GET  /partition} : get the partition for the given topic and key.
     *
     * @param topic the topic
     * @param key the partition key
     * @return the partition number
     */
    @GetMapping("/partition")
    public int getPartition(@RequestParam("topic") String topic, @RequestParam("key") String key) {
        return lagService.getPartition(topic, key);
    }

    /**
     * {@code GET  /lags} : get the last lags in time for a single consumer
     *
     * @param group the consumer group of the consumer
     * @param topic the topic subscribed by the consumer
     * @param partition the partition of the consumer (give partition or key but not both)
     * @param key the partition key used to get the partition of the consumer  (give partition or key but not both)
     * @return the last lags of the consumer
     */
    @GetMapping("/lags")
    public List<MessageLag> getLags(
        @RequestParam String group,
        @RequestParam String topic,
        @RequestParam(required = false) Integer partition,
        @RequestParam(required = false) String key) {
        List<Instant> samplingInstants = getSamplingInstantsFromNow(NUMBER_OF_SAMPLING_INSTANTS);
        return lagService.getConsumerLags(group,  getPartitionFromParams(topic, partition, key), samplingInstants);
    }

    /**
     * {@code GET  /speeds} : get the last consumption speeds in time for a single consumer.
     *
     * @param group the consumer group of the consumer
     * @param topic the topic subscribed by the consumer
     * @param partition the partition of the consumer (give partition or key but not both)
     * @param key the partition key used to get the partition of the consumer  (give partition or key but not both)
     * @return the last consumption speeds of the consumer
     */
    @GetMapping("/speeds")
    public List<MessageSpeed> getSpeeds(
        @RequestParam String group,
        @RequestParam String topic,
        @RequestParam(required = false) Integer partition,
        @RequestParam(required = false) String key) {
        List<Instant> samplingInstants = getSamplingInstantsFromNow(NUMBER_OF_SAMPLING_INSTANTS);
        return lagService.getConsumerSpeeds(group,  getPartitionFromParams(topic, partition, key), samplingInstants);
    }

    /**
     * {@code GET  /speed-stats} : get the average consumption speed of a given consumer
     *
     * @param group the consumer group of the consumer
     * @param topic the topic subscribed by the consumer
     * @param partition the partition of the consumer (give partition or key but not both)
     * @param key the partition key used to get the partition of the consumer  (give partition or key but not both)
     * @return the average speed of the consumer
     */
    @GetMapping("/speed-stats")
    public SpeedStats getSpeedStats(
        @RequestParam String group,
        @RequestParam String topic,
        @RequestParam(required = false) Integer partition,
        @RequestParam(required = false) String key) {
        List<Instant> samplingInstants = getSamplingInstantsFromNow(NUMBER_OF_SAMPLING_INSTANTS);
        return lagService.getSpeedStats(group, getPartitionFromParams(topic, partition, key), samplingInstants);
    }

    /**
     * {@code GET  /messages-remaining} : get the number of messages that a consumer still has to consume to reach a message published at a given time
     *
     * @param group the consumer group of the consumer
     * @param topic the topic subscribed by the consumer
     * @param partition the partition on which the message was published (give partition or key but not both)
     * @param key the partition key used when publishing the message  (give partition or key but not both)
     * @param publishTimestamp the timestamp at which the message was published
     * @return the number of messages that still have to be consumed
     */
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

    /**
     * {@code GET  /time-remaining} : get the time that a consumer still needs to consume a message published at a given time
     * @param group the consumer group of the consumer
     * @param topic the topic subscribed by the consumer
     * @param partition the partition on which the message was published (give partition or key but not both)
     * @param key the partition key used when publishing the message  (give partition or key but not both)
     * @param publishTimestamp the timestamp at which the message was published
     * @return the time that the consumer still needs to consume the message
     */
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

    /**
     * {@code GET  /time-remaining} : get the average time that a consumer still needs to consume a message published
     * at a given time (when you don't know the key or partition)
     * 
     * @param group the consumer group of the consumer
     * @param topic the topic subscribed by the consumer
     * @param publishTimestamp the timestamp at which the message was published
     * @return the time that the consumer still needs to consume the message
     */
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
