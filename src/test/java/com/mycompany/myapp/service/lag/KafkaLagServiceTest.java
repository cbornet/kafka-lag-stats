package com.mycompany.myapp.service.lag;

import com.mycompany.myapp.config.KafkaProperties;
import com.mycompany.myapp.service.lag.KafkaLagService.OffsetAndInstant;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

class KafkaLagServiceTest {

    private static final String TEST_TOPIC = "test_topic";
    private static final String TEST_KEY = "test_key";
    private static final int TEST_PARTITION = 6;
    private static final String TEST_GROUP = "test_group";
    private static final TopicPartition TEST_TOPIC_PARTITION = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    private static final ProducerRecord<String, String> TEST_RECORD = new ProducerRecord<>(TEST_TOPIC, TEST_KEY, "test_value");

    private static boolean started = false;
    private static KafkaContainer kafkaContainer;

    private static KafkaProperties kafkaProperties;
    private static ConsumerOffsetsReader offsetsReader;
    private static AdminClient adminClient;
    private static KafkaLagService lagService;

    @BeforeAll
    static void startServer() {
        kafkaProperties = new KafkaProperties();
        offsetsReader = mock(ConsumerOffsetsReader.class);
        adminClient = mock(AdminClient.class);
        lagService = spy(new KafkaLagService(kafkaProperties, offsetsReader, adminClient));
    }

    private static void startTestcontainer() {
        if (!started) {
            kafkaContainer = new KafkaContainer("5.3.1").withEnv("delete.topic.enable", "true");
            kafkaContainer.start();
            started = true;
        }
        createTopic(TEST_TOPIC);
        Map<String, String> consumerProps = new HashMap<>();
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        kafkaProperties.setConsumer(consumerProps);
        //System.setProperty("kafkaBootstrapServers", kafkaContainer.getBootstrapServers());
    }

    @Test
    void stddev() {
        List<Double> doubles = Arrays.asList(1d, 2d, 3d,4d);

        DoubleStats stddev = KafkaLagService.stddev(doubles);

        DoubleStats expected = new DoubleStats(2.5d, 1.118033988749895d);
        assertThat(stddev).isEqualTo(expected);
        assertThat(stddev.getStddevPercent()).isEqualTo(223.60679774997897d);
    }

    @Test
    void getPartitions() throws Exception {
        startTestcontainer();
        KafkaProducer<String, String> producer = createKafkaProducer();
        int expectedPartition = producer.send(TEST_RECORD).get().partition();

        int partition = lagService.getPartition(TEST_TOPIC, TEST_KEY);

        assertThat(partition).isEqualTo(expectedPartition);
        assertThat(partition).isEqualTo(TEST_PARTITION);
    }

    @Test
    void getProducerOffsets() throws Exception {
        startTestcontainer();
        KafkaProducer<String, String> producer = createKafkaProducer();
        producer.send(TEST_RECORD).get();

        Instant now = Instant.now();
        Instant oneMinuteAgo = now.minusSeconds(60);
        List<Instant> samplingInstants = Arrays.asList(now, oneMinuteAgo);
        List<OffsetAndInstant> producerOffsets = lagService.getProducerOffsets(TEST_TOPIC_PARTITION, samplingInstants);

        assertThat(producerOffsets).containsExactly(new OffsetAndInstant(1L, now), new OffsetAndInstant(0L, oneMinuteAgo));
    }

    @Nested
    class getConsumerOffsetsFromReadings {
        @Test
        void ok() {
            Instant twoMinutesAgo = Instant.now().minusSeconds(120);
            BlockingQueue<OffsetPoint> offsetPoints = mockGetGroupOffsetPoints();
            addOffsetPoints(offsetPoints, TEST_TOPIC_PARTITION, twoMinutesAgo, 1, 120);

            Optional<Long> offsets = lagService.getConsumerOffsetsFromReadings(TEST_GROUP, TEST_TOPIC_PARTITION, twoMinutesAgo.plusMillis(1100));

            assertThat(offsets).contains(1100L);
        }

        @Test
        void no_group() {
            Instant twoMinutesAgo = Instant.now().minusSeconds(120);
            BlockingQueue<OffsetPoint> offsetPoints = mockGetGroupOffsetPoints();
            addOffsetPoints(offsetPoints, TEST_TOPIC_PARTITION, twoMinutesAgo, 1, 120);

            Optional<Long> offsets = lagService.getConsumerOffsetsFromReadings("wrong_group", TEST_TOPIC_PARTITION, twoMinutesAgo.plusMillis(1100));

            assertThat(offsets).isEmpty();
        }

        @Test
        void only_before() {
            Instant now = Instant.now();
            Instant oneMinuteAgo = now.minusSeconds(60);
            BlockingQueue<OffsetPoint> offsetPoints = mockGetGroupOffsetPoints();
            addOffsetPoints(offsetPoints, TEST_TOPIC_PARTITION, oneMinuteAgo, 0, 59);

            Optional<Long> offsets = lagService.getConsumerOffsetsFromReadings(TEST_GROUP, TEST_TOPIC_PARTITION, now);

            assertThat(offsets).isEmpty();
        }

        @Test
        void only_after() {
            Instant oneMinuteAgo = Instant.now().minusSeconds(60);
            BlockingQueue<OffsetPoint> offsetPoints = mockGetGroupOffsetPoints();
            addOffsetPoints(offsetPoints, TEST_TOPIC_PARTITION, oneMinuteAgo, 1, 60);

            Optional<Long> offsets = lagService.getConsumerOffsetsFromReadings(TEST_GROUP, TEST_TOPIC_PARTITION, oneMinuteAgo);

            assertThat(offsets).isEmpty();
        }

        @Test
        void only_after_or_other_partition() {
            Instant oneMinuteAgo = Instant.now().minusSeconds(60);
            BlockingQueue<OffsetPoint> offsetPoints = mockGetGroupOffsetPoints();
            addOffsetPoints(offsetPoints, new TopicPartition(TEST_TOPIC, TEST_PARTITION + 1), oneMinuteAgo.minusSeconds(60), 0, 59);
            addOffsetPoints(offsetPoints, TEST_TOPIC_PARTITION, oneMinuteAgo, 1, 60);

            Optional<Long> offsets = lagService.getConsumerOffsetsFromReadings(TEST_GROUP, TEST_TOPIC_PARTITION, oneMinuteAgo);

            assertThat(offsets).isEmpty();
        }

        @Test
        void only_before_or_other_partition() {
            Instant oneMinuteAgo = Instant.now().minusSeconds(60);
            BlockingQueue<OffsetPoint> offsetPoints = mockGetGroupOffsetPoints();
            addOffsetPoints(offsetPoints, TEST_TOPIC_PARTITION, oneMinuteAgo.minusSeconds(60), 0, 59);
            addOffsetPoints(offsetPoints, new TopicPartition(TEST_TOPIC, TEST_PARTITION + 1), oneMinuteAgo, 1, 60);

            Optional<Long> offsets = lagService.getConsumerOffsetsFromReadings(TEST_GROUP, TEST_TOPIC_PARTITION, oneMinuteAgo);

            assertThat(offsets).isEmpty();
        }
    }

    @Nested
    class getConsumerLag {
        @Test
        void ok() {
            Instant twoMinutesAgo = Instant.now().minusSeconds(120);
            BlockingQueue<OffsetPoint> offsetPoints = mockGetGroupOffsetPoints();
            addOffsetPoints(offsetPoints, TEST_TOPIC_PARTITION, twoMinutesAgo, 1, 120);

            Instant testInstant = twoMinutesAgo.plusMillis(1100);
            MessageLag consumerLag = lagService.getConsumerLag(TEST_GROUP, TEST_TOPIC_PARTITION, 1500L, testInstant);

            assertThat(consumerLag).isEqualTo(new MessageLag(1100L, 1500L, 400L, testInstant));
        }

        @Test
        void empty() {
            Instant now = Instant.now();
            mockGetGroupOffsetPoints();

            MessageLag consumerLag = lagService.getConsumerLag(TEST_GROUP, TEST_TOPIC_PARTITION, 1500L, now);

            assertThat(consumerLag).isEqualTo(new MessageLag(null, 1500L, null, now));
        }
    }

    @ParameterizedTest
    @CsvSource({
            "42, 2, 0", // Consumer not lagging
            "3, 4, 1",  // Consumer lagging
            ",1,"       // No consumer offset
    })
    void getConsumerLags(Long consumerOffset, Long producerOffset, Long lag) throws Exception {
        Instant now = Instant.now();
        doReturn(Collections.singletonList(new OffsetAndInstant(producerOffset, now)))
                .when(lagService).getProducerOffsets(TEST_TOPIC_PARTITION, Collections.singletonList(now));

        BlockingQueue<OffsetPoint> offsetPoints = mockGetGroupOffsetPoints();

        Map<TopicPartition, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(TEST_TOPIC_PARTITION, consumerOffset);
        offsetPoints.put(new OffsetPoint(now.minusSeconds(1), partitionOffsets));
        offsetPoints.put(new OffsetPoint(now.plusSeconds(1), partitionOffsets));

        List<MessageLag> lags = lagService.getConsumerLags(TEST_GROUP, TEST_TOPIC_PARTITION, Collections.singletonList(now));

        assertThat(lags).containsExactly(new MessageLag(consumerOffset, producerOffset, lag, now));
    }

    @ParameterizedTest
    @CsvSource({
            "1000, 1, 2000, 1, 1000", // Happy path
            "0, 0, 1000, 1,",         // Previous lag 0
            "0, 1, 1000, 0,",         // Current lag 0
            ",, 1000, 1,",            // Previous no offset
            "0, 1,,,",                // Current no offset
    })
    void getConsumerSpeeds(Long previousOffset, Long previousLag, Long offset, Long lag, Double expectedSpeed) {
        Instant now = Instant.now();
        Instant oneSecondAgo = now.minusSeconds(1);
        Instant twoSecondsAgo = now.minusSeconds(2);
        List<Instant> samplingInstants = Arrays.asList(now, twoSecondsAgo, oneSecondAgo);
        Long previousPreviousOffset = previousOffset != null ? previousOffset/2 : null;

        doReturn(
                Arrays.asList(
                        new MessageLag(offset, null, lag, now),
                        new MessageLag(previousPreviousOffset, null, previousLag, twoSecondsAgo),
                        new MessageLag(previousOffset, null, previousLag, oneSecondAgo)
                )
        ).when(lagService).getConsumerLags(TEST_GROUP, TEST_TOPIC_PARTITION, samplingInstants);

        List<MessageSpeed> consumerSpeeds = lagService.getConsumerSpeeds(TEST_GROUP, TEST_TOPIC_PARTITION, samplingInstants);

        assertThat(consumerSpeeds).hasSize(3);
        assertThat(consumerSpeeds.get(0)).isEqualTo(
                new MessageSpeed(expectedSpeed, now, new MessageLag(offset, null, lag, now))
        );
        assertThat(consumerSpeeds.get(2)).isEqualTo(
                new MessageSpeed(null, twoSecondsAgo, new MessageLag(previousPreviousOffset, null, previousLag, twoSecondsAgo))
        );
    }

    @Test
    void getSpeedStats() {
        // 1. Mocker getConsumerSpeeds
        // 2. Tester une liste de speed dont un des speeds est null
        // 3. Ajouter toString et equals/hashcode à SpeedStats
        // 4. Vérifier que le résultat est ce qu'on attend.
    }

    private BlockingQueue<OffsetPoint> mockGetGroupOffsetPoints() {
        BlockingQueue<OffsetPoint> offsetPoints = new LinkedBlockingQueue<>();
        Map<String, BlockingQueue<OffsetPoint>> offsetPointsMap = new HashMap<>();
        offsetPointsMap.put(TEST_GROUP, offsetPoints);
        when(offsetsReader.getGroupOffsetPoints()).thenReturn(offsetPointsMap);
        return offsetPoints;
    }

    private void addOffsetPoints(BlockingQueue<OffsetPoint> offsetPoints, TopicPartition tp, Instant start, int from, int to) {
        IntStream.range(from,to)
                .mapToObj(start::plusSeconds)
                .map(instant -> {
                    Map<TopicPartition, Long> partitionOffsets = new HashMap<>();
                    partitionOffsets.put(tp, instant.toEpochMilli() - start.toEpochMilli());
                    return new OffsetPoint(instant, partitionOffsets);
                })
                .forEach(offsetPoints::add);
    }

    @NotNull
    private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("bootstrap.servers", kafkaContainer.getBootstrapServers());

        return new KafkaProducer<>(props);
    }

    private static void createTopic(String topicName) {
        // kafka container uses with embedded zookeeper
        // confluent platform and Kafka compatibility 5.1.x <-> kafka 2.1.x
        // kafka 2.1.x require option --zookeeper, later versions use --bootstrap-servers instead
        String deleteTopic =
                String.format(
                        "/usr/bin/kafka-topics --delete --zookeeper localhost:2181 --topic %s",
                        topicName);
        String createTopic =
                String.format(
                        "/usr/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 16 --topic %s",
                        topicName);
        try {
            kafkaContainer.execInContainer("/bin/sh", "-c", deleteTopic);
            final Container.ExecResult execResult = kafkaContainer.execInContainer("/bin/sh", "-c", createTopic);
            if (execResult.getExitCode() != 0) fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }


}
