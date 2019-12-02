package com.mycompany.myapp.service.lag;

import org.apache.kafka.clients.admin.AdminClient;

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class ConsumerOffsetsReaderTest {

    private static final String TEST_GROUP = "test_group";
    private static final String TEST_TOPIC = "test_topic";
    private static final int TEST_PARTITION = 6;
    private static final TopicPartition TEST_TOPIC_PARTITION = new TopicPartition(TEST_TOPIC, TEST_PARTITION);

    private static boolean started = false;
    private static KafkaContainer kafkaContainer;
    private Clock clock;

    private void startTestcontainer() {
        if (!started) {
            kafkaContainer = new KafkaContainer("5.3.1").withEnv("delete.topic.enable", "true");
            kafkaContainer.start();
            started = true;
        }
    }

    private  ConsumerOffsetsReader offsetsReader;
    private  AdminClient adminClient;


    @BeforeEach
    void startServer() {
        adminClient = mock(AdminClient.class);
        clock = Clock.fixed(Instant.parse("2019-01-01T00:00:00Z"), ZoneId.systemDefault());
        offsetsReader = spy(new ConsumerOffsetsReader(adminClient, clock));
    }

    @Test
    void getOffsets() throws InterruptedException, ExecutionException {
        ListConsumerGroupOffsetsResult mockListConsumerGroupOffsetsResults = mock(ListConsumerGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> future = new HashMap<>();
        future.put(TEST_TOPIC_PARTITION, new OffsetAndMetadata(42L));

        when(mockListConsumerGroupOffsetsResults.partitionsToOffsetAndMetadata())
            .thenReturn(KafkaFuture.completedFuture(future));

        when(adminClient.listConsumerGroupOffsets(TEST_GROUP)).thenReturn(mockListConsumerGroupOffsetsResults);

        Map<TopicPartition,Long> expected = new HashMap<>();
        expected.put(TEST_TOPIC_PARTITION, 42L);

        assertThat(offsetsReader.getOffsets(TEST_GROUP)).isEqualTo(expected);
    }
    @Nested
    class recordOffsets{
        @Test
        void empty() throws Exception {
            offsetsReader.recordOffsets(TEST_GROUP, new HashMap<>());
            OffsetPoint expected = new OffsetPoint(clock.instant(), new HashMap<>());
            assertThat(offsetsReader.getGroupOffsetPoints().get(TEST_GROUP).take()).isEqualTo(expected);
        }
        @Test
        void with_endOffsets() throws Exception {
            Map<TopicPartition, Long> endOffsets = new HashMap<>();
            endOffsets.put(TEST_TOPIC_PARTITION, 42L);

            offsetsReader.recordOffsets(TEST_GROUP, endOffsets);

            OffsetPoint expected = new OffsetPoint(clock.instant(), endOffsets);

            assertThat(offsetsReader.getGroupOffsetPoints().get(TEST_GROUP).take()).isEqualTo(expected);
        }

        @Test
        void ring_buffer_full() throws Exception {
            Map<TopicPartition, Long> endOffsets = new HashMap<>();
            endOffsets.put(TEST_TOPIC_PARTITION, 42L);

            BlockingQueue<OffsetPoint> queue = new ArrayBlockingQueue<>(2);

            Map<TopicPartition, Long> offsets = new HashMap<>();
            offsets.put(TEST_TOPIC_PARTITION, 41L);
            queue.add(new OffsetPoint(clock.instant(), offsets));
            queue.add(new OffsetPoint(clock.instant(), offsets));

            offsetsReader.getGroupOffsetPoints().put(TEST_GROUP, queue);

            offsetsReader.recordOffsets(TEST_GROUP, endOffsets);
            BlockingQueue<OffsetPoint> offsetPoints = offsetsReader.getGroupOffsetPoints().get(TEST_GROUP);

            assertThat(offsetPoints.take().getOffsets().get(TEST_TOPIC_PARTITION)).isEqualTo(41L);
            assertThat(offsetPoints.take().getOffsets().get(TEST_TOPIC_PARTITION)).isEqualTo(42L);
        }
    }

    @Test
    void getAndRecordOffsets(){


    }

}
