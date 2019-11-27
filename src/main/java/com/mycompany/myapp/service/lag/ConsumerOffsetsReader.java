package com.mycompany.myapp.service.lag;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Profile("!test")
public class ConsumerOffsetsReader implements InitializingBean, DisposableBean {

    private final Logger log = LoggerFactory.getLogger(ConsumerOffsetsReader.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static final int MAX_OFFSET_POINTS = 3600 * 4;

    private Map<String, BlockingQueue<OffsetPoint>> groupOffsetPoints = new HashMap<>();

    private final AdminClient client;

    public ConsumerOffsetsReader(AdminClient client) {
        this.client = client;
    }

    @Override
    public void destroy() {
        log.info("Shutdown Kafka offset reader");
        closed.set(true);
    }

    @Override
    public void afterPropertiesSet() {
        Thread readerThread = new Thread(() -> {
            try {
                log.info("Kafka offset reader started");
                while (!closed.get()) {
                    Thread.sleep(1000);
                    client.listConsumerGroups().all().get().stream()
                            .map(ConsumerGroupListing::groupId)
                            .forEach(this::getAndRecordOffsets);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
        readerThread.start();
    }

    private void getAndRecordOffsets(String groupId) {
        try {
            Map<TopicPartition, Long> endOffsets = getOffsets(groupId);
            recordOffsets(groupId, endOffsets);
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    private void recordOffsets(String groupId, Map<TopicPartition, Long> endOffsets) throws InterruptedException {
        if (!groupOffsetPoints.containsKey(groupId)) {
            groupOffsetPoints.put(groupId, new ArrayBlockingQueue<>(MAX_OFFSET_POINTS));
        }
        OffsetPoint offsetPoint = new OffsetPoint(Instant.now(), endOffsets);
        BlockingQueue<OffsetPoint> offsetPoints = groupOffsetPoints.get(groupId);
        if(!offsetPoints.offer(offsetPoint)){
            offsetPoints.poll();
            offsetPoints.put(offsetPoint);
        }
    }

    private Map<TopicPartition, Long> getOffsets(String groupId) throws InterruptedException, ExecutionException {
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        client.listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata().get()
            .forEach((k,v) -> endOffsets.put(k, v.offset()));
        return endOffsets;
    }


    public Map<String, BlockingQueue<OffsetPoint>> getGroupOffsetPoints() {
        return groupOffsetPoints;
    }

}
