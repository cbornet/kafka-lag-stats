package com.mycompany.myapp.service.lag;

import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.Map;

public class OffsetPoint {
    private final Instant timestamp;
    private final Map<TopicPartition, Long> offsets;

    OffsetPoint(Instant timestamp, Map<TopicPartition, Long> offsets) {
        this.timestamp = timestamp;
        this.offsets = offsets;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Map<TopicPartition, Long> getOffsets() {
        return offsets;
    }
}
