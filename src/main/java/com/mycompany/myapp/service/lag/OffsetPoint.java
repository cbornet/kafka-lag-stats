package com.mycompany.myapp.service.lag;

import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetPoint that = (OffsetPoint) o;
        return Objects.equals(timestamp, that.timestamp) &&
            Objects.equals(offsets, that.offsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, offsets);
    }

    @Override
    public String toString() {
        return "OffsetPoint{" +
            "timestamp=" + timestamp +
            ", offsets=" + offsets +
            '}';
    }
}
