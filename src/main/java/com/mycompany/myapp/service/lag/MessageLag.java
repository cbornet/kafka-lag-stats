package com.mycompany.myapp.service.lag;

import java.time.Instant;
import java.util.Objects;

public class MessageLag {
    private final Long consumerOffset;
    private final Long producerOffset;
    private final Long lagMessages;
    private final Instant timestamp;

    public MessageLag(Long consumerOffset, Long producerOffset, Long lagMessages, Instant timestamp) {
        this.consumerOffset = consumerOffset;
        this.producerOffset = producerOffset;
        this.lagMessages = lagMessages;
        this.timestamp = timestamp;
    }

    public Long getConsumerOffset() {
        return consumerOffset;
    }

    public Long getProducerOffset() {
        return producerOffset;
    }

    public Long getLagMessages() {
        return lagMessages;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageLag that = (MessageLag) o;
        return Objects.equals(consumerOffset, that.consumerOffset) &&
                Objects.equals(producerOffset, that.producerOffset) &&
                Objects.equals(lagMessages, that.lagMessages) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerOffset, producerOffset, lagMessages, timestamp);
    }

    @Override
    public String toString() {
        return "MessageLag{" +
                "consumerOffset=" + consumerOffset +
                ", producerOffset=" + producerOffset +
                ", lagMessages=" + lagMessages +
                ", timestamp=" + timestamp +
                '}';
    }
}
