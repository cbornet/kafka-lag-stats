package com.mycompany.myapp.service.lag;

import com.mycompany.myapp.web.rest.KafkaKafkaResource;

import java.util.Objects;

public class TimeRemaining {
    private final int partition;
    private final double timeRemaining;
    private final MessageLag messageLag;
    private final SpeedStats speedStats;

    public TimeRemaining(int partition, double timeRemaining, MessageLag messageLag, SpeedStats speedStats) {
        this.partition = partition;
        this.timeRemaining = timeRemaining;
        this.messageLag = messageLag;
        this.speedStats = speedStats;
    }

    public double getTimeRemaining() {
        return timeRemaining;
    }

    public SpeedStats getSpeedStats() {
        return speedStats;
    }

    public MessageLag getMessageLag() {
        return messageLag;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeRemaining that = (TimeRemaining) o;
        return partition == that.partition &&
            Double.compare(that.timeRemaining, timeRemaining) == 0 &&
            Objects.equals(messageLag, that.messageLag) &&
            Objects.equals(speedStats, that.speedStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, timeRemaining, messageLag, speedStats);
    }

    @Override
    public String toString() {
        return "TimeRemaining{" +
            "partition=" + partition +
            ", timeRemaining=" + timeRemaining +
            ", messageLag=" + messageLag +
            ", speedStats=" + speedStats +
            '}';
    }
}
