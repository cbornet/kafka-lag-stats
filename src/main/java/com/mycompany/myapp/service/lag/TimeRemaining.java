package com.mycompany.myapp.service.lag;

import com.mycompany.myapp.web.rest.KafkaKafkaResource;

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
}
