package com.mycompany.myapp.service.lag;


import java.time.Instant;
import java.util.Objects;

public class MessageSpeed {
    private final Double speed;
    private final Instant timestamp;
    private final MessageLag lag;

    public MessageSpeed(Double speed, Instant timestamp, MessageLag lag) {
        this.speed = speed;
        this.timestamp = timestamp;
        this.lag = lag;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Double getSpeed() {
        return speed;
    }

    public MessageLag getLag() {
        return lag;
    }

    @Override
    public String toString() {
        return "MessageSpeed{" +
                "speed=" + speed +
                ", timestamp=" + timestamp +
                ", lag=" + lag +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageSpeed that = (MessageSpeed) o;
        return Objects.equals(speed, that.speed) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(lag, that.lag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(speed, timestamp, lag);
    }
}
