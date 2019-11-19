package com.mycompany.myapp.service.lag;

import com.mycompany.myapp.service.lag.DoubleStats;
import com.mycompany.myapp.service.lag.MessageSpeed;

import java.util.List;
import java.util.Objects;

public class SpeedStats {
    private final DoubleStats meanSpeed;
    private final List<MessageSpeed> speeds;

    public SpeedStats(DoubleStats meanSpeed, List<MessageSpeed> speeds) {
        this.meanSpeed = meanSpeed;
        this.speeds = speeds;
    }

    public DoubleStats getMeanSpeed() {
        return meanSpeed;
    }

    public List<MessageSpeed> getSpeeds() {
        return speeds;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SpeedStats that = (SpeedStats) o;
        return Objects.equals(meanSpeed, that.meanSpeed) &&
            Objects.equals(speeds, that.speeds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(meanSpeed, speeds);
    }

    @Override
    public String toString() {
        return "SpeedStats{" +
            "meanSpeed=" + meanSpeed +
            ", speeds=" + speeds +
            '}';
    }
}
