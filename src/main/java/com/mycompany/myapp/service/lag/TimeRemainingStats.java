package com.mycompany.myapp.service.lag;

import java.util.Map;
import java.util.Objects;

public class TimeRemainingStats {
    private final DoubleStats meanTimeOverPartitions;
    private final Map<Integer, TimeRemaining> partitionTimesRemaining;

    public TimeRemainingStats(DoubleStats meanTimeOverPartitions, Map<Integer, TimeRemaining> partitionTimesRemaining) {
        this.meanTimeOverPartitions = meanTimeOverPartitions;
        this.partitionTimesRemaining = partitionTimesRemaining;
    }

    public DoubleStats getMeanTimeOverPartitions() {
        return meanTimeOverPartitions;
    }

    public Map<Integer, TimeRemaining> getPartitionTimesRemaining() {
        return partitionTimesRemaining;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeRemainingStats that = (TimeRemainingStats) o;
        return Objects.equals(meanTimeOverPartitions, that.meanTimeOverPartitions) &&
            Objects.equals(partitionTimesRemaining, that.partitionTimesRemaining);
    }

    @Override
    public int hashCode() {
        return Objects.hash(meanTimeOverPartitions, partitionTimesRemaining);
    }

    @Override
    public String toString() {
        return "TimeRemainingStats{" +
            "meanTimeOverPartitions=" + meanTimeOverPartitions +
            ", partitionTimesRemaining=" + partitionTimesRemaining +
            '}';
    }
}
