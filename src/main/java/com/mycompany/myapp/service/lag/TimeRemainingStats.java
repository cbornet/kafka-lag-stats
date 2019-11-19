package com.mycompany.myapp.service.lag;

import java.util.Map;

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

}
