package com.mycompany.myapp.service.lag;

import com.mycompany.myapp.service.lag.DoubleStats;
import com.mycompany.myapp.service.lag.MessageSpeed;

import java.util.List;

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
}
