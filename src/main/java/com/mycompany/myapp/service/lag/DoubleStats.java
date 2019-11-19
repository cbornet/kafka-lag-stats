package com.mycompany.myapp.service.lag;

import java.util.Objects;

public class DoubleStats {
    private final double mean;
    private final double stddev;
    private final double stddevPercent;

    public DoubleStats(double mean, double stddev) {
        this.mean = mean;
        this.stddev = stddev;
        this.stddevPercent = mean/stddev*100;
    }

    public double getMean() {
        return mean;
    }

    public double getStddev() {
        return stddev;
    }

    public double getStddevPercent() {
        return stddevPercent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DoubleStats that = (DoubleStats) o;
        return Double.compare(that.mean, mean) == 0 &&
                Double.compare(that.stddev, stddev) == 0 &&
                Double.compare(that.stddevPercent, stddevPercent) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mean, stddev, stddevPercent);
    }

    @Override
    public String toString() {
        return "DoubleStats{" +
                "mean=" + mean +
                ", stddev=" + stddev +
                ", stddevPercent=" + stddevPercent +
                '}';
    }
}
