package org.experimental.recoverability;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BackOff {
    private long step;
    private List<Long> durations = new ArrayList<>();

    public BackOff(long step) {
        this.step = step;
        for (int attempt = 0; attempt < 3; attempt++){
            durations.add(exponentialBackoff(attempt,step));
        }
    }

    public String getDurations() {
        return durations.stream().map(Object::toString).collect(Collectors.joining(","));
    }

    public Long get(int attempt){
        return durations.get(attempt);
    }

    public static long exponentialBackoff(int retryCount, long step) {
        return  ((long) Math.pow(2, retryCount) * step);
    }
}
