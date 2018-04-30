package org.experimental.recoverability;

public class ExponentialBackOff {

    public static long nextTimeout(int retryCount) {
        return  ((long) Math.pow(2, retryCount) * 100L);
    }
}
