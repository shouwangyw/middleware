package com.yw.zookeeper.example.util;

import java.util.concurrent.TimeUnit;

/**
 * @author yangwei
 */
public class Timer {
    private long startTime;

    public Timer() {
        startTime = System.currentTimeMillis();
    }

    public long elapsedTime() {
        return System.currentTimeMillis() - startTime;
    }

    public static void sleep(int second) {
        try {
            TimeUnit.SECONDS.sleep(second);
        } catch (Exception ignore) {}
    }
    public static void sleepMillis(int millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (Exception ignore) {}
    }
}
