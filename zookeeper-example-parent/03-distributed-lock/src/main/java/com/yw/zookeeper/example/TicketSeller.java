package com.yw.zookeeper.example;

import com.yw.zookeeper.example.lock.DistributedLock;
import com.yw.zookeeper.example.util.Timer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * 火车票销售
 *
 * @author yangwei
 */
@Slf4j
public class TicketSeller {
    private static final int THREAD_COUNT = 50;
    private volatile int ticketNums = 1000;
    /**
     * 不加锁方式卖票
     */
    private void sell() {
        Timer.sleepMillis((int) (Math.random() * 500));
        log.info("============================= 剩余 {} 张票 =============================", --ticketNums);
    }
    /**
     * 加锁方式卖票
     */
    private void sellWithLock() {
        DistributedLock lock = DistributedLock.get();
        lock.lock();
        sell();
        lock.unLock();
    }

    @Test
    public void test01() {
        log.info("售票开始");
        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread(() -> {
                while (ticketNums > 0) {
                    sell();
                }
            }).start();
        }
        Timer.sleep(30);
        log.info("售票结束，余票: {}", ticketNums);
    }

    @Test
    public void test02() {
        log.debug("售票开始");
        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread(() -> {
                while (ticketNums > 0) {
                    sellWithLock();
                }
            }).start();
        }
        Timer.sleep(30);
        log.info("售票结束，余票: {}", ticketNums);
    }
}
