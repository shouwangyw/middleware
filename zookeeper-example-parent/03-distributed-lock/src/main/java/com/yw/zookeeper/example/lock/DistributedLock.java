package com.yw.zookeeper.example.lock;

/**
 * @author yangwei
 */
public interface DistributedLock {
    /**
     * 获取锁
     */
    void lock();
    /**
     * 释放锁
     */
    void unLock();
    /**
     * 获取分布式锁实现
     */
    static DistributedLock get() {
        try {
            return ZkClientDistributedLock.getInstance();
        } catch (Exception ignore) {
        }
        return ZookeeperDistributedLock.getInstance();
    }
}
