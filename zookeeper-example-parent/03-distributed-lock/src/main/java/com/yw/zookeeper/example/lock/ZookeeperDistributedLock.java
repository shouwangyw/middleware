package com.yw.zookeeper.example.lock;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

import static com.yw.zookeeper.example.common.Constants.ZK_SERVER;
import static com.yw.zookeeper.example.common.Constants.ZK_SESSION_TIMEOUT;

/**
 * 基于Zookeeper原生API实现分布式锁
 *
 * @author yangwei
 */
@Slf4j
public class ZookeeperDistributedLock implements DistributedLock {
    private static final String LOCK_ROOT_PATH = "/dis_locks";
    private static final String LOCK_NODE_NAME = "Lock_";
    private ZooKeeper zooKeeper;
    private final ThreadLocal<String> lockPath = new ThreadLocal<>();

    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            log.info("event.path: {}", event.getPath());
            synchronized (this) {
                notifyAll();
            }
        }
    };

    private ZookeeperDistributedLock() {
        try {
            zooKeeper = new ZooKeeper(ZK_SERVER, ZK_SESSION_TIMEOUT, event -> {
                if (event.getState() == Watcher.Event.KeeperState.Disconnected) {
                    log.info("disconnected ...");
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static class Holder {
        private static final ZookeeperDistributedLock INSTANCE = new ZookeeperDistributedLock();
    }

    public static ZookeeperDistributedLock getInstance() {
        return Holder.INSTANCE;
    }

    @Override
    public void lock() {
        try {
            createLockNode();
            tryLock();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 创建锁节点
     */
    private void createLockNode() throws Exception {
        // 若根节点不存在，则创建根节点
        Stat stat = zooKeeper.exists(LOCK_ROOT_PATH, false);
        if (stat == null) {
            zooKeeper.create(LOCK_ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 创建临时顺序节点
        String path = zooKeeper.create(LOCK_ROOT_PATH + "/" + LOCK_NODE_NAME,
                Thread.currentThread().getName().getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        log.debug("{} 创建锁: {}", Thread.currentThread().getName(), path);
        lockPath.set(path);
    }

    /**
     * 尝试获取锁
     */
    private void tryLock() throws Exception {
        // 获取 /dis_locks 节点下所有子节点，按照节点序号排序
        List<String> lockPaths = zooKeeper.getChildren(LOCK_ROOT_PATH, false);
        Collections.sort(lockPaths);

        int index = lockPaths.indexOf(lockPath.get().substring(LOCK_ROOT_PATH.length() + 1));
        // 若 lockPath 是序号最小的节点，则获取锁
        if (index == 0) {
            log.info("{} 获得锁，lockPath: {}", Thread.currentThread().getName(), lockPath.get());
            return;
        } else {
            // 若 lockPath 不是序号最小的节点，则监听其前一个节点
            String preLockPath = lockPaths.get(index - 1);
            Stat stat = zooKeeper.exists(LOCK_ROOT_PATH + "/" + preLockPath, watcher);
            // 若前一个节点不存在了，比如：执行完毕、节点掉线，则重新获取锁
            // 否则，阻塞当前线程，直到 preLockPath 释放锁，被 watcher 观察到 并 notifyAll 后，重新获取锁
            if (stat != null) {
                // 否则，阻塞当前线程，直到 preLockPath 释放锁，被 watcher 观察到 并 notifyAll 后，重新获取锁
                log.debug("{} 等待锁 {} 释放", Thread.currentThread().getName(), preLockPath);
                synchronized (watcher) {
                    watcher.wait();
                }
            }
            tryLock();
        }
    }

    @Override
    public void unLock() {
        try {
            zooKeeper.delete(lockPath.get(), -1);
            log.info("{} 释放锁: {}", Thread.currentThread().getName(), lockPath.get());
            lockPath.remove();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
