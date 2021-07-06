package com.yw.zookeeper.example.lock;

import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.zookeeper.Watcher;

import java.util.Collections;
import java.util.List;

import static com.yw.zookeeper.example.common.Constants.ZK_SERVER;
import static com.yw.zookeeper.example.common.Constants.ZK_SESSION_TIMEOUT;

/**
 * 基于ZkClient API实现分布式锁
 *
 * @author yangwei
 */
@Slf4j
public class ZkClientDistributedLock implements DistributedLock {
    private static final String LOCK_ROOT_PATH = "/dis_locks";
    private static final String LOCK_NODE_NAME = "Lock_";
    private ZkClient zkClient;
    private final ThreadLocal<String> lockPath = new ThreadLocal<>();

    private IZkDataListener dataListener = new IZkDataListener() {
        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            log.debug("data changed");
        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception {
            // preLockPath被删除了，即前锁被释放了
            synchronized (this) {
                zkClient.unsubscribeAll();
                notifyAll();
            }
        }
    };

    private ZkClientDistributedLock() {
        try {
            zkClient = new ZkClient(ZK_SERVER, ZK_SESSION_TIMEOUT);
            zkClient.setZkSerializer(new BytesPushThroughSerializer());
            zkClient.subscribeStateChanges(new IZkStateListener() {
                @Override
                public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                    if (state == Watcher.Event.KeeperState.Disconnected) {
                        log.warn("disconnected");
                    }
                }

                @Override
                public void handleNewSession() throws Exception {
                }

                @Override
                public void handleSessionEstablishmentError(Throwable error) throws Exception {
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static class Holder {
        private static final ZkClientDistributedLock INSTANCE = new ZkClientDistributedLock();
    }

    public static ZkClientDistributedLock getInstance() {
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
        boolean exists = zkClient.exists(LOCK_ROOT_PATH);
        if (!exists) {
            zkClient.createPersistent(LOCK_ROOT_PATH, new byte[0]);
        }
        // 创建临时顺序节点
        String path = zkClient.createEphemeralSequential(LOCK_ROOT_PATH + "/" + LOCK_NODE_NAME,
                Thread.currentThread().getName().getBytes());
        log.debug("{} 创建锁: {}", Thread.currentThread().getName(), path);
        lockPath.set(path);
    }

    /**
     * 尝试获取锁
     */
    private void tryLock() throws Exception {
        // 获取 /dis_locks 节点下所有子节点，按照节点序号排序
        List<String> lockPaths = zkClient.getChildren(LOCK_ROOT_PATH);
        Collections.sort(lockPaths);

        if (lockPath.get() == null) {
            return;
        }
        int index = lockPaths.indexOf(lockPath.get().substring(LOCK_ROOT_PATH.length() + 1));
        // 若 lockPath 是序号最小的节点，则获取锁
        if (index == 0) {
            log.info("{} 获得锁，lockPath: {}", Thread.currentThread().getName(), lockPath.get());
            return;
        } else {
            // 若 lockPath 不是序号最小的节点，则监听其前一个节点
            String preLockPath = lockPaths.get(index - 1);
            // 若前一个节点不存在了，比如：执行完毕、节点掉线，则重新获取锁
            // 否则，阻塞当前线程，直到 preLockPath 释放锁，被 watcher 观察到 并 notifyAll 后，重新获取锁
            boolean exists = zkClient.exists(LOCK_ROOT_PATH + "/" + preLockPath);
            if (!exists) {
                tryLock();
            } else {
                log.debug("{} 等待锁 {} 释放", Thread.currentThread().getName(), preLockPath);
                zkClient.subscribeDataChanges(LOCK_ROOT_PATH + "/" + preLockPath, dataListener);
                synchronized (dataListener) {
                    dataListener.wait();
                }
                tryLock();
            }
        }
    }

    @Override
    public void unLock() {
        try {
            zkClient.delete(lockPath.get(), -1);
            log.info("{} 释放锁: {}", Thread.currentThread().getName(), lockPath.get());
            lockPath.remove();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
