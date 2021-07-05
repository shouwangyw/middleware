package com.yw.zookeeper.example.common;

/**
 * @author yangwei
 */
public interface Constants {
    /**
     * 单机版zk地址
     */
    String ZK_SERVER = "129.168.254.120:2181";
    /**
     * 集群IP:端口信息
     */
    String ZK_CLUSTER_SERVER = "192.168.254.128:2181,192.168.254.130:2181,192.168.254.132:2181";
    /**
     * ZkClient会话超时时间
     */
    int ZK_SESSION_TIMEOUT = 15000;
    /**
     * ZkClient连接超时时间
     */
    int ZK_CONNECTION_TIMEOUT = 13000;
}