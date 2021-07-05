package com.yw.zookeeper.example.common;

/**
 * @author yangwei
 */
public interface Constants {
    /**
     * 单机版zk地址
     */
    String ZK_SERVER = "192.168.254.120:2181";
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
    /**
     * 需要多少个 workerServer
     */
    int DEFAULT_WORKER = 5;
    /**
     * 发布订阅根节点
     */
    String PUB_SUB_ROOT = "/pubsub";
    /**
     * 配置节点名称
     */
    String CONFIG_PATH = PUB_SUB_ROOT +  "/config";
    /**
     * 命令节点名称
     */
    String COMMAND_PATH = PUB_SUB_ROOT + "/command";
    /**
     * 服务器列表节点名称
     */
    String SERVERS_PATH = PUB_SUB_ROOT + "/servers";

}