package com.yw.zookeeper.example.service;

import com.alibaba.fastjson.JSON;
import com.yw.zookeeper.example.model.DbConfig;
import com.yw.zookeeper.example.model.ServerConfig;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

/**
 * @author yangwei
 */
@Slf4j
public class WorkerService {
    private String configPath;
    private String serversPath;
    private ServerConfig serverConfig;
    private DbConfig dbConfig;
    private ZkClient zkClient;
    private IZkDataListener dataListener;

    public WorkerService(ZkClient zkClient, String configPath, String serversPath, ServerConfig serverConfig, DbConfig dbConfig) {
        this.zkClient = zkClient;
        this.configPath = configPath;
        this.serversPath = serversPath;
        this.serverConfig = serverConfig;
        this.dbConfig = dbConfig;
        this.dataListener = new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                updateDbConfig(JSON.parseObject(new String((byte[]) data), DbConfig.class));
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {

            }
        };
    }

    private void updateDbConfig(DbConfig dbConfig) {
        this.dbConfig = dbConfig;
        System.out.println("Database Config is: " + dbConfig.toString());
        zkClient.delete(getPath());
        register();
    }

    public void start() {
        System.out.println("Worker start ...");
        register();
        zkClient.subscribeDataChanges(configPath, dataListener);
    }

    private void register() {
        try {
            zkClient.createEphemeral(getPath(), JSON.toJSONString(dbConfig).getBytes());
        } catch (ZkNoNodeException e) {
            zkClient.createPersistent(serversPath, true);
            register();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private String getPath() {
        return serversPath.concat("/").concat(serverConfig.getAddress());
    }

    public void stop() {
        System.out.println("Worker stop ...");
        zkClient.unsubscribeDataChanges(configPath, dataListener);
    }
}
