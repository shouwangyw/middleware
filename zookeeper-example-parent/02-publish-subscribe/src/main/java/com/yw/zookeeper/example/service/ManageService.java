package com.yw.zookeeper.example.service;

import com.alibaba.fastjson.JSON;
import com.yw.zookeeper.example.model.DbConfig;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.util.List;

/**
 * 配置管理服务
 * @author yangwei
 */
public class ManageService {
    private ZkClient zkClient;
    private String configPath;
    private String serversPath;
    private String commandPath;
    private DbConfig dbConfig;
    private IZkChildListener childListener;
    private IZkDataListener dataListener;
    private List<String> workerList;

    private static final String COMMAND_LIST = "list";
    private static final String COMMAND_CREATE = "create";
    private static final String COMMAND_UPDATE = "update";

    public ManageService(ZkClient zkClient, String configPath, String serversPath, String commandPath, DbConfig dbConfig) {
        this.zkClient = zkClient;
        this.configPath = configPath;
        this.serversPath = serversPath;
        this.commandPath = commandPath;
        this.dbConfig = dbConfig;
        this.childListener = (parentPath, currentChilds) -> {
            workerList = currentChilds;
            System.out.println("Worker list changed, newWorker list is: " + workerList.toString());
        };
        this.dataListener = new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                executeCmd(new String((byte[]) data));
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {

            }
        };
    }
    private void executeCmd(String cmdType) {
        if (COMMAND_LIST.equalsIgnoreCase(cmdType)) {
            System.out.println(workerList.toString());
        } else if (COMMAND_CREATE.equalsIgnoreCase(cmdType)) {
            executeCreate();
        } else if (COMMAND_UPDATE.equalsIgnoreCase(cmdType)) {
            executeUpdate();
        } else {
            System.out.println("Invalid Command: " + cmdType);
        }
    }

    private void executeCreate() {
        if (zkClient.exists(configPath)) {
            return;
        }
        try {
            zkClient.createPersistent(configPath, JSON.toJSONString(dbConfig).getBytes());
        } catch (ZkNodeExistsException e) {
            zkClient.writeData(configPath, JSON.toJSONString(dbConfig).getBytes());
        } catch (ZkNoNodeException e) {
            String parentPath = configPath.substring(0, configPath.lastIndexOf("/"));
            zkClient.createPersistent(parentPath, true);
            executeCreate();
        }
    }

    private void executeUpdate() {
        dbConfig.setUsername(dbConfig.getUsername() + "_update");
        try {
            zkClient.writeData(configPath, JSON.toJSONString(dbConfig).getBytes());
        } catch (ZkNoNodeException e) {
            executeCreate();
        }
    }

    public void start() {
        if (!zkClient.exists(commandPath)) {
            zkClient.createPersistent(commandPath, true);
        }
        zkClient.subscribeDataChanges(commandPath, dataListener);
        zkClient.subscribeChildChanges(serversPath, childListener);
    }

    public void stop() {
        zkClient.unsubscribeChildChanges(serversPath, childListener);
        zkClient.unsubscribeDataChanges(commandPath, dataListener);
    }

}
