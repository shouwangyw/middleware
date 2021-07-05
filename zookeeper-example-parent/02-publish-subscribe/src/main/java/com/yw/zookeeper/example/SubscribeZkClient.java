package com.yw.zookeeper.example;

import com.yw.zookeeper.example.model.DbConfig;
import com.yw.zookeeper.example.model.ServerConfig;
import com.yw.zookeeper.example.service.ManageService;
import com.yw.zookeeper.example.service.WorkerService;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static com.yw.zookeeper.example.common.Constants.*;

/**
 * @author yangwei
 */
@Slf4j
public class SubscribeZkClient {

    @Test
    public void test01() {
        List<ZkClient> zkClients = new ArrayList<>();
        List<WorkerService> workerServices = new ArrayList<>();
        ManageService manageService = null;
        try {
            DbConfig dbConfig = new DbConfig()
                    .setUrl("jdbc:mysql://192.168.254.128:3306/test")
                    .setUsername("root")
                    .setPassword("123456");

            ZkClient manageClient = new ZkClient(ZK_SERVER,
                    ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, new BytesPushThroughSerializer());
            manageService = new ManageService(manageClient, CONFIG_PATH, SERVERS_PATH, COMMAND_PATH, dbConfig);
            manageService.start();

            for (int i = 0; i < DEFAULT_WORKER; i++) {
                ZkClient zkClient = new ZkClient(ZK_SERVER,
                        ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, new BytesPushThroughSerializer());
                zkClients.add(zkClient);
                ServerConfig serverConfig = new ServerConfig()
                        .setId(i)
                        .setName("Worker_" + i)
                        .setAddress("192.168.254." + i);
                WorkerService workerService = new WorkerService(zkClient,
                        CONFIG_PATH, SERVERS_PATH, serverConfig, dbConfig);
                workerServices.add(workerService);
                workerService.start();
            }
            Scanner in = new Scanner(System.in);
            in.nextLine();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            System.out.println("Shutting down ...");
            for (WorkerService workerService : workerServices) {
                workerService.stop();
            }
            for (ZkClient zkClient : zkClients) {
                zkClient.close();
            }
            if (manageService != null) {
                manageService.stop();
            }
        }
    }
}
