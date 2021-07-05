package com.yw.zookeeper.example;

import com.yw.zookeeper.example.common.Constants;
import com.yw.zookeeper.example.serializer.StringZkSerializer;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

/**
 * @author yangwei
 */
public class ZkClientTest {
    private static final String PATH = "/mylog";

    @Test
    public void test01() {
        // ----------- 创建会话 -----------
        // 创建 zkclient
//        ZkClient zkClient = new ZkClient(Constants.ZK_SERVER);
        ZkClient zkClient = new ZkClient(Constants.ZK_CLUSTER_SERVER);
        // 为 zkclient 指定序列化器
//        zkClient.setZkSerializer(new SerializableSerializer());
        zkClient.setZkSerializer(new StringZkSerializer());

        // ----------- 创建节点 -----------
        // 指定创建持久节点
        CreateMode mode = CreateMode.PERSISTENT;
        // 指定节点数据内容
        String data = "first log";
        // 创建节点
        String nodeName = zkClient.create(PATH, data, mode);
        System.out.println("新创建的节点名称为：" + nodeName);

        // --------- 获取数据内容 ----------
        Object readData = zkClient.readData(PATH);
        System.out.println("节点的数据内容为：" + readData);

        // --------- 注册Watcher ---------
        zkClient.subscribeDataChanges(PATH, new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                System.out.print("节点" + dataPath);
                System.out.println("的数据已经更新为了" + data);
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println(dataPath + "的数据内容被删除");
            }
        });

        // --------- 更新数据内容 ----------
        zkClient.writeData(PATH, "second log");
        String updateData = zkClient.readData(PATH);
        System.out.println("更新过的数据内容为：" + updateData);

        // ----------- 删除节点 -----------
        zkClient.delete(PATH);

        // --------- 判断节点存在性 -----------
        boolean isExists = zkClient.exists(PATH);
        System.out.println(PATH + "节点仍存在吗？" + isExists);
    }
}
