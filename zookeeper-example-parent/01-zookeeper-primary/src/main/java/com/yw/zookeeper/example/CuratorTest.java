package com.yw.zookeeper.example;

import com.yw.zookeeper.example.common.Constants;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * @author yangwei
 */
public class CuratorTest {
    @Test
    public void test01() throws Exception {
        // ----------- 创建会话 -----------
        // 创建重试策略对象：第1秒重试1次，最多重试3次
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        // 创建客户端
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(Constants.ZK_CLUSTER_SERVER)
//                .connectString(Constants.ZK_SERVER)
                .sessionTimeoutMs(Constants.ZK_SESSION_TIMEOUT)
                .connectionTimeoutMs(Constants.ZK_CONNECTION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .namespace("logs")
                .build();
        // 开启客户端
        client.start();
        // 指定要创建和操作的节点，注意：其是相对于 /logs 节点的
        String nodePath = "/host";

        // ----------- 创建节点 -----------
        String nodeName = client.create().forPath(nodePath, "myhost".getBytes());
        System.out.println("新创建的节点名称为：" + nodeName);

        // ---- 获取数据内容并注册Watcher ----
        byte[] data = client.getData().usingWatcher((CuratorWatcher) event ->
                System.out.println(event.getPath() + "数据内容发送变化")).forPath(nodePath);
        System.out.println("节点的数据内容为：" + new String(data));

        // --------- 更新数据内容 ----------
        client.setData().forPath(nodePath, "newhost".getBytes());
        // 获取更新过的数据内容
        byte[] newData = client.getData().forPath(nodePath);
        System.out.println("更新过的数据内容为：" + new String(newData));

        // ----------- 删除节点 -----------
        client.delete().forPath(nodePath);

        // --------- 判断节点存在性 -----------
        Stat stat = client.checkExists().forPath(nodePath);
        boolean isExists = true;
        if (stat == null) {
            isExists = false;
        }
        System.out.println(nodePath + "节点仍存在吗？" + isExists);
    }

    @Test
    public void testRecursiveDelete() throws Exception {
        // ---------------- 创建会话 -----------
        // 创建重试策略对象：第1秒重试1次，最多重试3次
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        // 创建客户端
        CuratorFramework client = CuratorFrameworkFactory
                .builder()
                .connectString(Constants.ZK_CLUSTER_SERVER)
                .sessionTimeoutMs(Constants.ZK_SESSION_TIMEOUT)
                .connectionTimeoutMs(Constants.ZK_CONNECTION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .build();
        // 开启客户端
        client.start();

        String nodePath = "/dubbo";

        // ---------------- 删除节点 -----------
        // 删除指定节点，并且会递归删除其所有子孙节点
        client.delete().deletingChildrenIfNeeded().forPath(nodePath);
    }
}
