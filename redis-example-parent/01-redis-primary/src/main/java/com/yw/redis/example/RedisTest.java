package com.yw.redis.example;

import com.yw.redis.example.util.Timer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Set;

/**
 * 测试 Redis 连通性
 *
 * @author yangwei
 */
@Slf4j
public class RedisTest {

    @Test
    public void testJedis() {
        try (Jedis jedis = new Jedis("192.168.254.128", 6379)) {
            System.out.println(jedis.ping());
        }
    }

    @Test
    public void testJedisPool() {
        try (JedisPool jedisPool = new JedisPool("192.168.254.128", 6379);
             Jedis jedis = jedisPool.getResource()) {
            jedis.set("mytest", "hello world, this is jedis client");
            System.out.println(jedis.get("mytest"));
        }
    }

    @Test
    public void testJedisCluster() {
        // 创建一个连接，JedisCluster对象在系统中是单例存在
        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort("192.168.254.128", 7001));
        nodes.add(new HostAndPort("192.168.254.128", 7002));
        nodes.add(new HostAndPort("192.168.254.128", 7003));
        nodes.add(new HostAndPort("192.168.254.128", 7004));
        nodes.add(new HostAndPort("192.168.254.128", 7005));
        nodes.add(new HostAndPort("192.168.254.128", 7006));
        try (JedisCluster cluster = new JedisCluster(nodes)) {
            // 执行JedisCluster对象中的方法，方法与redis一一对应
            cluster.set("cluster-test", "my jedis cluster test");
            System.out.println(cluster.get("cluster-test"));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private ApplicationContext context;

    @Before
    public void before() {
        context = new ClassPathXmlApplicationContext("classpath:application-context.xml");
    }

    @Test
    public void testSpringRedis() {
        JedisCluster cluster = (JedisCluster) context.getBean("jedisCluster");
        cluster.set("name", "zhangsan");
        System.out.println(cluster.get("name"));
    }

    private static final int BATCH_SIZE = 100;

    @Test
    public void testPipeline() {
        Jedis jedis = new Jedis("192.168.254.128", 6379);
        Timer t1 = new Timer();
        for (int i = 0; i < BATCH_SIZE; i++) {
            jedis.set("key" + i, "val" + i);
            jedis.del("key" + i);
        }
        System.out.printf("批量执行 %d 条，耗时：%d ms \n", BATCH_SIZE, t1.elapsedTime());
        // 批量执行 100 条，耗时：55 ms
        // 批量执行 1000 条，耗时：629 ms
        // 批量执行 10000 条，耗时：6045 ms

        Pipeline pipe = jedis.pipelined();
        Timer t2 = new Timer();
        for (int i = 0; i < BATCH_SIZE; i++) {
            pipe.set("key" + i, "val" + i);
            pipe.del("key" + i);
        }
        pipe.sync();
        System.out.printf("批量执行 %d 条，耗时：%d ms \n", BATCH_SIZE, t2.elapsedTime());
        // 批量执行 100 条，耗时：6 ms
        // 批量执行 1000 条，耗时：12 ms
        // 批量执行 10000 条，耗时：40 ms
    }
}
