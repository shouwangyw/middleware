package com.yw.redis.example;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

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
        try (JedisCluster cluster = new JedisCluster(nodes)){
            // 执行JedisCluster对象中的方法，方法与redis一一对应
            cluster.set("cluster-test", "my jedis cluster test");
            System.out.println(cluster.get("cluster-test"));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
