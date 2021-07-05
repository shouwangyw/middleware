package com.yw.mysql.example.config;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.InlineShardingStrategyConfiguration;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author yangwei
 */
@Slf4j
public class DataShardingConfig {
    private volatile DataSource dataSource;

    public DataSource getDataSource() {
        if (dataSource == null) {
            synchronized (DataShardingConfig.class) {
                if (dataSource == null) {
                    try {
                        dataSource = create();
                    } catch (SQLException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
        return dataSource;
    }

    private static DataSource create() throws SQLException {
        // 配置真实数据源
        Map<String, DataSource> dataSourceMap = new HashMap<>(2);

        // 配置第一个数据源
        DruidDataSource dataSource1 = new DruidDataSource();
        dataSource1.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource1.setUrl("jdbc:mysql://192.168.254.129:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8");
        dataSource1.setUsername("root");
        dataSource1.setPassword("123456");
        dataSourceMap.put("ds0", dataSource1);

        // 配置第二个数据源
        DruidDataSource dataSource2 = new DruidDataSource();
        dataSource2.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource2.setUrl("jdbc:mysql://192.168.254.130:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8");
        dataSource2.setUsername("root");
        dataSource2.setPassword("123456");
        dataSourceMap.put("ds1", dataSource2);

        // 配置 t_order 表规则
        TableRuleConfiguration orderRuleConfig = new TableRuleConfiguration("t_order", "ds${0..1}.t_order${0..1}");

        // 配置数据库表分片策略
        orderRuleConfig.setDatabaseShardingStrategyConfig(new InlineShardingStrategyConfiguration("user_id", "ds${user_id % 2}"));
        orderRuleConfig.setTableShardingStrategyConfig(new InlineShardingStrategyConfiguration("order_id", "t_order${order_id % 2}"));

        // 配置分片规则
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        shardingRuleConfig.getTableRuleConfigs().add(orderRuleConfig);

        // 创建数据源
        return ShardingDataSourceFactory.createDataSource(dataSourceMap, shardingRuleConfig, new Properties());
    }
}
