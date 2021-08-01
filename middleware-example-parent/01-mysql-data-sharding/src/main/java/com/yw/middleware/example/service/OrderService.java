package com.yw.middleware.example.service;

import com.yw.middleware.example.config.DataShardingConfig;
import com.yw.middleware.example.po.Order;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author yangwei
 */
public class OrderService {

    private static final DataShardingConfig CONFIG = new DataShardingConfig();
    private DataSource dataSource;

    public OrderService() {
        dataSource = CONFIG.getDataSource();
    }

    public boolean addOrderInfo(Order order) throws Exception {
        String sql = "insert into t_order(order_id, user_id, info) values (?,?,?)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, order.getOrderId());
            ps.setInt(2, order.getUserId());
            ps.setString(3, order.getInfo());

            return ps.execute();
        }
    }
}