package com.yw.middleware.example;

import com.yw.middleware.example.po.Order;
import com.yw.middleware.example.service.OrderService;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yangwei
 */
public class DataShardingTest {
    private OrderService orderService;

    @Before
    public void before() {
        orderService = new OrderService();
    }

    @Test
    public void test01() throws Exception {
        int userId = 10;
        for (int i = 1; i <= 20; i++) {
            if (i >= 10) {
                userId = 21;
            }
            Order order = new Order()
                    .setOrderId(i)
                    .setUserId(userId)
                    .setInfo("订单信息：user_id=" + userId + ",order_id=" + i);

            boolean result = orderService.addOrderInfo(order);
            if (result) {
                System.out.println("订单" + i + "添加成功");
            }
        }
    }
}