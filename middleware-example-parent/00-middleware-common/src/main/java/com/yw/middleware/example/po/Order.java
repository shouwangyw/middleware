package com.yw.middleware.example.po;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author yangwei
 */
@Data
@Accessors(chain = true)
public class Order {
    private int orderId;
    private int userId;
    private String info;
}
