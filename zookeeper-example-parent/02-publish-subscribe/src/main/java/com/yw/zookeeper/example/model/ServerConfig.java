package com.yw.zookeeper.example.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author yangwei
 */
@Data
@Accessors(chain = true)
public class ServerConfig {
    private int id;
    private String name;
    private String address;
}
