package com.yw.zookeeper.example.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author yangwei
 */
@Data
@Accessors(chain = true)
public class DbConfig {
    private String url;
    private String username;
    private String password;
}
