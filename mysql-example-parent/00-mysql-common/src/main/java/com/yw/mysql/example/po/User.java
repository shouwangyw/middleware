package com.yw.mysql.example.po;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author yangwei
 */
@Data
@Accessors(chain = true)
public class User {
    private int id;
    private String name;
    private int age;
    private String address;
}
