package com.yw.mongo.example.po;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author yangwei
 */
@Data
@Accessors(chain = true)
public class User {
    private String name;
    private int age;
    private String sex;
}
