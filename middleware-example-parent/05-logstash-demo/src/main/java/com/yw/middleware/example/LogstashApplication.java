package com.yw.middleware.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author yangwei
 */
@SpringBootApplication
public class LogstashApplication {

    public static void main(String[] args) {
        //入口函数
        SpringApplication.run(LogstashApplication.class,args);
    }
}