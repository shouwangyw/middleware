package com.yw.middleware.example.controler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yangwei
 */
@Slf4j
@RestController
public class LogController {

    @RequestMapping("/hello")
    public Object sayHello(String name) {
        log.debug("开始执行sayHello方法");
        log.info("hello:" + name);
        log.debug("sayHello方法执行完毕");
        return "OK";
    }
}