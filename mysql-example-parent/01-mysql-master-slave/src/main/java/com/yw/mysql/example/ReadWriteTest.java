package com.yw.mysql.example;

import com.yw.mysql.example.po.User;
import com.yw.mysql.example.service.UserService;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author yangwei
 */
public class ReadWriteTest {
    private UserService userService;

    @Before
    public void before() {
        userService = new UserService();
    }

    @Test
    public void test01() throws Exception {
        User user = new User()
                .setName("武松")
                .setAge(23)
                .setAddress("清河县");

        boolean result = userService.addUser(user);
        if (result) {
            System.out.println("添加用户成功");
        }

        List<User> users = userService.getUserList();
        users.forEach(System.out::println);

        users = userService.getUserList();
        users.forEach(System.out::println);
    }
}
