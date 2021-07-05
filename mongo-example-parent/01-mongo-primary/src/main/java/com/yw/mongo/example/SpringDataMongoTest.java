package com.yw.mongo.example;

import com.mongodb.client.result.UpdateResult;
import com.yw.mongo.example.dao.UserDao;
import com.yw.mongo.example.po.User;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

/**
 * @author yangwei
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:spring-mongo.xml")
public class SpringDataMongoTest {
    @Resource
    private UserDao userDao;

    @Test
    public void test01() {
        userDao.save(new User("张三", 23, "M"));
        userDao.save(new User("李四", 24, "M"));
        userDao.save(new User("王五", 15, "F"));

        System.out.println("插入成功");
    }

    @Test
    public void test02() {
        Query query = new Query(Criteria.where("name").is("张三"));
        for (User user : userDao.find(query)) {
            System.out.println(user);
        }
        System.out.println("=======================");
        for (User user : userDao.find(new Query())) {
            System.out.println(user);
        }
        System.out.println("=======================");
        System.out.println(userDao.findOne(query));
    }

    @Test
    public void test03() {
        Query query = new Query(Criteria.where("name").is("王五"));
        Update update = new Update().set("sex", "纯爷们");
        UpdateResult result = userDao.update(query, update);
        System.out.println(result.getUpsertedId());
    }

    @Test
    public void test04() {
        for (User user : userDao.findAll("user")) {
            System.out.println(user);
        }
    }

    @Test
    public void test05() {
        long count = userDao.count(new Query(), "user");
        System.out.println("总条数: " + count);
    }


    @Test
    public void test06() {
        Query query = new Query(Criteria.where("name").is("李四"));
        userDao.remove(query);
    }
}
