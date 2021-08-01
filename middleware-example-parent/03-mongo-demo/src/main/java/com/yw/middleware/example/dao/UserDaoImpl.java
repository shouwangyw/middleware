package com.yw.middleware.example.dao;

import com.mongodb.client.result.UpdateResult;
import com.yw.middleware.example.po.User;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author yangwei
 */
@Repository
public class UserDaoImpl implements UserDao {
    @Resource
    private MongoTemplate template;

    @Override
    public void save(User user) {
        template.save(user);
    }

    @Override
    public List<User> find(Query query) {
        return template.find(query, User.class);
    }

    @Override
    public User findOne(Query query) {
        return template.findOne(query, User.class);
    }

    @Override
    public UpdateResult update(Query query, Update update) {
        return template.updateMulti(query, update, User.class);
    }

    @Override
    public List<User> findAll(String collectionName) {
        return template.findAll(User.class, collectionName);
    }

    @Override
    public long count(Query query, String collectionName) {
        return template.count(query, collectionName);
    }

    @Override
    public void remove(Query query) {
        template.remove(query, "user");
    }
}