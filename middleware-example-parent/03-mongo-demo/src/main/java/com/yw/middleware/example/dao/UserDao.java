package com.yw.middleware.example.dao;

import com.mongodb.client.result.UpdateResult;
import com.yw.middleware.example.po.User;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;

/**
 * @author yangwei
 */
public interface UserDao {
    /**
     * 新增
     */
    void save(User user);

    /**
     * 根据条件查询
     */
    List<User> find(Query query);

    /**
     * 根据条件查询一个
     */
    User findOne(Query query);

    /**
     * 根据条件更新
     */
    UpdateResult update(Query query, Update update);

    /**
     * 获取所有该类型记录，并指定集合名
     */
    List<User> findAll(String collectionName);

    /**
     * 根据条件查询总数
     */
    long count(Query query, String collectionName);

    /**
     * 根据条件删除
     */
    void remove(Query query);
}