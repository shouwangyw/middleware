package com.yw.middleware.example.service;

import com.yw.middleware.example.config.MasterSlaveConfig;
import com.yw.middleware.example.po.User;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yangwei
 */
public class UserService {

    private static final MasterSlaveConfig CONFIG = new MasterSlaveConfig();
    private DataSource dataSource;

    public UserService() {
        dataSource = CONFIG.getDataSource();
    }

    public boolean addUser(User user) throws Exception {
        String sql = "insert into t_user0(name, age, address) values (?, ?, ?)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, user.getName());
            ps.setInt(2, user.getAge());
            ps.setString(3, user.getAddress());
            return ps.execute();
        }
    }

    public List<User> getUserList() throws Exception {
        String sql = "select id, name, age, concat(address, @@hostname) as address from t_user0";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<User> users = new ArrayList<>();
            while (rs.next()) {
                User user = new User()
                        .setId(rs.getInt("id"))
                        .setName(rs.getString("name"))
                        .setAge(rs.getInt("age"))
                        .setAddress(rs.getString("address"));

                users.add(user);
            }
            return users;
        }
    }
}