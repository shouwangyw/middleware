package com.yw.middleware.example;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author yangwei
 */
public class MongoTest {
    private MongoClient client;

    @Before
    public void before() {
        client = new MongoClient("192.168.254.128", 27017);
    }

    @Test
    public void testConnect() {
        MongoDatabase database = client.getDatabase("test");
        Assert.assertNotNull(database);
        System.out.println("Connect Successful: " + database.getName());
    }

    @Test
    public void testCreateCollection() {
        MongoDatabase database = client.getDatabase("test");
        database.createCollection("col1");
        database.createCollection("col2");

        MongoCollection<Document> collection = database.getCollection("col");
        Assert.assertNotNull(collection);
        System.out.println("集合创建成功: " + collection.getNamespace());

        MongoIterable<String> colNames = database.listCollectionNames();
        for (String name : colNames) {
            System.out.println("Collection Name: " + name);
        }
    }

    @Test
    public void testInsertDocument() {
        MongoDatabase database = client.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("col1");
        List<Document> documents = Arrays.asList(
                new Document("name", "Zhangsan").append("age", 23).append("sex", "男"),
                new Document("name", "Lisi").append("age", 24).append("sex", "男"),
                new Document("name", "Wangwu").append("age", 35).append("sex", "女")
        );
        collection.insertMany(documents);
        System.out.println("文档插入成功");
    }

    @Test
    public void testQueryDocument() {
        MongoDatabase database = client.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("col1");

        // 方法一
        for (Document document : collection.find()) {
            System.out.println(document.toJson());
        }

        // 方法二
        MongoCursor<Document> cursor = collection.find().iterator();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            System.out.println(document.toJson());
        }
    }

    @Test
    public void testUpdateDocument() {
        MongoDatabase database = client.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("col1");
        collection.updateMany(Filters.eq("age", 23), new Document("$set", new Document("age", 32)));
        for (Document document : collection.find()) {
            System.out.println(document.toJson());
        }
    }

    @Test
    public void testDeleteDocument() {
        MongoDatabase database = client.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("col1");
        // 删除符合条件的第一个文档
        collection.deleteOne(Filters.eq("age", 24));
        collection.deleteOne(Filters.eq("name", "Wangwu"));
        // 删除符合条件的所有文档
        collection.deleteMany(Filters.eq("age", 20));

        for (Document document : collection.find()) {
            System.out.println(document.toJson());
        }
    }
}
