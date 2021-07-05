package com.yw.mongo.example;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author yangwei
 */
public class MongoQueryTest {
    private MongoCollection<Document> collection;

    @Before
    public void before() {
        MongoClient client = new MongoClient("59.110.227.7", 27017);
        MongoDatabase database = client.getDatabase("test");
        collection = database.getCollection("list1");
        if (collection != null) collection.drop();
        database.createCollection("list1");
        collection = database.getCollection("list1");

        List<Document> documents = Arrays.asList(
                new Document("name", "Zhangsan").append("age", 23).append("sex", "男"),
                new Document("name", "Lisi").append("age", 24).append("sex", "男"),
                new Document("name", "Wangwu").append("age", 35).append("sex", "女")
        );
        collection.insertMany(documents);
    }

    @Test
    public void testQuery01() {
        // 获取第一个记录
        Document first = collection.find().first();
        // 根据key值获得数据
        System.out.println(first.get("name"));

        // 查询指定字段
        Document queryDoc = new Document();
        queryDoc.append("_id", 0); // 不包含 _id
        queryDoc.append("name", 1); // 包含 name

        for (Document document : collection.find().projection(queryDoc)) {
            System.out.println(document.toJson());
        }
    }

    @Test
    public void testQuery02() {
        // 按指定字段排序
        Document queryDoc = new Document();
        queryDoc.append("age", -1); // 按年龄逆序

        for (Document document : collection.find().sort(queryDoc)) {
            System.out.println(document.toJson());
        }
    }

    @Test
    public void testFilters01() {
        // 查询 name 等于 Zhangsan 的记录
        FindIterable<Document> documents = collection.find(Filters.eq("name", "Zhangsan"));
        for (Document document : documents) {
            System.out.println(document.toJson());
        }
    }

    @Test
    public void testFilters02() {
        // 查询 age 大于 3 的记录
        FindIterable<Document> documents = collection.find(Filters.gt("age", 23));
        for (Document document : documents) {
            System.out.println(document.toJson());
        }
    }

    @Test
    public void testFilters03() {
        // 查询 age 小于 33 的记录 或 name = Zhangsan 的记录
        FindIterable<Document> documents = collection.find(Filters.or(
                Filters.eq("name", "Zhangsan"), Filters.lt("age", 33))
        );
        for (Document document : documents) {
            System.out.println(document.toJson());
        }
    }

    @Test
    public void testLike() {
        // 利用正则 模糊匹配 包含 zhang 的
        Pattern pattern = Pattern.compile("^.*zhang.*$", Pattern.CASE_INSENSITIVE);
        BasicDBObject query = new BasicDBObject();
        query.put("name", pattern);

        for (Document document : collection.find(query)) {
            System.out.println(document.toJson());
        }
    }

    @Test
    public void testPage() {
        Pattern pattern = Pattern.compile("^.*zh.*$", Pattern.CASE_INSENSITIVE);
        BasicDBObject query = new BasicDBObject();
        query.put("name", pattern);

        // 排序：按年龄倒序
        BasicDBObject sort = new BasicDBObject();
        sort.put("gae", -1);
        // 获得总条数
        System.out.println("共计: " + collection.count() + " 条记录");
        // 取出第1至3条记录
        FindIterable<Document> documents = collection.find(query).sort(sort).skip(0).limit(3);
        for (Document document : documents) {
            System.out.println(document.toJson());
        }
    }
}
