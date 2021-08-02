package com.yw.middleware.example.dao.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * @author yangwei
 */
@Data
@Accessors(chain = true)
@Document(indexName = "supergo", type = "goods")
public class Goods {
    @Id
    @Field(type = FieldType.Long, store = true)
    private long id;
    @JsonProperty(value = "goods_name")
    @Field(type = FieldType.Text, store = true, analyzer = "ik_max_word")
    private String goodsName;
    @JsonProperty(value = "seller_id")
    @Field(type = FieldType.Keyword, store = true)
    private String sellerId;
    @JsonProperty(value = "nick_name")
    @Field(type = FieldType.Keyword, store = true)
    private String nickName;
    @JsonProperty(value = "brand_id")
    @Field(type = FieldType.Long, store = true)
    private long brandId;
    @JsonProperty(value = "brand_name")
    @Field(type = FieldType.Keyword, store = true)
    private String brandName;
    @JsonProperty(value = "goods_name")
    @Field(type = FieldType.Long, store = true)
    private long category1Id;
    @JsonProperty(value = "goods_name")
    @Field(type = FieldType.Keyword, store = true)
    private String cname1;
    @JsonProperty(value = "category2_id")
    @Field(type = FieldType.Long, store = true)
    private long category2Id;
    @Field(type = FieldType.Keyword, store = true)
    private String cname2;
    @JsonProperty(value = "category3_id")
    @Field(type = FieldType.Long, store = true)
    private long category3Id;
    @Field(type = FieldType.Keyword, store = true)
    private String cname3;
    @JsonProperty(value = "small_pic")
    @Field(type = FieldType.Keyword, store = true, index = false)
    private String smallPic;
    @Field(type = FieldType.Float, store = true)
    private double price;
}
