package com.yw.middleware.example.domain;

import com.yw.middleware.example.dao.entity.Goods;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @author yangwei
 */
@Data
@Accessors(chain = true)
public class SearchResult {
    private List<Goods> goodsList;
    private List<?> aggs;
    private Integer page;
    private Integer totalPage;
}
