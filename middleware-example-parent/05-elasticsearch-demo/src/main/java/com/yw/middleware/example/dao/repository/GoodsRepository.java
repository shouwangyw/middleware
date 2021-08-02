package com.yw.middleware.example.dao.repository;

import com.yw.middleware.example.dao.entity.Goods;
import org.springframework.data.elasticsearch.repository.ElasticsearchCrudRepository;

/**
 * @author yangwei
 */
public interface GoodsRepository extends ElasticsearchCrudRepository<Goods, Long> {

}
