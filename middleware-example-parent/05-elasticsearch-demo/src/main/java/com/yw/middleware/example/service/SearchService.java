package com.yw.middleware.example.service;

import com.yw.middleware.example.dao.entity.Goods;
import com.yw.middleware.example.domain.SearchResult;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yangwei
 */
@Service
public class SearchService {
    @Resource
    private ElasticsearchTemplate elasticsearchTemplate;

    public SearchResult search(String queryString, List<Map> filters, int page, int rows) {
        //创建一个查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.queryStringQuery(queryString).defaultField("goods_name"))
                //设置分页条件
                .withPageable(PageRequest.of(page, rows))
                //设置聚合条件
                .addAggregation(AggregationBuilders.terms("brand_aggs").field("brand_name"))
                .addAggregation(AggregationBuilders.terms("category_aggs").field("cname3"))
                //设置高亮显示
                .withHighlightFields(new HighlightBuilder.Field("goods_name").preTags("<em>").postTags("</em>"));
        //设置过滤条件
        if (filters != null && !filters.isEmpty()) {
            for (Map filer : filters) {
                queryBuilder.withFilter(QueryBuilders.termQuery((String) filer.get("key"), filer.get("value")));
            }
        }
        //创建查询对象
        NativeSearchQuery query = queryBuilder.build();
        //-----------------
        //执行查询
        AggregatedPage<Goods> aggregatedPage = elasticsearchTemplate.queryForPage(query, Goods.class, new SearchResultMapper() {
            @Override
            public <T> AggregatedPage<T> mapResults(SearchResponse searchResponse, Class<T> clazz, Pageable pageable) {
                List<Goods> goodsList = new ArrayList<>();
                SearchHits searchHits = searchResponse.getHits();
                searchHits.forEach(hits -> {
                    Goods goodsEntity = new Goods();
                    goodsEntity.setId((Long) hits.getSourceAsMap().get("id"))
                            .setGoodsName((String) hits.getSourceAsMap().get("goods_name"))
                            .setSellerId((String) hits.getSourceAsMap().get("seller_id"))
                            .setNickName((String) hits.getSourceAsMap().get("nick_name"))
                            .setBrandId((Integer) hits.getSourceAsMap().get("brand_id"))
                            .setBrandName((String) hits.getSourceAsMap().get("brand_name"))
                            .setCategory1Id((Integer) hits.getSourceAsMap().get("category1_id"))
                            .setCname1((String) hits.getSourceAsMap().get("cname1"))
                            .setCategory2Id((Integer) hits.getSourceAsMap().get("category2_id"))
                            .setCname2((String) hits.getSourceAsMap().get("cname2"))
                            .setCategory3Id((Integer) hits.getSourceAsMap().get("category3_id"))
                            .setCname3((String) hits.getSourceAsMap().get("cname3"))
                            .setSmallPic((String) hits.getSourceAsMap().get("small_pic"))
                            .setPrice((Double) hits.getSourceAsMap().get("price"));
                    //取高亮结果
                    HighlightField highlightField = hits.getHighlightFields().get("goods_name");
                    if (highlightField != null) {
                        String hl = highlightField.getFragments()[0].string();
                        goodsEntity.setGoodsName(hl);
                    }
                    goodsList.add(goodsEntity);
                });
                return new AggregatedPageImpl<>((List<T>) goodsList, pageable, searchHits.totalHits, searchResponse.getAggregations());
            }
        });
        //取商品列表
        List<Goods> content = aggregatedPage.getContent();
        //取聚合结果
        Terms termBrand = (Terms) aggregatedPage.getAggregation("brand_aggs");
        List<String> brandAggsList = termBrand.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getKeyAsString).collect(Collectors.toList());
        Map<String, Object> brandMap = new HashMap<>(4);
        brandMap.put("name", "品牌");
        brandMap.put("content", brandAggsList);
        brandMap.put("filed", "brand_name");
        Terms termCategory = (Terms) aggregatedPage.getAggregation("category_aggs");
        List<String> categoryAggsList = termCategory.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getKeyAsString).collect(Collectors.toList());
        Map<String, Object> catMap = new HashMap<>(4);
        catMap.put("name", "分类");
        catMap.put("content", categoryAggsList);
        catMap.put("filed", "cname3");
        List<Map> aggsList = new ArrayList<>();
        aggsList.add(brandMap);
        aggsList.add(catMap);
        //取总记录数
        int totalPages = aggregatedPage.getTotalPages();
        //封装成一个SearchResult对象
        SearchResult searchResult = new SearchResult();
        searchResult.setGoodsList(content);
        searchResult.setAggs(aggsList);
        searchResult.setPage(page);
        searchResult.setTotalPage(totalPages);
        //返回结果
        return searchResult;
    }
}
