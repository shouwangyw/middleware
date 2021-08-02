package com.yw.middleware.example.controler;

import com.yw.middleware.example.domain.SearchResult;
import com.yw.middleware.example.service.SearchService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author yangwei
 */
@RestController
public class SearchController {
    @Resource
    private SearchService searchService;

    @GetMapping("/search")
    public SearchResult search(@RequestParam(required = true) String keyword,
                               @RequestParam(required = false) String ev,
                               @RequestParam(defaultValue = "1") int page,
                               @RequestParam(defaultValue = "50") int rows) {
        // 将ev转换成List类型
        List<Map> filters = null;
        if (StringUtils.isNotBlank(ev)) {
            String[] strings = ev.split("\\|");
            filters = Stream.of(strings).map(e -> {
                Map<String, String> fs = new HashMap<>(2);
                fs.put("key", e.split("-")[0]);
                fs.put("value", e.split("-")[1]);
                return fs;
            }).collect(Collectors.toList());
        }
        return searchService.search(keyword, filters, page, rows);
    }
}
