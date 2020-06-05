package com.hdb.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

public class ESUtil {
    private static final String ES_URL = "127.0.0.1";
    private static final int ES_PORT = 9200;
    private static final String ES_NAME = "hdb_share_building_details_index";

    public static void main(String[] args) {
        Map map = new HashMap();
        map.put("wx_open_id", "01");
        map.put("wx_union_id", "01");

        List<String> list = multiSearch(map, 10);

        System.out.println(list.size());

        for (String value: list) {
            System.out.println(value);
        }
    }

    /**
     * 多条件查询
     * @param mustMap
     * @param length
     * @return
     */
    public static List<String> multiSearch(Map<String,Object> mustMap, int length) {
        // 根据多个条件 生成 boolQueryBuilder
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        // 循环添加多个条件
        for (Map.Entry<String, Object> entry : mustMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders
                    .matchQuery(entry.getKey(), entry.getValue()));
        }

        // boolQueryBuilder生效
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.size(length);

        // 其中listSearchResult是自己编写的方法，以供多中查询方式使用。
        return listSearchResult(searchSourceBuilder);
    }

    /**
     * 用来处理搜索结果，转换成链表
     * @param builder
     * @return
     */
    public static List<String> listSearchResult(SearchSourceBuilder builder) {
        // 提交查询
        SearchRequest searchRequest = new SearchRequest(ES_NAME);
        searchRequest.source(builder);
        RestHighLevelClient client = getRestHighLevelClient();

        // 获得response
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // 从response中获得结果
        List<String> list = new LinkedList();
        SearchHits hits = searchResponse.getHits();
        Iterator<SearchHit> iterator = hits.iterator();
        while(iterator.hasNext()) {
            SearchHit next = iterator.next();
            list.add(next.getSourceAsString());
        }
        return list;
    }

    /**
     * getRestHighLevelClient
     * @return
     */
    public static RestHighLevelClient getRestHighLevelClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(ES_URL, ES_PORT, "http")));
        return client;
    }

    /**
     * 删除es的整个数据库
     * @return
     * @throws IOException
     */
    public static boolean delete() throws IOException {
        RestHighLevelClient client = getRestHighLevelClient();
        DeleteIndexRequest request =
                new DeleteIndexRequest(ES_NAME);
        client.indices().delete(request, RequestOptions.DEFAULT);
        return true;
    }

    /**
     * 后文段模糊查找方法，可以理解为 like value?
     * @param key
     * @param prefix
     * @param size
     * @return
     */
    public static List<String> fuzzy(String key, String prefix,int size) {
        PrefixQueryBuilder builder = QueryBuilders.prefixQuery(key, prefix);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(size);
        searchSourceBuilder.query(builder);
        return listSearchResult(searchSourceBuilder);
    }
}
