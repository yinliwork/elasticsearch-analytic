package work.yinli.elasticsearch.analytic.service.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import work.yinli.elasticsearch.analytic.service.core.dto.QueryResult;
import work.yinli.elasticsearch.analytic.service.core.dto.YinliSearchRequest;
import work.yinli.elasticsearch.analytic.service.core.dto.YinliSearchResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/8 17:32
 **/
@Slf4j
public abstract class AbstractSearch<T> {


    public final QueryResult<YinliSearchResponse> search(Object queryBuilder) {
        QueryResult<YinliSearchResponse> queryResult = new QueryResult<>();
        queryResult.setSuccess(false);
        Map<Integer, List<Map<String, String>>> agg = new HashMap<>();
        agg.put(0, new ArrayList<>());
        agg.put(1, new ArrayList<>());
        agg.put(2, new ArrayList<>());
        Map<String, String> map = new HashMap<>();
        DefaultContext.addVariable("aggs", map);
        YinliSearchRequest yinliSearchRequest = preSearch(queryBuilder); // 搜索前
        log.info("request:{}", yinliSearchRequest.toString());
        Object result; // 搜索
        try {
            result = execute(yinliSearchRequest);
            YinliSearchResponse yinliSearchResponse = postSearch(result); // 搜索后
            yinliSearchResponse.setDsl(String.valueOf(DefaultContext.getVariable("dsl")));
            DefaultContext.clearAll(); // 清除当前线程上下文
            queryResult.setSuccess(true);
            queryResult.setData(yinliSearchResponse);
            queryResult.setErrorMsg("");
        } catch (Exception e) {
            log.error("search error, request:{}", yinliSearchRequest, e);
            queryResult.setSuccess(false);
            YinliSearchResponse yinliSearchResponse = new YinliSearchResponse();
            yinliSearchResponse.setDsl(String.valueOf(DefaultContext.getVariable("dsl")));
            queryResult.setData(yinliSearchResponse);
            return queryResult;
        }
        return queryResult;
    }


    public final QueryResult<YinliSearchResponse> update(Object updateBuilder) {
        QueryResult<YinliSearchResponse> queryResult = new QueryResult<>();
        queryResult.setSuccess(false);
        YinliSearchRequest yinliSearchRequest = preSearch(updateBuilder); // 搜索前
        Object result; // 搜索
        try {
            result = execute(yinliSearchRequest);
        } catch (Exception e) {
            log.error("search error, request:{}", yinliSearchRequest, e);
            return queryResult;
        }
        YinliSearchResponse yinliSearchResponse = postSearch(result); // 搜索后
        yinliSearchResponse.setDsl(String.valueOf(DefaultContext.getVariable("dsl")));
        DefaultContext.clearAll(); // 清除当前线程上下文
        queryResult.setSuccess(true);
        queryResult.setData(yinliSearchResponse);
        queryResult.setErrorMsg("");
        return queryResult;
    }


    protected abstract YinliSearchRequest preSearch(Object builder);

    protected abstract YinliSearchResponse postSearch(Object result);

    private Object execute(YinliSearchRequest yinliSearchRequest) throws Exception {
        HttpHost http = new HttpHost("localhost", 9200, "http");
        RestClientBuilder restClientBuilder = RestClient.builder(http);
        restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> {
            BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
//            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("es-cluster-dev", "YJI7nLHhrtmquakO"));
            return httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
        });
        RestHighLevelClient restHighClient = new RestHighLevelClient(restClientBuilder);
        if (restHighClient == null) {
        }
        UpdateByQueryRequest updateRequest = yinliSearchRequest.getUpdateRequest();
        if (updateRequest != null) {
            SearchSourceBuilder source = updateRequest.getSearchRequest().source();
            DefaultContext.addVariable("dsl", source);
            return restHighClient.updateByQuery(updateRequest, RequestOptions.DEFAULT);
        } else if (yinliSearchRequest.getSearchRequest() != null) {
            SearchRequest searchRequest = yinliSearchRequest.getSearchRequest();
            DefaultContext.addVariable("dsl", searchRequest.source());
            return restHighClient.search(searchRequest, RequestOptions.DEFAULT);
        }
        return null;
    }


}
