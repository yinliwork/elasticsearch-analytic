package work.yinli.elasticsearch.analytic.service.core;

import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.util.CollectionUtils;
import work.yinli.elasticsearch.analytic.service.core.dto.AggregationResult;
import work.yinli.elasticsearch.analytic.service.core.dto.YinliSearchRequest;
import work.yinli.elasticsearch.analytic.service.core.dto.YinliSearchResponse;
import work.yinli.elasticsearch.analytic.tool.builder.CommonQueryBuilder;
import work.yinli.elasticsearch.analytic.tool.builder.CommonUpdateBuilder;
import work.yinli.elasticsearch.analytic.tool.builder.ExportQueryBuilder;
import work.yinli.elasticsearch.analytic.tool.builder.YinliRange;
import work.yinli.elasticsearch.analytic.tool.builder.group.GroupBuilder;
import work.yinli.elasticsearch.analytic.tool.builder.group.GroupClause;
import work.yinli.elasticsearch.analytic.tool.builder.group.GroupOperation;
import work.yinli.elasticsearch.analytic.tool.builder.order.YinliOrder;
import work.yinli.elasticsearch.analytic.tool.builder.where.WhereBuilder;
import work.yinli.elasticsearch.analytic.tool.builder.where.WhereClause;
import work.yinli.elasticsearch.analytic.tool.builder.where.WhereOperation;

import java.util.*;
import java.util.stream.Collectors;

import static work.yinli.elasticsearch.analytic.tool.builder.group.GroupOperation.MAX;
import static work.yinli.elasticsearch.analytic.tool.builder.group.GroupOperation.TERMS;


/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/8 17:53
 **/
@Slf4j
public class DefaultSearch<T> extends AbstractSearch<T> {
    public static final Integer EXPORT_SIZE = 50; // 导出的默认大小
    public static String[] SEARCH_EXCLUDES = new String[]{""}; // 默认排除字段

    private static void parseAggregationResponse(Aggregations aggregations, Map<String, String> aggsContext, Map<String, Object> aggsResult) {
        // 解析聚合结果
        for (Map.Entry<String, String> aggsCtx : aggsContext.entrySet()) {
            String opt = aggsCtx.getValue();
            String field = aggsCtx.getKey();
            if (opt.equals(GroupOperation.RANGE)) {
                ParsedRange range = aggregations.get(field);
                List<? extends Range.Bucket> buckets = range.getBuckets();
                List<AggregationResult> rangeList = new ArrayList<>();
                for (Range.Bucket bucket : buckets) {
                    String key = bucket.getKeyAsString();
                    long docCount = bucket.getDocCount();
                    AggregationResult aggregationResult = new AggregationResult(key, docCount);
                    rangeList.add(aggregationResult);
                    if (bucket.getAggregations() != null) {
                        Map<String, Object> subAggs = new HashMap<>();
                        parseAggregationResponse(bucket.getAggregations(), aggsContext, subAggs);
                        aggregationResult.setSubAggregations(subAggs);
                    }
                }
                aggsResult.put(field, rangeList);
            } else if (opt.equals(TERMS) && aggregations.get(field) != null) {
                ParsedStringTerms terms = aggregations.get(field);
                List<? extends Terms.Bucket> buckets = terms.getBuckets();
                List<AggregationResult> termsList = new ArrayList<>();
                for (Terms.Bucket bucket : buckets) {
                    String key = bucket.getKeyAsString();
                    long docCount = bucket.getDocCount();
                    AggregationResult aggregationResult = new AggregationResult(key, docCount);
                    termsList.add(aggregationResult);
                    if (bucket.getAggregations() != null) {
                        Map<String, Object> subAggs = new HashMap<>();
                        parseAggregationResponse(bucket.getAggregations(), aggsContext, subAggs);
                        aggregationResult.setSubAggregations(subAggs);
                    }
                }
                aggsResult.put(field, termsList);

            } else if (opt.equals(GroupOperation.AVG) && aggregations.get(field) != null) {
                ParsedAvg parsedAvg = aggregations.get(field);
                double value = parsedAvg.getValue();
                if (Double.isInfinite(value)) { // 查询语句错误导致无穷大，设置 0
                    value = 0;
                }
                aggsResult.put(field, value);

            } else if (opt.equals(GroupOperation.MIN) && aggregations.get(field) != null) {
                ParsedMin min = aggregations.get(field);
                Object value = min.getValue();
                String minValueAsString = min.getValueAsString();
                if (StringUtils.isNotEmpty(minValueAsString)) {
                    value = minValueAsString;
                }
                aggsResult.put(field, value);
            } else if (opt.equals(MAX) && aggregations.get(field) != null) {
                ParsedMax max = aggregations.get(field);
                Object value = max.getValue();
                String maxValueAsString = max.getValueAsString();
                if (StringUtils.isNotEmpty(maxValueAsString)) {
                    value = maxValueAsString;
                }
                aggsResult.put(field, value);
            } else if (opt.equals(GroupOperation.DISTINCT) && aggregations.get(field) != null) {
                Cardinality cardinality = aggregations.get(field);
                long docCount = cardinality.getValue();
                aggsResult.put(field, docCount);
            } else if (opt.equals(GroupOperation.NESTED) && aggregations.get(field) != null) {
                ParsedNested nested = aggregations.get(field);
                if (nested.getAggregations() != null) {
                    parseAggregationResponse(nested.getAggregations(), aggsContext, aggsResult);
                }
            } else if (opt.equals(GroupOperation.FILTER) && aggregations.get(field) != null) {
                Filters filters = aggregations.get(field);
                List<? extends Filters.Bucket> buckets = filters.getBuckets();
                List<AggregationResult> filterList = new ArrayList<>();
                for (Filters.Bucket bucket : buckets) {
                    String key = bucket.getKeyAsString();
                    long docCount = bucket.getDocCount();
                    AggregationResult aggregationResult = new AggregationResult(key, docCount);
                    filterList.add(aggregationResult);
                    if (bucket.getAggregations() != null) {
                        Map<String, Object> subAggs = new HashMap<>();
                        parseAggregationResponse(bucket.getAggregations(), aggsContext, subAggs);
                        aggregationResult.setSubAggregations(subAggs);
                    }
                }
                aggsResult.put(field, filterList);
            }
        }


    }

    @Override
    protected YinliSearchRequest preSearch(Object builder) {
        if (builder instanceof CommonQueryBuilder) {
            log.info("QueryBuilder is instance of CommonQueryBuilder");
            return commonQueryHandler(builder);
        } else if (builder instanceof ExportQueryBuilder) {
            log.info("QueryBuilder is instance of ExportQueryBuilder");
            return exportQueryHandler(builder);
        } else if (builder instanceof CommonUpdateBuilder) {
            log.info("QueryBuilder is instance of CommonUpdateBuilder");
            return commonUpdateHandler(builder);

        }
        return null;
    }

    private YinliSearchRequest exportQueryHandler(Object builder) {
        YinliSearchRequest yinliSearchRequest = new YinliSearchRequest();
        ExportQueryBuilder exportQueryBuilder = (ExportQueryBuilder) builder;
        List<String> tables = exportQueryBuilder.getTables();
        String lastId = exportQueryBuilder.getLastId();
//        if (lastId == null) {
//            throw new IllegalArgumentException("lastId is null");
//        }
//        log.info("ExportQuery: {}", JSON.toJSONString(exportQueryBuilder));
        List<String> selectFields = exportQueryBuilder.getSelectFields();
        int size = EXPORT_SIZE;
        List<WhereBuilder> whereBuilders = exportQueryBuilder.getWhereBuilders();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery(); // 封装布尔查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); // 封装基础查询条件
        SearchRequest searchRequest = new SearchRequest(); // 封装查询请求
        // 设置查询条件
        searchSourceBuilder.size(size);
        if (selectFields != null && !selectFields.isEmpty()) {// 设置返回字段
            searchSourceBuilder.fetchSource(selectFields.toArray(new String[0]), SEARCH_EXCLUDES);
        } else {
            searchSourceBuilder.fetchSource(null, SEARCH_EXCLUDES);
        }
        searchSourceBuilder.sort("_id", SortOrder.DESC);
        if (lastId != null) {
            searchSourceBuilder.searchAfter(new Object[]{lastId}); // 为防止重复，可以使用多字段排序，这里放的就是最后一条排序的字段数组；这里id能保证索引内唯一
        }
        // 解析 where 条件
        if (whereBuilders != null && !whereBuilders.isEmpty()) {
            for (WhereBuilder whereBuilder : whereBuilders) {
                // 一个 where 对应一个 ES filter，布尔检索没有用到算分的场景，直接使用 filter，ES 能缓存结果
                List<QueryBuilder> filter = QueryBuilders.boolQuery().filter();
                WhereParse(filter, whereBuilder);
                if (!CollectionUtils.isEmpty(filter)) {
                    boolQueryBuilder.filter().addAll(filter);
                }
            }
        }
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.trackTotalHits(true);
        // 设置查询请求
        searchRequest.source(searchSourceBuilder);
        // 设置索引
        searchRequest.indices(tables.toArray(new String[0]));
        yinliSearchRequest.setSearchRequest(searchRequest);
        return yinliSearchRequest;
    }

    private YinliSearchRequest commonUpdateHandler(Object updateBuilder) {
        YinliSearchRequest yinliSearchRequest = new YinliSearchRequest();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery(); // 封装布尔查询条件
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(); // 封装查询请求
        CommonUpdateBuilder<T> commonUpdateBuilder = (CommonUpdateBuilder<T>) updateBuilder;
        Map<String, Object> updateFields = commonUpdateBuilder.getUpdateFields();
        List<String> tables = commonUpdateBuilder.getTables();
        List<WhereBuilder> whereBuilders = commonUpdateBuilder.getWhereBuilders();
        // 解析 where 条件
        if (whereBuilders != null && !whereBuilders.isEmpty()) {
            for (WhereBuilder whereBuilder : whereBuilders) {
                // 一个 where 对应一个 ES filter，布尔检索没有用到算分的场景，直接使用 filter，ES 能缓存结果
                List<QueryBuilder> filter = QueryBuilders.boolQuery().filter();
                WhereParse(filter, whereBuilder);
                if (!CollectionUtils.isEmpty(filter)) {
                    boolQueryBuilder.filter().addAll(filter);
                }
            }
        }
        // 设置索引
        updateByQueryRequest.indices(tables.toArray(new String[0]));
        updateByQueryRequest.setQuery(boolQueryBuilder);
        Set<Map.Entry<String, Object>> entries = updateFields.entrySet();
        StringBuilder script = new StringBuilder();
        for (Map.Entry<String, Object> entry : entries) {
            script.append(String.format("ctx._source.%s= params.%s", entry.getKey(), entry.getKey()));
        }
        updateByQueryRequest.setScript(new Script(ScriptType.INLINE, "painless", script.toString(), updateFields));
        yinliSearchRequest.setUpdateRequest(updateByQueryRequest);
        return yinliSearchRequest;
    }

    public YinliSearchRequest commonQueryHandler(Object queryBuilder) {
        YinliSearchRequest yinliSearchRequest = new YinliSearchRequest();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery(); // 封装布尔查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); // 封装基础查询条件
        SearchRequest searchRequest = new SearchRequest(); // 封装查询请求
        CommonQueryBuilder<T> commonQueryBuilder = (CommonQueryBuilder<T>) queryBuilder;
        int size = commonQueryBuilder.getSize();
        int offset = commonQueryBuilder.getOffset();
        List<String> tables = commonQueryBuilder.getTables();
        List<String> selectFields = commonQueryBuilder.getSelectFields();
        List<YinliOrder> yinliOrderList = commonQueryBuilder.getOrderList();
        List<WhereBuilder> whereBuilders = commonQueryBuilder.getWhereBuilders();
        GroupBuilder groupBuilder = commonQueryBuilder.getGroupBuilder();

        DefaultContext.addVariable("size", size);
        DefaultContext.addVariable("offset", offset);
        // 设置查询条件
        searchSourceBuilder.from(offset);
        searchSourceBuilder.size(size);
        if (selectFields != null && !selectFields.isEmpty()) {// 设置返回字段
            searchSourceBuilder.fetchSource(selectFields.toArray(new String[0]), SEARCH_EXCLUDES);
        } else {
            searchSourceBuilder.fetchSource(null, SEARCH_EXCLUDES);
        }
        // 解析 where 条件
        if (whereBuilders != null && !whereBuilders.isEmpty()) {
            for (WhereBuilder whereBuilder : whereBuilders) {
                // 一个 where 对应一个 ES filter，布尔检索没有用到算分的场景，直接使用 filter，ES 能缓存结果
                List<QueryBuilder> filter = QueryBuilders.boolQuery().filter();
                WhereParse(filter, whereBuilder);
                if (!CollectionUtils.isEmpty(filter)) {
                    boolQueryBuilder.filter().addAll(filter);
                }
            }
            // 设置整体查询条件
            searchSourceBuilder.query(boolQueryBuilder);
        }
        // 设置排序
        if (yinliOrderList != null && !yinliOrderList.isEmpty()) {
            for (YinliOrder yinliOrder : yinliOrderList) {
                searchSourceBuilder.sort(yinliOrder.getField(), SortOrder.fromString(yinliOrder.getOrder()));
            }
        }
        // 设置分组
        if (groupBuilder != null) {
            AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
            groupParser(builder, groupBuilder);
            builder.getAggregatorFactories().forEach(searchSourceBuilder::aggregation);
        }
        searchSourceBuilder.trackTotalHits(true);
        // 设置查询请求
        searchRequest.source(searchSourceBuilder);
        // 设置索引
        searchRequest.indices(tables.toArray(new String[0]));
        yinliSearchRequest.setSearchRequest(searchRequest);
        return yinliSearchRequest;

    }

    @Override
    protected YinliSearchResponse postSearch(Object result) {
        if (result instanceof BulkByScrollResponse) {
            log.info("result is instance of BulkByScrollResponse");
            YinliSearchResponse yinliSearchResponse = new YinliSearchResponse();
            BulkByScrollResponse bulkByScrollResponse = (BulkByScrollResponse) result;
            List<BulkItemResponse.Failure> bulkFailures = bulkByScrollResponse.getBulkFailures();
            if (!CollectionUtils.isEmpty(bulkFailures)) {
                log.error("update bulkFailures:{}", bulkFailures);
            }
            yinliSearchResponse.setTotalCount(bulkByScrollResponse.getUpdated());
            return yinliSearchResponse;
        }
        if (!(result instanceof SearchResponse)) {
            log.error("result is not instance of SearchResponse");
            return null;
        }

        YinliSearchResponse yinliSearchResponse = new YinliSearchResponse();
        SearchResponse searchResponse = (SearchResponse) result;
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits().value;
        List<Map<String, Object>> resultList = new ArrayList<>();
        // 处理查询结果
        SearchHit[] datas = hits.getHits();

        for (SearchHit data : datas) {
            Map<String, Object> sourceAsMap = data.getSourceAsMap();
            resultList.add(sourceAsMap);
        }
        // 处理聚合结果
        Aggregations aggregations = searchResponse.getAggregations(); // 获取顶层对象
        Map<String, Object> aggs = new HashMap<>();
        if (aggregations != null) {
            Map<String, String> aggsContext = (Map<String, String>) DefaultContext.getVariable("aggs");
            if (!aggsContext.isEmpty()) {
                parseAggregationResponse(aggregations, aggsContext, aggs);
            }
        }
        if (datas.length > 0) {
            String lastId = datas[datas.length - 1].getId();
            yinliSearchResponse.setLastId(lastId);
        } else {
            log.warn("data list is empty, resp:{}", result);
        }
        int size = EXPORT_SIZE;
        int offset = 0;
        Object sizeObj = DefaultContext.getVariable("size");
        Object offsetObj = DefaultContext.getVariable("offset");
        if (!Objects.isNull(sizeObj)) {
            size = (int) sizeObj;
        }
        if (!Objects.isNull(offsetObj)) {
            offset = (int) offsetObj;
        }
        yinliSearchResponse.setEnd(resultList.size() < size);

        yinliSearchResponse.setOffset(offset);
        yinliSearchResponse.setSize(size);
        yinliSearchResponse.setTotalCount(totalHits);
        yinliSearchResponse.setResultList(resultList);
        yinliSearchResponse.setAggregations(aggs);
        return yinliSearchResponse;
    }


    /**
     * 递归解析 where 条件
     *
     * @param filters      ES 查询条件
     * @param whereBuilder 数咖查询条件
     */
    private void WhereParse(List<QueryBuilder> filters, WhereBuilder whereBuilder) {
        List<WhereBuilder> orList = whereBuilder.getOrList();
        List<WhereBuilder> andList = whereBuilder.getAndList();
        List<WhereBuilder> notList = whereBuilder.getNotList();
        List<Map<String, WhereBuilder>> nestList = whereBuilder.getNestList();
        List<WhereClause> whereClauses = whereBuilder.getWhereClauses();
        List<Map<String, List<YinliRange>>> rangeList = whereBuilder.getRangeList();

        if (CollectionUtils.isEmpty(whereClauses) && CollectionUtils.isEmpty(orList)
                && CollectionUtils.isEmpty(andList) && CollectionUtils.isEmpty(notList)
                && CollectionUtils.isEmpty(nestList) && CollectionUtils.isEmpty(rangeList)) {
            return;
        }
        // 赋值
        if (!CollectionUtils.isEmpty(whereClauses)) {
            for (WhereClause whereClause : whereClauses) {
                String field = whereClause.getField();
                Object val = whereClause.getVal();
                String whereOperation = whereClause.getOperation();
                switch (whereOperation) {
                    case WhereOperation.EQ:
                        filters.add(QueryBuilders.termQuery(field, val));
                        break;
                    case WhereOperation.GT:
                        filters.add(QueryBuilders.rangeQuery(field).gt(val));
                        break;
                    case WhereOperation.LT:
                        filters.add(QueryBuilders.rangeQuery(field).lt(val));
                        break;
                    case WhereOperation.GTE:
                        filters.add(QueryBuilders.rangeQuery(field).gte(val));
                        break;
                    case WhereOperation.LTE:
                        filters.add(QueryBuilders.rangeQuery(field).lte(val));
                        break;
                    case WhereOperation.LIKE:
                        filters.add(QueryBuilders.matchPhraseQuery(field, val));
                        break;
                    case WhereOperation.IN:
                        if (val instanceof List) {
                            // list to object[]
                            Object[] vals = ((List<?>) val).toArray(new Object[0]);
                            filters.add(QueryBuilders.termsQuery(field, vals));
                        } else if (val instanceof Object[]) {
                            filters.add(QueryBuilders.termsQuery(field, val));
                        } else {
                            throw new IllegalArgumentException("IN operation must have a list value");
                        }
                        break;

                    default:
                        break;
                }
            }
        }
        // range 查询
        if (!CollectionUtils.isEmpty(rangeList)) {
            for (Map<String, List<YinliRange>> rangeMap : rangeList) {
                for (Map.Entry<String, List<YinliRange>> entry : rangeMap.entrySet()) {
                    String field = entry.getKey();
                    List<YinliRange> ranges = entry.getValue();
                    if (!CollectionUtils.isEmpty(ranges)) {
                        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field);
                        for (YinliRange range : ranges) {
                            rangeQueryBuilder.from(range.getFrom()).to(range.getTo());
                        }
                        filters.add(rangeQueryBuilder);
                    }
                }
            }
        }

        // nest 查询
        if (!CollectionUtils.isEmpty(nestList)) {
            for (Map<String, WhereBuilder> nestMap : nestList) {
                for (Map.Entry<String, WhereBuilder> entry : nestMap.entrySet()) {
                    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                    WhereParse(boolQueryBuilder.must(), entry.getValue());
                    filters.add(QueryBuilders.nestedQuery(entry.getKey(), boolQueryBuilder, ScoreMode.None));
                }
            }
        }
        // and 条件
        if (!CollectionUtils.isEmpty(andList)) {
            for (WhereBuilder andBuilder : andList) {
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                WhereParse(boolQueryBuilder.must(), andBuilder);
                filters.add(boolQueryBuilder);
            }
        }
        // or 条件
        if (!CollectionUtils.isEmpty(orList)) {
            for (WhereBuilder orBuilder : orList) {
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                WhereParse(boolQueryBuilder.should(), orBuilder);
                filters.add(boolQueryBuilder);
            }
        }
        // not 条件
        if (!CollectionUtils.isEmpty(notList)) {
            for (WhereBuilder notBuilder : notList) {
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                WhereParse(boolQueryBuilder.mustNot(), notBuilder);
                filters.add(boolQueryBuilder);
            }
        }

    }

    // 解析分组
    public void groupParser(AggregatorFactories.Builder aggBuilder, GroupBuilder groupBuilder) {
        // 设置递归结束条件
        if (groupBuilder == null) {
            return;
        }
        List<GroupClause> groupClauses = groupBuilder.getGroupClauses();
        List<Map<String, GroupBuilder>> nestAggsBuilders = groupBuilder.getNestAggsBuilders();
        List<Map<String, List<YinliRange>>> rangeAggsBuilders = groupBuilder.getRangeAggsBuilders();
        Map<String, List<GroupBuilder>> subGroupMap = groupBuilder.getSubGroupMap();
        Map<String, WhereBuilder> filters = groupBuilder.getFilters();
        if (!CollectionUtils.isEmpty(filters)) {
            Map<String, String> map = (Map<String, String>) DefaultContext.getVariable("aggs");
            List<FiltersAggregator.KeyedFilter> keyedFilters = new ArrayList<>(filters.size());
            for (Map.Entry<String, WhereBuilder> entry : filters.entrySet()) {
                String field = entry.getKey();// filter 的名称
                WhereBuilder whereBuilder = entry.getValue(); // filter 的条件
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                WhereParse(boolQueryBuilder.filter(), whereBuilder);
                keyedFilters.add(new FiltersAggregator.KeyedFilter(field, boolQueryBuilder));
                if (map != null) {
                    map.put(field, GroupOperation.FILTER);
                }
            }
            String key = keyedFilters.get(keyedFilters.size() - 1).key() + "_filter";
            aggBuilder.addAggregator(AggregationBuilders.filters(key, keyedFilters.toArray(new FiltersAggregator.KeyedFilter[0])));
            if (map != null) {
                map.put(key, GroupOperation.FILTER);
            }
        }

        // 解析 bucket 分组
        if (!CollectionUtils.isEmpty(groupClauses)) {
            for (GroupClause groupClause : groupClauses) {
                String field = groupClause.getField();
                String operation = groupClause.getOperation();
                int size = groupClause.getSize();
                YinliOrder order = groupClause.getOrder();
                if (operation.equals(TERMS)) {
                    if (size > 0) {
                        if (order != null) {
                            aggBuilder.addAggregator(AggregationBuilders.terms(field).field(field).size(size).order(BucketOrder.aggregation(order.getField(), order.getOrder().equals("asc"))));
                        } else {
                            aggBuilder.addAggregator(AggregationBuilders.terms(field).field(field).size(size));
                        }
                    } else {
                        if (order != null) {
                            aggBuilder.addAggregator(AggregationBuilders.terms(field).field(field).order(BucketOrder.aggregation(order.getField(), order.getOrder().equals("asc"))));
                        } else {
                            aggBuilder.addAggregator(AggregationBuilders.terms(field).field(field));
                        }
                    }
                    Map<String, String> map = (Map<String, String>) DefaultContext.getVariable("aggs");
                    if (map != null) {
                        map.put(field, TERMS);
                    }
                } else if (operation.equals(GroupOperation.AVG)) {
                    aggBuilder.addAggregator(AggregationBuilders.avg(field).field(field));
                    Map<String, String> map = (Map<String, String>) DefaultContext.getVariable("aggs");
                    if (map != null) {
                        map.put(field, GroupOperation.AVG);
                    }
                } else if (operation.equals(MAX)) {
                    aggBuilder.addAggregator(AggregationBuilders.max(field + "Max").field(field));
                    Map<String, String> map = (Map<String, String>) DefaultContext.getVariable("aggs");
                    if (map != null) {
                        map.put(field + "Max", MAX);
                    }
                } else if (operation.equals(GroupOperation.MIN)) {
                    aggBuilder.addAggregator(AggregationBuilders.min(field + "Min").field(field));
                    Map<String, String> map = (Map<String, String>) DefaultContext.getVariable("aggs");
                    if (map != null) {
                        map.put(field + "Min", GroupOperation.MIN);
                    }
                } else if (operation.equals(GroupOperation.DISTINCT)) {
                    aggBuilder.addAggregator(AggregationBuilders.cardinality(field).field(field));
                    Map<String, String> map = (Map<String, String>) DefaultContext.getVariable("aggs");
                    if (map != null) {
                        map.put(field, GroupOperation.DISTINCT);
                    }
                }
            }
        }
        // 解析范围分组
        if (!CollectionUtils.isEmpty(rangeAggsBuilders)) {
            for (Map<String, List<YinliRange>> rangeAggsBuilder : rangeAggsBuilders) {
                for (Map.Entry<String, List<YinliRange>> entry : rangeAggsBuilder.entrySet()) {
                    String field = entry.getKey();
                    List<YinliRange> ranges = entry.getValue();
                    if (!CollectionUtils.isEmpty(ranges)) {
                        RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders.range(field).field(field);
                        for (YinliRange range : ranges) {
                            if (range.getFrom() != null && range.getTo() != null) {
                                rangeAggregationBuilder.addRange(Double.parseDouble(String.valueOf(range.getFrom())), Double.parseDouble(String.valueOf(range.getTo())));
                            }
                        }
                        aggBuilder.addAggregator(rangeAggregationBuilder);
                        Map<String, String> map = (Map<String, String>) DefaultContext.getVariable("aggs");
                        if (map != null) {
                            map.put(field, GroupOperation.RANGE);
                        }
                    }
                }
            }
        }
        // 解析嵌套分组
        if (!CollectionUtils.isEmpty(nestAggsBuilders)) {
            // nest 同级可能会存在多个
            for (Map<String, GroupBuilder> nestAggsBuilder : nestAggsBuilders) {
                // 同级下只能有一个 path
                for (Map.Entry<String, GroupBuilder> entry : nestAggsBuilder.entrySet()) {
                    String path = entry.getKey();// path
                    GroupBuilder subAggregations = entry.getValue(); // 子分组,可以是 terms, range, nest
                    AggregatorFactories.Builder nestBuilder = new AggregatorFactories.Builder();
                    groupParser(nestBuilder, subAggregations);
                    NestedAggregationBuilder nestedAggregationBuilder = AggregationBuilders.nested(path, path);
                    nestBuilder.getAggregatorFactories().forEach(nestedAggregationBuilder::subAggregation);
                    aggBuilder.addAggregator(nestedAggregationBuilder);
                    Map<String, String> map = (Map<String, String>) DefaultContext.getVariable("aggs");
                    if (map != null) {
                        map.put(path, GroupOperation.NESTED);
                    }
                }
            }
        }
        // 解析子分组，根据 map 获取到父子级对应关系
        if (!CollectionUtils.isEmpty(subGroupMap)) {
            for (Map.Entry<String, List<GroupBuilder>> entry : subGroupMap.entrySet()) {
                String field = entry.getKey();
                List<GroupBuilder> subGroup = entry.getValue();
                AggregatorFactories.Builder subBuilder = new AggregatorFactories.Builder();
                for (GroupBuilder builder : subGroup) {
                    groupParser(subBuilder, builder);
                    List<AggregationBuilder> preAggs = aggBuilder.getAggregatorFactories()
                            .stream()
                            .filter(aggregationBuilder -> aggregationBuilder.getName().equals(field) || aggregationBuilder.getName().equals(field + "_filter")).collect(Collectors.toList());
                    if (CollectionUtils.isEmpty(preAggs)) {
                        throw new IllegalArgumentException("sub clause must have a parent clause!");
                    }
                    AggregationBuilder preAggregationBuilder = preAggs.get(0);
                    subBuilder.getAggregatorFactories().forEach(preAggregationBuilder::subAggregation);
                }
            }
        }

    }


}
