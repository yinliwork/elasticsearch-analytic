package work.yinli.elasticsearch.analytic.service;

import org.junit.jupiter.api.Test;
import work.yinli.elasticsearch.analytic.service.core.DefaultSearch;
import work.yinli.elasticsearch.analytic.service.core.dto.QueryResult;
import work.yinli.elasticsearch.analytic.service.core.dto.YinliSearchResponse;
import work.yinli.elasticsearch.analytic.tool.builder.QueryBuilder;
import work.yinli.elasticsearch.analytic.tool.builder.QueryBuilders;
import work.yinli.elasticsearch.analytic.tool.builder.group.GroupBuilder;
import work.yinli.elasticsearch.analytic.tool.builder.where.WhereBuilder;

import java.util.Arrays;

public class CommonQueryTest {
    @Test
    void test_multi_sub_aggregation() {
        GroupBuilder groupBuilder = new GroupBuilder();
        groupBuilder.addFilter("filter", new WhereBuilder().eq("field", "value"))
                .sub(new GroupBuilder().addField("field1")
                        .sub(new GroupBuilder().addField("field2"))
                        .sub(new GroupBuilder().addField("field3"))
                );
                
                
        DefaultSearch<Object> search = new DefaultSearch<>();

        QueryBuilder<Object> main = new QueryBuilders<>()
                .common()
                .from(Arrays.asList(""))
                .groupBy(groupBuilder).limit(0, 0);
        QueryResult<YinliSearchResponse> result = search.search(main);
        String dsl = result.getData().getDsl();
        System.out.println(dsl);

    }
}