package work.yinli.elasticsearch.analytic.tool.builder;



import work.yinli.elasticsearch.analytic.tool.builder.group.GroupBuilder;
import work.yinli.elasticsearch.analytic.tool.builder.order.Order;
import work.yinli.elasticsearch.analytic.tool.builder.where.WhereBuilder;

import java.util.List;

public interface QueryBuilder<T> {
    QueryBuilder<T> select(String... selectFields);

    QueryBuilder<T> from(List<String> tables);


    QueryBuilder<T> where(WhereBuilder whereBuilder);


    QueryBuilder<T> groupBy(String... groupFields);

    QueryBuilder<T> groupBy(GroupBuilder groupBuilder);

    QueryBuilder<T> orderBy(Order order);

    QueryBuilder<T> orderBy(String field);

    QueryBuilder<T> limit(int offset, int size);

}
