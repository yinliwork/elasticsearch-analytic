package work.yinli.elasticsearch.analytic.tool.builder;


import work.yinli.elasticsearch.analytic.tool.builder.where.WhereBuilder;

import java.util.List;
import java.util.Map;

public interface UpdateBuilder<T> {

   UpdateBuilder<T> update(Map<String, Object> fields);

   UpdateBuilder<T> update(String field, Object value);

   UpdateBuilder<T> from(List<String> tables);

   UpdateBuilder<T> where(WhereBuilder whereBuilder);
}
