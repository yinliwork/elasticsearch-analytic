package work.yinli.elasticsearch.analytic.tool.builder;


import work.yinli.elasticsearch.analytic.tool.builder.where.WhereBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/11 15:19
 **/
public class CommonUpdateBuilder<T> implements UpdateBuilder<T> {
    List<String> tables = new ArrayList<>();

    Map<String, Object> updateFields = new HashMap<>();

    /**
     * 一个 where 对应一组 ES filter
     */
    List<WhereBuilder> whereBuilders = new ArrayList<>();

    @Override
    public UpdateBuilder<T> update(Map<String, Object> multiFields) {
        this.updateFields.putAll(multiFields);
        return this;
    }

    @Override
    public UpdateBuilder<T> update(String field, Object value) {
        this.updateFields.put(field, value);
        return this;
    }

    @Override
    public UpdateBuilder<T> from(List<String> tables) {
        this.tables.addAll(tables);
        return this;
    }

    @Override
    public UpdateBuilder<T> where(WhereBuilder whereBuilder) {
        this.whereBuilders.add(whereBuilder);
        return this;
    }

    public List<String> getTables() {
        return tables;
    }

    public List<WhereBuilder> getWhereBuilders() {
        return whereBuilders;
    }

    public Map<String, Object> getUpdateFields() {
        return updateFields;
    }
}
