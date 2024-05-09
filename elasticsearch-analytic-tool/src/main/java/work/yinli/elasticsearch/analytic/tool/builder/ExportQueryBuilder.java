package work.yinli.elasticsearch.analytic.tool.builder;

import work.yinli.elasticsearch.analytic.tool.builder.group.GroupBuilder;
import work.yinli.elasticsearch.analytic.tool.builder.order.Order;
import work.yinli.elasticsearch.analytic.tool.builder.where.WhereBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExportQueryBuilder<T> implements QueryBuilder<T> {
    List<String> tables = new ArrayList<>();
    List<String> selectFields = new ArrayList<>();

    List<Order> skOrderList = new ArrayList<>();
    /**
     * 一个 where 对应一组 ES filter
     */
    List<WhereBuilder> whereBuilders = new ArrayList<>();
    Integer offset = 0;
    Integer size = 0;
    /**
     * 用于分页查询时，记录上一次查询的最后一个 id
     */
    private String lastId;

    @Override
    public QueryBuilder<T> select(String... selectFields) {
        this.selectFields.addAll(Arrays.asList(selectFields));
        return this;
    }

    @Override
    public QueryBuilder<T> from(List<String> tables) {
        this.tables.addAll(tables);
        return this;
    }

    public QueryBuilder<T> lastId(String lastId) {
        this.lastId = lastId;
        return this;
    }

    @Override
    public QueryBuilder<T> where(WhereBuilder whereBuilder) {
        this.whereBuilders.add(whereBuilder);
        return this;
    }

    @Override
    public QueryBuilder<T> groupBy(String... groupFields) {
        return null;
    }

    @Override
    public QueryBuilder<T> groupBy(GroupBuilder groupBuilder) {
        return null;
    }

    @Override
    public QueryBuilder<T> orderBy(Order skOrder) {
        return null;
    }

    @Override
    public QueryBuilder<T> orderBy(List<Order> skOrders) {
        return null;
    }

    @Override
    public QueryBuilder<T> orderBy(String field) {
        return null;
    }

    @Override
    public QueryBuilder<T> limit(int offset, int size) {
        this.offset = offset;
        this.size = size;
        return this;
    }

    public List<String> getTables() {
        return tables;
    }

    public List<String> getSelectFields() {
        return selectFields;
    }

    public List<Order> getOrderList() {
        return skOrderList;
    }

    public Integer getOffset() {
        return offset;
    }

    public Integer getSize() {
        return size;
    }

    public List<WhereBuilder> getWhereBuilders() {
        return whereBuilders;
    }

    public String getLastId() {
        return lastId;
    }

    public void setLastId(String lastId) {
        this.lastId = lastId;
    }
}
