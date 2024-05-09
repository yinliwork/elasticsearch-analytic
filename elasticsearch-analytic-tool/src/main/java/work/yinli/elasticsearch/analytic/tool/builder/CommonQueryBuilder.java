package work.yinli.elasticsearch.analytic.tool.builder;

import work.yinli.elasticsearch.analytic.tool.builder.group.GroupBuilder;
import work.yinli.elasticsearch.analytic.tool.builder.order.Order;
import work.yinli.elasticsearch.analytic.tool.builder.where.WhereBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/11 15:19
 **/
public class CommonQueryBuilder<T> implements QueryBuilder<T> {
    List<String> tables = new ArrayList<>();
    List<String> selectFields = new ArrayList<>();

    List<Order> skOrderList = new ArrayList<>();

    /**
     * 一个 where 对应一组 ES filter
     */
    List<WhereBuilder> whereBuilders = new ArrayList<>();

    GroupBuilder groupBuilder = new GroupBuilder();

    Integer offset = 0;

    Integer size = 10;

    public List<String> getTables() {
        return tables;
    }

    public List<String> getSelectFields() {
        return selectFields;
    }

    public List<Order> getOrderList() {
        return skOrderList;
    }

    public List<WhereBuilder> getWhereBuilders() {
        return whereBuilders;
    }

    public GroupBuilder getGroupBuilder() {
        return groupBuilder;
    }

    public void setGroupBuilder(GroupBuilder groupBuilder) {
        this.groupBuilder = groupBuilder;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

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

    @Override
    public QueryBuilder<T> where(WhereBuilder whereBuilder) {
        this.whereBuilders.add(whereBuilder);
        return this;
    }

    public QueryBuilder<T> where() {
        this.whereBuilders.add(new WhereBuilder());
        return this;
    }

    @Override
    public QueryBuilder<T> groupBy(String... groupFields) {
        if (groupFields.length > 3) {
            throw new IllegalArgumentException("groupFields length must be less than 3");
        }
        if (groupFields.length == 1) {
            groupBuilder.addField(groupFields[0]);
        }
        if (groupFields.length == 2) {
            groupBuilder.addField(groupFields[0])
                    .sub(new GroupBuilder().addField(groupFields[1]));
        }
        if (groupFields.length == 3) {
            groupBuilder.addField(groupFields[0])
                    .sub(new GroupBuilder().addField(groupFields[1])
                            .sub(new GroupBuilder().addField(groupFields[2])));
        }
        return this;
    }

    @Override
    public QueryBuilder<T> groupBy(GroupBuilder groupBuilder) {
        this.groupBuilder = groupBuilder;
        return this;
    }

    @Override
    public QueryBuilder<T> orderBy(Order skOrder) {
        skOrderList.add(skOrder);
        return this;
    }

    @Override
    public QueryBuilder<T> orderBy(List<Order> skOrders) {
        skOrderList.addAll(skOrders);
        return this;
    }

    @Override
    public QueryBuilder<T> orderBy(String field) {
        skOrderList.add(new Order(field, "desc"));
        return this;
    }

    @Override
    public QueryBuilder<T> limit(int offset, int size) {
        this.offset = offset;
        this.size = size;

        return this;
    }
}
