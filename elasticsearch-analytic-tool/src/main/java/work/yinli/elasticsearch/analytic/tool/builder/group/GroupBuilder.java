package work.yinli.elasticsearch.analytic.tool.builder.group;


import work.yinli.elasticsearch.analytic.tool.builder.Range;
import work.yinli.elasticsearch.analytic.tool.builder.order.Order;
import work.yinli.elasticsearch.analytic.tool.builder.where.WhereBuilder;

import java.util.*;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/11 14:39
 **/
public class GroupBuilder {
    // 分组语句
    private final List<GroupClause> groupClauses = new ArrayList<>();
    // 嵌套分组语句, key为外层分组字段 对应 es nest path
    private final List<Map<String, GroupBuilder>> nestAggsBuilders = new ArrayList<>();
    // 范围分组语句
    private final List<Map<String, List<Range>>> rangeAggsBuilders = new ArrayList<>();

    private final Map<String, WhereBuilder> filters = new HashMap<>();

    // 子分组
    private final LinkedList<GroupBuilder> subGroups = new LinkedList<>();

    // 绑定子分组和上级的关系
    private final Map<String, List<GroupBuilder>> subGroupMap = new HashMap<>();

    // 记录上一步是啥操作
    private String preOpt;

    private int level;


    public GroupBuilder addField(String field) {
        groupClauses.add(new GroupClause(field, GroupOperation.TERMS));
        this.preOpt = GroupOperation.TERMS;
        return this;
    }

    public GroupBuilder addField(String field, int size) {
        groupClauses.add(new GroupClause(field, GroupOperation.TERMS, size));
        this.preOpt = GroupOperation.TERMS;
        return this;
    }

    public GroupBuilder addField(String field, int size, Order order) {
        groupClauses.add(new GroupClause(field, GroupOperation.TERMS, size, order));
        this.preOpt = GroupOperation.TERMS;
        return this;
    }

    public GroupBuilder addFilter(String field, WhereBuilder whereBuilder) {
        this.filters.put(field, whereBuilder);
        this.preOpt = GroupOperation.FILTER;
        return this;
    }

    public GroupBuilder addFilters(Map<String, WhereBuilder> filters) {
        this.filters.putAll(filters);
        this.preOpt = GroupOperation.FILTER;
        return this;
    }

    /**
     * 在调用 sub 时要和上一步进行绑定，多级聚合需要使用这个方法
     *
     * @param groupBuilder 子分组
     * @return
     */
    public GroupBuilder sub(GroupBuilder groupBuilder) {
        if (this.preOpt == null) {
            throw new IllegalArgumentException("sub must after other operation!");
        }
        if (level > 2) {
            throw new IllegalArgumentException("sub level must less than 3!");
        }
        switch (this.preOpt) {
            case GroupOperation.NESTED:
                Set<String> nestKeys = this.rangeAggsBuilders.get(this.rangeAggsBuilders.size() - 1).keySet();
                nestKeys.forEach(key -> {
                    this.subGroupMap.put(key, this.subGroups);
                });
                break;
            case GroupOperation.RANGE:
                this.rangeAggsBuilders.get(this.rangeAggsBuilders.size() - 1).forEach((k, v) -> {
                    this.subGroupMap.put(k, this.subGroups);
                });
                break;
            case GroupOperation.FILTER:
                String filterField = this.filters.keySet().iterator().next();
                this.subGroupMap.put(filterField, this.subGroups);
                break;
            default:
                String field = this.groupClauses.get(this.groupClauses.size() - 1).getField();
                this.subGroupMap.put(field, this.subGroups);
                break;
        }
        this.level++;
        if (this.subGroups.isEmpty()) {
            this.subGroups.add(groupBuilder);
            return this;
        }
        this.subGroups.get(subGroups.size() - 1).subGroups.add(groupBuilder);
        return this;
    }

    public GroupBuilder nest(String outerFiled, GroupBuilder groupBuilder) {
        Map<String, GroupBuilder> map = new HashMap<>();
        map.put(outerFiled, groupBuilder);
        nestAggsBuilders.add(map);
        this.preOpt = GroupOperation.NESTED;
        return this;
    }


    public GroupBuilder range(String filed, List<Range> ranges) {
        Map<String, List<Range>> map = new HashMap<>();
        map.put(filed, ranges);
        this.rangeAggsBuilders.add(map);
        this.preOpt = GroupOperation.RANGE;
        return this;
    }


    // -----------------metrics 聚合操作 below-----------------
    // 以下操作只能在最后一步操作, 不能再继续进行子分组操作,因为es不支持
    public GroupBuilder avg(String field) {
        groupClauses.add(new GroupClause(field, GroupOperation.AVG));
        this.preOpt = GroupOperation.AVG;
        return this;
    }

    public GroupBuilder distinct(String field) {
        groupClauses.add(new GroupClause(field, GroupOperation.DISTINCT));
        this.preOpt = GroupOperation.DISTINCT;
        return this;
    }

    public List<GroupClause> getGroupClauses() {
        return groupClauses;
    }

    public List<Map<String, GroupBuilder>> getNestAggsBuilders() {
        return nestAggsBuilders;
    }

    public List<Map<String, List<Range>>> getRangeAggsBuilders() {
        return rangeAggsBuilders;
    }


    public Map<String, List<GroupBuilder>> getSubGroupMap() {
        return subGroupMap;
    }

    public Map<String, WhereBuilder> getFilters() {
        return filters;
    }

    public int getLevel() {
        return level;
    }


}
