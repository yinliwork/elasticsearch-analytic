package work.yinli.elasticsearch.analytic.tool.builder.where;


import work.yinli.elasticsearch.analytic.tool.builder.Range;

import java.util.*;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/11 14:34
 **/
public class WhereBuilder {
    List<WhereClause> whereClauses = new ArrayList<>();
    List<WhereBuilder> orList = new ArrayList<>();
    List<WhereBuilder> andList = new ArrayList<>();
    List<WhereBuilder> notList = new ArrayList<>();

    List<Map<String, List<Range>>> rangeList = new ArrayList<>();
    List<Map<String, WhereBuilder>> nestList = new ArrayList<>();

    public WhereBuilder() {
    }

    public List<WhereClause> getWhereClauses() {
        return whereClauses;
    }

    public List<WhereBuilder> getOrList() {
        return orList;
    }

    public List<WhereBuilder> getAndList() {
        return andList;
    }

    public List<WhereBuilder> getNotList() {
        return notList;
    }

    public List<Map<String, WhereBuilder>> getNestList() {
        return nestList;
    }

    public WhereBuilder or(WhereBuilder whereBuilder) {
        orList.add(whereBuilder);
        return this;
    }

    /**
     * 同时添加多个or条件，并列查询
     *
     * @param whereBuilders
     * @return
     */
    public WhereBuilder or(List<WhereBuilder> whereBuilders) {
        orList.addAll(whereBuilders);
        return this;
    }

    public WhereBuilder and(WhereBuilder whereBuilder) {
        andList.add(whereBuilder);
        return this;
    }

    /**
     * 同时添加多个and条件，并列查询
     *
     * @param whereBuilders
     * @return
     */
    public WhereBuilder and(List<WhereBuilder> whereBuilders) {
        andList.addAll(whereBuilders);
        return this;
    }

    public WhereBuilder not(WhereBuilder whereBuilder) {
        notList.add(whereBuilder);
        return this;
    }

    /**
     * 同时添加多个not条件，并列查询
     *
     * @param whereBuilders
     * @return
     */
    public WhereBuilder not(List<WhereBuilder> whereBuilders) {
        notList.addAll(whereBuilders);
        return this;
    }

    public WhereBuilder range(String field, Range... ranges) {
        Map<String, List<Range>> map = new HashMap<>();
        map.put(field, Arrays.asList(ranges));
        this.rangeList.add(map);
        return this;
    }


    public WhereBuilder eq(String field, Object val) {
        this.whereClauses.add(new WhereClause(field, val, WhereOperation.EQ));
        return this;
    }

    public WhereBuilder ne(String field, Object val) {
        this.whereClauses.add(new WhereClause(field, val, WhereOperation.NE));
        return this;
    }

//    public WhereBuilder gt(String field, Object val) {
//        this.whereClauses.add(new WhereClause(field, val, WhereOperation.GT));
//        return this;
//    }

//    public WhereBuilder lt(String field, Object val) {
//        this.whereClauses.add(new WhereClause(field, val, WhereOperation.LT));
//        return this;
//    }

    public WhereBuilder gte(String field, Object val) {
        this.whereClauses.add(new WhereClause(field, val, WhereOperation.GTE));
        return this;
    }

    public WhereBuilder lte(String field, Object val) {
        this.whereClauses.add(new WhereClause(field, val, WhereOperation.LTE));
        return this;
    }

    public WhereBuilder like(String field, Object val) {
        this.whereClauses.add(new WhereClause(field, val, WhereOperation.LIKE));
        return this;
    }

    public WhereBuilder in(String field, Object... val) {
        if (val == null || val.length == 0) {
            throw new IllegalArgumentException("in value can not be null or empty");
        }
        if (val[0] instanceof List) {
            throw new IllegalArgumentException("in value can not be List");
        }
        this.whereClauses.add(new WhereClause(field, val, WhereOperation.IN));
        return this;
    }

    public WhereBuilder nest(String outerFiledName, WhereBuilder whereBuilder) {
        Map<String, WhereBuilder> map = new HashMap<>();
        map.put(outerFiledName, whereBuilder);
        this.nestList.add(map);
        return this;
    }

    public WhereBuilder nest(String outerFiledName, WhereBuilder whereBuilder, String scoreMode) {
        return this;
    }


    public List<Map<String, List<Range>>> getRangeList() {
        return rangeList;
    }

}