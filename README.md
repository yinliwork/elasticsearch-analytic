![ElasticSearch](https://img.shields.io/badge/ElasticSearch-7.15.1-blue)  ![jdk](https://img.shields.io/badge/jdk-8-blue)

# 前沿

本项目是为了解决使用 ElasticSearch 做**数据分析**的场景下，使用更便捷的方式构造查询语句。


## 优势

1. 依赖隔离，在未来做 ES 版本升级时只需要对工具包进行升级，而业务代码无需改动。
2. 使用简单，所见即所得。
3. 查询性能优化，在内部使用布尔检索，支持非常复杂的查询。
4. 支持数据全量导出。
5. 支持 update 更新多个字段。

## 不适合
1. 需要计算得分的场景。
2. 全文检索（虽然这是 ES 的特性，但是此工具重点是聚合计算）


目前在公司内部（舆情检索）正在使用，能够满足大部分的查询需求，能在亿级别文档下快速返回结果。
感兴趣可以[邮件交流](mailto:ahianzhang@gmail.com)。

![core.png](.github/assets/img/core.png)

查询分为三个部分 WhereBuilder、GroupBuilder 和 QueryBuilder，他们分别代表查询、分组聚合、主查询语句构造。详情如下

## WhereBuilder
```java
WhereBuilder test = new WhereBuilder()
        .and(new WhereBuilder()
                .or(new WhereBuilder().range("IndexTime", new YinliRange("2024-03-08 18:01:00", "2024-03-08 18:01:00")))
                .or(new WhereBuilder().eq("field", "val").eq("keywordId", "4109"))
                .or(new WhereBuilder().like("Author", "作者"))
                .nest("path", new WhereBuilder().eq("field", "val")))
        .not(new WhereBuilder().eq("website", "twitter"));
```
外层三种逻辑关系：
- and 子句必须全部命中
- or 子句至少有一个命中
- not 子句必须不能命中
这三层逻辑关系可以任意组合，支持多层嵌套。<html><font color=red>``or(xx)`` 指的是括号里面的逻辑关系是 or 。</font></html> 所以如果查询条件是 A 或 B 时，写作 ``or(new WhereBuilder().eq("A").eq("B"))``

Where 子句支持查询条件如下：

- range：范围查询
- eq：等于
- in: 多值查询
- like：模糊查询，对应 ES 的 match_phrase
- regex: 正则查询
- nest：查询嵌套对象的内容
- word-spacing: 词间距查询

GroupBuilder 子句支持分组条件如下：

- addField：按某个 field 字段统计数量
- addFilter：添加过滤条件
- sub：开启子查询
- nest: nested 类型中的数据统计
- distinct：数据去重
- avg：平均值
- max：最大值
- min：最小值

## GroupBuilder

```java
GroupBuilder groupBuilder = new GroupBuilder()
                .addField("sub")
                .sub(new GroupBuilder()
                        .addField("subSub", 999)
                        .sub(new GroupBuilder().addField("subSubSub", 999)))
                .nest("path", new GroupBuilder().addField("nest.field").addField("nest.field2").sub(new GroupBuilder().addField("nestSub")))
                .range("rangeField", Arrays.asList(new YinliRange(1, 2), new YinliRange(2, 3)));
```

## QueryBuilder

```java

QueryBuilder<Object> query = new QueryBuilders<>().common().select("ID")
                .from(Arrays.asList("indexName"))
                .where(test)
                .orderBy("IndexTime")
//                .groupBy("published")
                .groupBy(groupBuilder)
                .limit(0, 10);
```
## Response

```json
{
    "data": {
        "aggregations": {
            "XXX": [
                {
                    "docCount": 2,
                    "key": "e"
                }
            ]
        },
        "dsl": "...",
        "offset": 0,
        "resultList": [
            {
                "ID": "1"
            },
            {
                "ID": "2"
            },
            {
                "ID": "3"
            }
        ],
        "size": 10,
        "totalCount": 3
    },
    "errorMsg": "",
    "success": true
}
```
# 使用文档

[wiki](https://github.com/yinliwork/elasticsearch-analytic/wiki)