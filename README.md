# Overview

查询分为三个部分 WhereBuilder、GroupBuilder 和 SkQueryBuilder，他们分别代表查询、分组聚合、主查询语句构造。详情如下

## WhereBuilder
```java
WhereBuilder test = new WhereBuilder()
        .and(new WhereBuilder()
                .or(new WhereBuilder().range("IndexTime", new SkRange("2024-03-08 18:01:00", "2024-03-08 18:01:00")))
                .or(new WhereBuilder().eq("field", "val").eq("keywordId", "4109"))
                .or(new WhereBuilder().like("Author", "作者"))
                .nest("path", new WhereBuilder().eq("field", "val")))
        .not(new WhereBuilder().eq("originType", "twitter"));
```

## GroupBuilder

```java
GroupBuilder groupBuilder = new GroupBuilder()
                .addField("sub")
                .sub(new GroupBuilder()
                        .addField("subSub", 999)
                        .sub(new GroupBuilder().addField("subSubSub", 999)))
                .nest("path", new GroupBuilder().addField("nest.field").addField("nest.field2").sub(new GroupBuilder().addField("nestSub")))
                .range("rangeField", Arrays.asList(new SkRange(1, 2), new SkRange(2, 3)));
```

## QueryBuilder

```java

SkQueryBuilder<Object> query = new SkQueryBuilders<>().common().select("ID")
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
            "Fragments.Sentiment": [
                {
                    "docCount": 2,
                    "key": "e"
                }
            ]
        },
        "dsl": "{\"from\":0,\"size\":10,\"query\":{\"bool\":{\"filter\":[{\"bool\":{\"must\":[{\"range\":{\"IndexTime\":{\"from\":\"2024-03-05 13:59:58\",\"to\":\"2024-03-08 18:01:00\",\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"_source\":{\"includes\":[\"ID\"],\"excludes\":[]},\"aggregations\":{\"Fragments\":{\"nested\":{\"path\":\"Fragments\"},\"aggregations\":{\"Fragments.Sentiment\":{\"terms\":{\"field\":\"Fragments.Sentiment\",\"size\":100,\"min_doc_count\":1,\"shard_min_doc_count\":0,\"show_term_doc_count_error\":false,\"order\":[{\"_count\":\"desc\"},{\"_key\":\"asc\"}]}}}}}}",
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