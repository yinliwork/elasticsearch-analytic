package work.yinli.elasticsearch.analytic.tool.builder;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/8 18:01
 **/
public final class QueryBuilders<T> {
    public CommonQueryBuilder<T> common() {
        return new CommonQueryBuilder<T>();
    }

    public ExportQueryBuilder<T> export() {
        return new ExportQueryBuilder<T>();
    }
}
