package work.yinli.elasticsearch.analytic.tool.builder.group;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/11 16:27
 **/
public class GroupOperation {
    // 支持的操作
    public static final String TERMS = "terms";

    public static final String NESTED = "nested";

    public static final String RANGE = "range";
    public static final String AVG = "avg";
    public static final String DISTINCT = "distinct";
    public static final String FILTER = "filter";

    // 未支持的操作


}
