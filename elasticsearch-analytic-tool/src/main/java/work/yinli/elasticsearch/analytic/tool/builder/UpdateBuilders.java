package work.yinli.elasticsearch.analytic.tool.builder;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/8 18:01
 **/
public final class UpdateBuilders<T> {
    public CommonUpdateBuilder<T> common() {
        return new CommonUpdateBuilder<>();
    }
}
