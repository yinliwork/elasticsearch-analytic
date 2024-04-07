package work.yinli.elasticsearch.analytic.tool.builder.order;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/11 16:30
 **/
public class Order {
    private final String field;
    private final String order;

    public Order(String field, String order) {
        this.field = field;
        this.order = order;
    }

    public String getField() {
        return field;
    }

    public String getOrder() {
        return order;
    }
}
