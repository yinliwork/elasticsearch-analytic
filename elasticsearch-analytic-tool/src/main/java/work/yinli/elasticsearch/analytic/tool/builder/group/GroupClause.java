package work.yinli.elasticsearch.analytic.tool.builder.group;


import work.yinli.elasticsearch.analytic.tool.builder.order.Order;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/11 15:57
 **/
public final class GroupClause {
    private final String field;
    private String operation = GroupOperation.TERMS;
    private int size = 100;
    private Order order;


    public GroupClause(String field, String operation) {
        this.field = field;
        this.operation = operation;
    }

    public GroupClause(String field, String operation, int size) {
        this.field = field;
        this.operation = operation;
        this.size = size;
    }

    public GroupClause(String field, String operation, Order order) {
        this.field = field;
        this.operation = operation;
        this.order = order;
    }

    public String getField() {
        return field;
    }

    public String getOperation() {
        return operation;
    }

    public int getSize() {
        return size;
    }

    public Order getOrder() {
        return order;
    }

    public GroupClause(String field, String operation, int size, Order order) {
        this.field = field;
        this.operation = operation;
        this.size = size;
        this.order = order;
    }
}
