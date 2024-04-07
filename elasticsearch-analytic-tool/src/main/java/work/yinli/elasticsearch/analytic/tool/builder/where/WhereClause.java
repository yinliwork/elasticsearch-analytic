package work.yinli.elasticsearch.analytic.tool.builder.where;

import java.util.Map;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/11 15:33
 **/
public final class WhereClause {

    public WhereClause(String field, Object val, String operation) {
        this.field = field;
        this.val = val;
        this.operation = operation;
    }

    public WhereClause(String field, Object val, Map<String, Object> config, String operation) {
        this.field = field;
        this.val = val;
        this.config = config;
        this.operation = operation;
    }

    private String field;
    private Object val;
    private Map<String, Object> config;
    private String operation;

    public String getField() {
        return field;
    }

    public Object getVal() {
        return val;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public String getOperation() {
        return operation;
    }
}

