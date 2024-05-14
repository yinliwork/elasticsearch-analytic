package work.yinli.elasticsearch.analytic.service.core;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/15 10:26
 **/
public class DefaultContext {

    private static final ThreadLocal<Map<String, Object>> local = new ThreadLocal<>();

    public static void addVariable(String key, Object value) {
        if (local.get() == null) {
            Map<String, Object> map = new HashMap<>();
            local.set(map);
        }
        local.get().put(key, value);
    }

    public static Map<String, Object> getContext() {
        return local.get();
    }

    public static Object getVariable(String key) {
        if (local.get() == null) {
            return null;
        }
        return local.get().get(key);
    }

    public static void clearAll() {
        local.remove();
    }
}
