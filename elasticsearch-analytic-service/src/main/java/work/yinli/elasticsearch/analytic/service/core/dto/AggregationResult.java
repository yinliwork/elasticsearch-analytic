package work.yinli.elasticsearch.analytic.service.core.dto;

import java.util.Map;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/15 14:16
 **/
public class AggregationResult {
    private String key;
    private long docCount;

    private Map<String, Object> subAggregations;

    public AggregationResult(String key, long docCount) {
        this.key = key;
        this.docCount = docCount;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getDocCount() {
        return docCount;
    }

    public void setDocCount(long docCount) {
        this.docCount = docCount;
    }

    public Map<String, Object> getSubAggregations() {
        return subAggregations;
    }

    public void setSubAggregations(Map<String, Object> subAggregations) {
        this.subAggregations = subAggregations;
    }

    @Override
    public String toString() {
        return "AggregationResult{" +
                "key='" + key + '\'' +
                ", docCount=" + docCount +
                '}';
    }
}
