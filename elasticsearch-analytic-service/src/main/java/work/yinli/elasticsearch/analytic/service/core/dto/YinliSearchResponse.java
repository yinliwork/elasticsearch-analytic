package work.yinli.elasticsearch.analytic.service.core.dto;

import java.util.Map;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/13 10:21
 **/
public class YinliSearchResponse {
    private int offset;
    private int size;
    private String dsl;
    private String lastId;
    private boolean isEnd;

    private long totalCount;
    private Object resultList;

    private Map<String, Object> aggregations;


    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getDsl() {
        return dsl;
    }

    public void setDsl(String dsl) {
        this.dsl = dsl;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public Object getResultList() {
        return resultList;
    }

    public void setResultList(Object resultList) {
        this.resultList = resultList;
    }

    public Map<String, Object> getAggregations() {
        return aggregations;
    }

    public void setAggregations(Map<String, Object> aggregations) {
        this.aggregations = aggregations;
    }

    public String getLastId() {
        return lastId;
    }

    public void setLastId(String lastId) {
        this.lastId = lastId;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }

    @Override
    public String toString() {
        return "SearchResponse{" +
                "offset=" + offset +
                ", size=" + size +
                ", dsl=【【【'" + dsl + '\'' +
                "】】】, totalCount=" + totalCount +
                ", resultList=" + resultList +
                ", aggregations=" + aggregations +
                '}';
    }
}
