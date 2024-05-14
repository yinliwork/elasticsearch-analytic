package work.yinli.elasticsearch.analytic.service.core.dto;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/13 10:29
 **/
public class YinliSearchRequest {
    private SearchRequest searchRequest;

    private UpdateByQueryRequest updateRequest;

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public void setSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    public UpdateByQueryRequest getUpdateRequest() {
        return updateRequest;
    }

    public void setUpdateRequest(UpdateByQueryRequest updateRequest) {
        this.updateRequest = updateRequest;
    }
}
