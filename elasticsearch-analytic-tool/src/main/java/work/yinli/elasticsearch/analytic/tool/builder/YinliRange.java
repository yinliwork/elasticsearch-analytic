package work.yinli.elasticsearch.analytic.tool.builder;

/**
 * @author ahianzhang
 * @version 1.0
 * @date 2024/3/11 16:31
 **/
public class YinliRange {
    public Object from;
    public Object to;

    public YinliRange(Object from, Object to) {
        this.from = from;
        this.to = to;
    }

    public Object getFrom() {
        return from;
    }

    public Object getTo() {
        return to;
    }
}
