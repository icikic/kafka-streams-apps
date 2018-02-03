package icikic.kstreams.pageview.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PageViewStats {
    public long count;
    public PageView pageView;

    public PageViewStats(@JsonProperty("pageView") PageView pv, @JsonProperty("count") long count) {
        this.pageView = pv;
        this.count = count;
    }

}
