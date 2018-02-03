package icikic.kstreams.pageview.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PageViewWithContent {
    public final PageUpdate update;
    public final PageView view;


    public PageViewWithContent(@JsonProperty("view") PageView view, @JsonProperty("update") PageUpdate update) {
        this.view = view;
        this.update = update;
    }

    @Override
    public String toString() {
        return "PageViewWithContent{" +
                "update=" + update +
                ", view=" + view +
                '}';
    }
}
