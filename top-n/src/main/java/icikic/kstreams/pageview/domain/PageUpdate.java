package icikic.kstreams.pageview.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PageUpdate {
    public final String page;
    public final String content;
    public final String user;

    public PageUpdate(@JsonProperty("page") final String page, @JsonProperty("content") final String content, @JsonProperty("user") final String user) {
        this.page = page;
        this.content = content;
        this.user = user;
    }

    @Override
    public String toString() {
        return "PageUpdate{" +
                "page='" + page + '\'' +
                ", content='" + content + '\'' +
                ", user='" + user + '\'' +
                '}';
    }
}
