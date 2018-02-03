package icikic.kstreams.pageview.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class PageView {
    public final String page;
    public final String user;
    public final String country;

    public PageView(@JsonProperty("page") final String page,
                    @JsonProperty("user") final String user,
                    @JsonProperty("country") final String country) {
        this.page = page;
        this.user = user;
        this.country = country;
    }

    @Override
    public String toString() {
        return "PageView{" +
                "page='" + page + '\'' +
                ", country='" + country + '\'' +
                ", user='" + user + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageView pageView = (PageView) o;
        return  Objects.equals(page, pageView.page) &&
                Objects.equals(country, pageView.country) &&
                Objects.equals(user, pageView.user);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page, country, user);
    }
}
