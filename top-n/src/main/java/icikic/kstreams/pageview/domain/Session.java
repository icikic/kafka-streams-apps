package icikic.kstreams.pageview.domain;

import java.util.ArrayList;
import java.util.List;

public class Session {
    public final List<PageView> pageViews = new ArrayList<>();

    public Session addPageView(PageView pv) {
        pageViews.add(pv);
        return this;
    }

    public Session merge(Session other) {
        pageViews.addAll(other.pageViews);
        return this;
    }

    public int size() {
        return pageViews.size();
    }
}
