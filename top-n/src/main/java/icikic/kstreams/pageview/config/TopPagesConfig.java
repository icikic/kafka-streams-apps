package icikic.kstreams.pageview.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("top-n")
public class TopPagesConfig {
    private String pageViewTopic;
    private String pageUpdateTopic;
    private String topPagesPerCountyTopic;
    private long requestsPerSecondThreshold;
    private long pageSizeThreshold;
    private long timeBucketInSeconds;
    private int N;

    public String getPageViewTopic() {
        return pageViewTopic;
    }

    public void setPageViewTopic(String pageViewTopic) {
        this.pageViewTopic = pageViewTopic;
    }

    public String getPageUpdateTopic() {
        return pageUpdateTopic;
    }

    public void setPageUpdateTopic(String pageUpdateTopic) {
        this.pageUpdateTopic = pageUpdateTopic;
    }

    public String getTopPagesPerCountyTopic() {
        return topPagesPerCountyTopic;
    }

    public void setTopPagesPerCountyTopic(String topPagesPerCountyTopic) {
        this.topPagesPerCountyTopic = topPagesPerCountyTopic;
    }

    public long getRequestsPerSecondThreshold() {
        return requestsPerSecondThreshold;
    }

    public void setRequestsPerSecondThreshold(long requestsPerSecondThreshold) {
        this.requestsPerSecondThreshold = requestsPerSecondThreshold;
    }

    public long getPageSizeThreshold() {
        return pageSizeThreshold;
    }

    public void setPageSizeThreshold(long pageSizeThreshold) {
        this.pageSizeThreshold = pageSizeThreshold;
    }

    public long getTimeBucketInSeconds() {
        return timeBucketInSeconds;
    }

    public void setTimeBucketInSeconds(long timeBucketInSeconds) {
        this.timeBucketInSeconds = timeBucketInSeconds;
    }

    public int getN() {
        return N;
    }

    public void setN(int n) {
        N = n;
    }

    @Override
    public String toString() {
        return "TopPagesConfig{" +
                "pageViewTopic='" + pageViewTopic + '\'' +
                ", pageUpdateTopic='" + pageUpdateTopic + '\'' +
                ", topPagesPerCountyTopic='" + topPagesPerCountyTopic + '\'' +
                ", requestsPerSecondThreshold=" + requestsPerSecondThreshold +
                ", pageSizeThreshold=" + pageSizeThreshold +
                ", timeBucketInSeconds=" + timeBucketInSeconds +
                ", N=" + N +
                '}';
    }
}
