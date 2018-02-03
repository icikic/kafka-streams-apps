package icikic.kstreams.movavg.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("scores")
public class ScoreStreamConfig {
    private String topic;
    private String averagesStoreName;
    private long windowSizeInSeconds;
    private long windowAdvanceInSeconds;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getAveragesStoreName() {
        return averagesStoreName;
    }

    public void setAveragesStoreName(String store) {
        this.averagesStoreName = store;
    }

    public long getWindowSizeInSeconds() {
        return windowSizeInSeconds;
    }

    public void setWindowSizeInSeconds(int windowSizeInSeconds) {
        this.windowSizeInSeconds = windowSizeInSeconds;
    }

    public long getWindowAdvanceInSeconds() {
        return windowAdvanceInSeconds;
    }

    public void setWindowAdvanceInSeconds(int windowAdvanceInSeconds) {
        this.windowAdvanceInSeconds = windowAdvanceInSeconds;
    }

    public long getWindowAdvanceInMillis() {
        return getWindowAdvanceInSeconds() * 1000;
    }

    public long getWindowSizeInMillis() {
        return getWindowSizeInSeconds() * 1000;
    }

    @Override
    public String toString() {
        return "ScoreStreamConfig{" +
                "topic='" + topic + '\'' +
                ", averagesStoreName='" + averagesStoreName + '\'' +
                ", windowSizeInSeconds=" + windowSizeInSeconds +
                ", windowAdvanceInSeconds=" + windowAdvanceInSeconds +
                '}';
    }
}
