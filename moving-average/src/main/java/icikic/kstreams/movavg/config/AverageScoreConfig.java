package icikic.kstreams.movavg.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("moving.average")
public class AverageScoreConfig {
    private String scoreTopic;
    private String averageScoreStoreName;
    private long windowSizeInSeconds;
    private long windowAdvanceInSeconds;

    public String getScoreTopic() {
        return scoreTopic;
    }

    public void setScoreTopic(String scoreTopic) {
        this.scoreTopic = scoreTopic;
    }

    public String getAverageScoreStoreName() {
        return averageScoreStoreName;
    }

    public void setAverageScoreStoreName(String store) {
        this.averageScoreStoreName = store;
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
                "scoreTopic='" + scoreTopic + '\'' +
                ", averageScoreStoreName='" + averageScoreStoreName + '\'' +
                ", windowSizeInSeconds=" + windowSizeInSeconds +
                ", windowAdvanceInSeconds=" + windowAdvanceInSeconds +
                '}';
    }
}
