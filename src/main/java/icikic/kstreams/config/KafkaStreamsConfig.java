package icikic.kstreams.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@ConfigurationProperties("kafka.streams")
public class KafkaStreamsConfig {
    private Properties properties;
    private String scoresTopic;
    private String averagesStore;
    private long scoresWindowSizeInSeconds;
    private long scoresWindowAdvanceInSeconds;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getScoresTopic() {
        return scoresTopic;
    }

    public void setScoresTopic(String scoresTopic) {
        this.scoresTopic = scoresTopic;
    }

    public String getAveragesStore() {
        return averagesStore;
    }

    public void setAveragesStore(String store) {
        this.averagesStore = store;
    }

    public long getScoresWindowSizeInSeconds() {
        return scoresWindowSizeInSeconds;
    }

    public void setScoresWindowSizeInSeconds(int scoresWindowSizeInSeconds) {
        this.scoresWindowSizeInSeconds = scoresWindowSizeInSeconds;
    }

    public long getScoresWindowAdvanceInSeconds() {
        return scoresWindowAdvanceInSeconds;
    }

    public void setScoresWindowAdvanceInSeconds(int scoresWindowAdvanceInSeconds) {
        this.scoresWindowAdvanceInSeconds = scoresWindowAdvanceInSeconds;
    }

    public long getScoresWindowAdvanceInMillis() {
        return getScoresWindowAdvanceInSeconds() * 1000;
    }

    public long getScoresWindowSizeInMillis() {
        return getScoresWindowSizeInSeconds() * 1000;
    }

    @Override
    public String toString() {
        return "KafkaStreamsConfig {" +
                "properties=" + properties +
                ", scoresTopic='" + scoresTopic + '\'' +
                ", averagesStore='" + averagesStore + '\'' +
                ", scoresWindowSizeInSeconds=" + scoresWindowSizeInSeconds +
                ", scoresWindowAdvanceInSeconds=" + scoresWindowAdvanceInSeconds +
                '}';
    }
}
