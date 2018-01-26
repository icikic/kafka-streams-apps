package icikic.kstreams.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@ConfigurationProperties("kafka.streams")
public class KafkaStreamConfig {
    private Properties properties;
    private String scoreTopic;
    private String averageScoreStore;
    private int averageWindowDurationInSeconds;
    private int averageWindowAdvanceInSeconds;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getScoreTopic() {
        return scoreTopic;
    }

    public void setScoreTopic(String scoreTopic) {
        this.scoreTopic = scoreTopic;
    }

    public String getAverageScoreStore() {
        return averageScoreStore;
    }

    public void setAverageScoreStore(String store) {
        this.averageScoreStore = store;
    }

    public int getAverageWindowDurationInSeconds() {
        return averageWindowDurationInSeconds;
    }

    public void setAverageWindowDurationInSeconds(int averageWindowDurationInSeconds) {
        this.averageWindowDurationInSeconds = averageWindowDurationInSeconds;
    }

    public int getAverageWindowAdvanceInSeconds() {
        return averageWindowAdvanceInSeconds;
    }

    public void setAverageWindowAdvanceInSeconds(int averageWindowAdvanceInSeconds) {
        this.averageWindowAdvanceInSeconds = averageWindowAdvanceInSeconds;
    }

    @Override
    public String toString() {
        return "KafkaStreamsProperties{" +
                "properties=" + properties +
                ", scoreTopic='" + scoreTopic + '\'' +
                ", averageScoreStore='" + averageScoreStore + '\'' +
                ", averageWindowDurationInSeconds=" + averageWindowDurationInSeconds +
                ", averageWindowAdvanceInSeconds=" + averageWindowAdvanceInSeconds +
                '}';
    }
}
