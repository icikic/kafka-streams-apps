package icikic.kstreams.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@ConfigurationProperties("kafka.streams")
public class KafkaStreamsProperties {
    private Properties properties;
    private String scoreTopic;
    private String averageScoreTopic;
    private int averageWindowDurationInSeconds;
    private int averageWindowAdvanceInSeconds;
    private String applicationHost;
    private int applicationPort;

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

    public String getAverageScoreTopic() {
        return averageScoreTopic;
    }

    public void setAverageScoreTopic(String averageScoreTopic) {
        this.averageScoreTopic = averageScoreTopic;
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

    public String getApplicationHost() {
        return applicationHost;
    }

    public void setApplicationHost(String applicationHost) {
        this.applicationHost = applicationHost;
    }

    public int getApplicationPort() {
        return applicationPort;
    }

    public void setApplicationPort(int applicationPort) {
        this.applicationPort = applicationPort;
    }

    @Override
    public String toString() {
        return "KafkaStreamsProperties{" +
                "properties=" + properties +
                ", scoreTopic='" + scoreTopic + '\'' +
                ", averageScoreTopic='" + averageScoreTopic + '\'' +
                ", averageWindowDurationInSeconds=" + averageWindowDurationInSeconds +
                ", averageWindowAdvanceInSeconds=" + averageWindowAdvanceInSeconds +
                '}';
    }
}
