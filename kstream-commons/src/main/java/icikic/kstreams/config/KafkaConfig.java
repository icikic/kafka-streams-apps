package icikic.kstreams.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@ConfigurationProperties("kafka")
public class KafkaConfig {
    private Properties properties;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "KafkaStreamsConfig{" +
                "properties=" + properties +
                '}';
    }
}
