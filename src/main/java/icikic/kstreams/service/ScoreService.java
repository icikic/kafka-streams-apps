package icikic.kstreams.service;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Service
public class ScoreService implements icikic.kstreams.service.Service {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreService.class);

    private final KafkaStreams kafkaStreams;

    @Autowired
    public ScoreService(final KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @PostConstruct
    @Override
    public void start() {
        this.kafkaStreams.cleanUp();
        this.kafkaStreams.start();
        LOGGER.info("[{}] STARTED", kafkaStreams);
    }

    @Override
    @PreDestroy
    public void stop() {
        this.kafkaStreams.close();
        LOGGER.info("[{}] CLOSED", kafkaStreams);
    }
}
