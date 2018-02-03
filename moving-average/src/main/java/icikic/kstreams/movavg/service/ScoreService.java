package icikic.kstreams.movavg.service;

import icikic.kstreams.service.KStreamsLifecycle;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Service
public class ScoreService implements KStreamsLifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreService.class);

    private final KafkaStreams kafkaStreams;

    @Autowired
    public ScoreService(@Qualifier("MovingAverageStream") final KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @PostConstruct
    @Override
    public void start() {
        this.kafkaStreams.cleanUp();
        this.kafkaStreams.start();

        LOGGER.info("[{}] STARTED", kafkaStreams);
    }

    @PreDestroy
    @Override
    public void stop() {
        this.kafkaStreams.close();
        LOGGER.info("[{}] CLOSED", kafkaStreams);
    }

    public KafkaStreams getKafkaStreams() {
        return kafkaStreams;
    }
}
