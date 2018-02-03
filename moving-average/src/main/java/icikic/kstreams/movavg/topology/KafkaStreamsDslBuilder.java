package icikic.kstreams.movavg.topology;

import icikic.kstreams.config.KafkaConfig;
import icikic.kstreams.movavg.config.AverageScoreConfig;
import icikic.kstreams.movavg.domain.Average;
import icikic.kstreams.movavg.domain.Score;
import icikic.kstreams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import java.util.concurrent.TimeUnit;

@Configuration
@Profile("default")
@Import(KafkaConfig.class)
public class KafkaStreamsDslBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsDslBuilder.class);

    private final KafkaConfig kafkaConfig;
    private final AverageScoreConfig scoreConfig;

    public KafkaStreamsDslBuilder(final KafkaConfig kafkaConfig, final AverageScoreConfig scoreConfig) {
        this.kafkaConfig = kafkaConfig;
        this.scoreConfig = scoreConfig;
    }

    @Bean(name = "MovingAverageStream")
    public KafkaStreams buildKafkaStreams() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Score> scores = builder.stream(scoreConfig.getScoreTopic(), Consumed.with(Serdes.Long(), new JsonSerde<>(Score.class)));

        scores.groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(scoreConfig.getWindowSizeInSeconds()))
                        .advanceBy(TimeUnit.SECONDS.toMillis(scoreConfig.getWindowAdvanceInSeconds())))
                .aggregate(
                        Average::new,
                        (key, value, aggregate) -> aggregate.update(value),
                        Materialized.<Long, Average, WindowStore<Bytes, byte[]>>as(scoreConfig.getAverageScoreStoreName())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(new JsonSerde<>(Average.class)));

        final Topology topology = builder.build();
        LOGGER.debug("{}", topology.describe());
        return new KafkaStreams(topology, kafkaConfig.getProperties());
    }
}
