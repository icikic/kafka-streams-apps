package icikic.kstreams.topology;

import icikic.kstreams.config.KafkaStreamsConfig;
import icikic.kstreams.domain.Average;
import icikic.kstreams.domain.Score;
import icikic.kstreams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import java.util.concurrent.TimeUnit;

@Configuration
@Profile("default")
public class KafkaStreamsDslBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsDslBuilder.class);

    private KafkaStreamsConfig kafkaStreamsConfig;

    public KafkaStreamsDslBuilder(final KafkaStreamsConfig props) {
        this.kafkaStreamsConfig = props;
    }

    @Bean(name = "StreamsUsingDSL")
    public KafkaStreams buildKafkaStreams() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Score> scores = builder.stream(kafkaStreamsConfig.getScoresTopic(), Consumed.with(Serdes.Long(), new JsonSerde<>(Score.class)));

        scores.groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(kafkaStreamsConfig.getScoresWindowSizeInSeconds()))
                        .advanceBy(TimeUnit.SECONDS.toMillis(kafkaStreamsConfig.getScoresWindowAdvanceInSeconds())))
                .aggregate(
                        Average::new,
                        (key, value, aggregate) -> aggregate.update(value),
                        Materialized.<Long, Average, WindowStore<Bytes, byte[]>>as(kafkaStreamsConfig.getAveragesStore())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(new JsonSerde<>(Average.class)));

        final Topology topology = builder.build();
        LOGGER.debug("{}", topology.describe());
        return new KafkaStreams(topology, kafkaStreamsConfig.getProperties());
    }
}
