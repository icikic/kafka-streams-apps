package icikic.kstreams.config;

import icikic.kstreams.domain.Average;
import icikic.kstreams.domain.Score;
import icikic.kstreams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class ScoreServiceConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreServiceConfig.class);

    private KafkaStreamsConfig kafkaStreamsConfig;

    public ScoreServiceConfig(final KafkaStreamsConfig props) {
        this.kafkaStreamsConfig = props;
    }

    @Bean
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

        return new KafkaStreams(builder.build(), kafkaStreamsConfig.getProperties());
    }
}
