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
public class ScoreStreamConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreStreamConfig.class);

    private KafkaStreamConfig kafkaStreamConfig;

    public ScoreStreamConfig(final KafkaStreamConfig props) {
        this.kafkaStreamConfig = props;
    }

    @Bean
    public KafkaStreams movingAverage() {
        LOGGER.debug("Properties {}", kafkaStreamConfig);
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Score> scores = builder.stream(kafkaStreamConfig.getScoreTopic(), Consumed.with(Serdes.Long(), new JsonSerde<>(Score.class)));

        scores.groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(kafkaStreamConfig.getAverageWindowDurationInSeconds()))
                        .advanceBy(TimeUnit.SECONDS.toMillis(kafkaStreamConfig.getAverageWindowAdvanceInSeconds())))
                .aggregate(
                        Average::new,
                        (key, value, aggregate) -> aggregate.update(value),
                        Materialized.<Long, Average, WindowStore<Bytes, byte[]>>as(kafkaStreamConfig.getAverageScoreStore())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(new JsonSerde<>(Average.class)));

        return new KafkaStreams(builder.build(), kafkaStreamConfig.getProperties());
    }

}
