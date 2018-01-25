package icikic.kstreams.config;

import icikic.kstreams.domain.AverageScore;
import icikic.kstreams.domain.Score;
import icikic.kstreams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class KafkaStreamsConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsConfiguration.class);

    private KafkaStreamsProperties kafkaStreamsProperties;

    public KafkaStreamsConfiguration(final KafkaStreamsProperties props) {
        this.kafkaStreamsProperties = props;
    }

    @Bean
    public KafkaStreams kstreams() {
        LOGGER.debug("Properties {}", kafkaStreamsProperties);
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Score> scores = builder.stream(kafkaStreamsProperties.getScoreTopic(), Consumed.with(Serdes.Long(), new JsonSerde<>(Score.class)));

        final KTable<Windowed<Long>, AverageScore> averages = scores
                .groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(kafkaStreamsProperties.getAverageWindowDurationInSeconds()))
                        .advanceBy(TimeUnit.SECONDS.toMillis(kafkaStreamsProperties.getAverageWindowAdvanceInSeconds())))
                .aggregate(
                        AverageScore::new,
                        (key, value, aggregate) -> aggregate.update(value),
                        Materialized.<Long, AverageScore, WindowStore<Bytes, byte[]>>as(kafkaStreamsProperties.getAverageScoreTopic())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(new JsonSerde<>(AverageScore.class)));

        //averages.toStream().print(Printed.toSysOut());
        return new KafkaStreams(builder.build(), kafkaStreamsProperties.getProperties());
    }


    public void setProperties(KafkaStreamsProperties props) {
        this.kafkaStreamsProperties = kafkaStreamsProperties;
    }

}
