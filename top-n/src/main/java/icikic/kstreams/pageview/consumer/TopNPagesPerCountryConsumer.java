package icikic.kstreams.pageview.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;

import static icikic.kstreams.config.ConfigUtils.kafkaConfig;
import static java.util.Optional.ofNullable;
import static org.apache.kafka.common.serialization.Serdes.String;

public class TopNPagesPerCountryConsumer implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopNPagesPerCountryConsumer.class);
    private static final String PAGEVIEW_CONSUMER_RUN_TIME_IN_SECONDS = "PAGEVIEW_CONSUMER_RUN_TIME_IN_SECONDS";

    private static final long DEFAULT_PAGEVIEW_CONSMER_RUN_TIME_IN_SECONDS = Long.MAX_VALUE;

    private final long runTimeInSeconds;

    private final KafkaConsumer<Windowed<String>, String> consumer;

    public TopNPagesPerCountryConsumer(Map<String, String> conf) {
        this.consumer = new KafkaConsumer<>(kafkaConfig(conf),
                new WindowedDeserializer<>(String().deserializer()),
                String().deserializer());
        this.consumer.subscribe(Collections.singleton("TOP_PAGES_PER_COUNTRY"));
        this.runTimeInSeconds = ofNullable(conf.get(PAGEVIEW_CONSUMER_RUN_TIME_IN_SECONDS)).map(Long::parseLong).orElse(DEFAULT_PAGEVIEW_CONSMER_RUN_TIME_IN_SECONDS);
    }
    public static void main(String[] args) {
        try (final TopNPagesPerCountryConsumer consumer = new TopNPagesPerCountryConsumer(System.getenv())) {
            consumer.start();
        }
    }

    public void start() {

        final long timeout = System.currentTimeMillis() + runTimeInSeconds * 1000;
        while (System.currentTimeMillis() < timeout) {
            final ConsumerRecords<Windowed<String>, String> records = consumer.poll(1000);
            records.forEach(record -> {
                LOGGER.info("{}: {}", record.key().key(), Arrays.asList(record.value().split("\n")));
            });

        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
