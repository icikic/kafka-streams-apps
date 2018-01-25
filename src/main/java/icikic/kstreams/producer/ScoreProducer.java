package icikic.kstreams.producer;

import com.google.common.util.concurrent.RateLimiter;
import icikic.kstreams.domain.Score;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;


/**
 */
public class ScoreProducer implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreProducer.class);

    private static final String SCORE_GENERATOR_EVENTS_PER_SECOND = "SCORE_GENERATOR_EVENTS_PER_SECOND";
    private static final String SCORE_GENERATOR_RUN_TIME_IN_SECONDS = "SCORE_GENERATOR_RUN_TIME_IN_SECONDS";

    private static final int  DEFAULT_EVENTS_PER_SECOND = 1;
    private static final long DEFAULT_RUN_TIME_IN_SECONDS = Long.MAX_VALUE;

    private final int eventsPerSecond;
    private final long runTimeInSeconds;
    private final KafkaProducer<Long, Score> kafkaProducer;
    private final Iterator<Score> scores;

    ScoreProducer() {
        this(System.getenv());
    }

    ScoreProducer(Map<String, String> env) {
        this.eventsPerSecond = ofNullable(env.get(SCORE_GENERATOR_EVENTS_PER_SECOND)).map(Integer::parseInt).orElse(DEFAULT_EVENTS_PER_SECOND);
        this.runTimeInSeconds = ofNullable(env.get(SCORE_GENERATOR_RUN_TIME_IN_SECONDS)).map(Long::parseLong).orElse(DEFAULT_RUN_TIME_IN_SECONDS);
        this.kafkaProducer = new KafkaProducer<>(kafkaConfig(env));
        this.scores = Stream.generate(new ScoreGenerator()).iterator();
    }

    public void start() {
        final RateLimiter rateLimiter = RateLimiter.create(eventsPerSecond);
        final long runUntil = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(runTimeInSeconds);
        final LongAdder count = new LongAdder();
        while (System.currentTimeMillis() < runUntil) {
            rateLimiter.acquire();
            if (scores.hasNext()) {
                sendOne(scores.next());
                count.increment();
            }
        }

        LOGGER.info("PUBLISHED " + count.longValue() + " events");
    }

    private void sendOne(final Score score) {
        ProducerRecord<Long, Score> record = new ProducerRecord<>("SCORES", score.playerId, score);
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Exception while sending kafka message", exception);
            } else {
                LOGGER.info("{} successfully sent to {}, {}, {}", score, metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }

    public static void main(String[] args) {
        try (final ScoreProducer producer = new ScoreProducer()) {
            producer.start();
        }
    }

    private static Properties kafkaConfig(Map<String, String> env) {
        return env.entrySet().stream().filter(e -> e.getKey().startsWith("KAFKA_")).map(e -> {
            final String newKey = e.getKey()
                    .replace("KAFKA_", "")
                    .replace("_", ".")
                    .toLowerCase();
            return new AbstractMap.SimpleEntry<>(newKey, e.getValue());
        }).collect(toProperties(AbstractMap.SimpleEntry::getKey,
                                AbstractMap.SimpleEntry::getValue));

    }

    private static <T, K, U> Collector<T, ?, Properties> toProperties(final Function<? super T, ? extends K> keyMapper,
                                                                      final Function<? super T, ? extends U> valueMapper) {
        return Collectors.toMap(keyMapper,
                valueMapper,
                throwingMerger(),
                Properties::new);
    }

    private static <T>BinaryOperator<T> throwingMerger() {
        return (u, v) -> { throw new IllegalStateException(String.format("Duplicate keys %s", u)); };
    }
}
