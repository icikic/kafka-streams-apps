package icikic.kstreams.pageview.producer;

import com.google.common.util.concurrent.RateLimiter;
import icikic.kstreams.pageview.domain.PageUpdate;
import icikic.kstreams.pageview.domain.PageView;
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

import static icikic.kstreams.config.ConfigUtils.kafkaConfig;
import static java.util.Optional.ofNullable;


/**
 */
public class PageViewProducer implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PageViewProducer.class);

    private static final String PAGEVIEW_GENERATOR_EVENTS_PER_SECOND = "PAGEVIEW_GENERATOR_EVENTS_PER_SECOND";
    private static final String PAGEVIEW_GENERATOR_RUN_TIME_IN_SECONDS = "PAGEVIEW_GENERATOR_RUN_TIME_IN_SECONDS";

    private static final int  DEFAULT_EVENTS_PER_SECOND = 1;
    private static final long DEFAULT_RUN_TIME_IN_SECONDS = Long.MAX_VALUE;

    private final int eventsPerSecond;
    private final long runTimeInSeconds;
    private final KafkaProducer<String, PageView> pvProducer;
    private final KafkaProducer<String, PageUpdate> puProducer;

    PageViewProducer() {
        this(System.getenv());
    }

    PageViewProducer(Map<String, String> env) {
        this.eventsPerSecond = ofNullable(env.get(PAGEVIEW_GENERATOR_EVENTS_PER_SECOND)).map(Integer::parseInt).orElse(DEFAULT_EVENTS_PER_SECOND);
        this.runTimeInSeconds = ofNullable(env.get(PAGEVIEW_GENERATOR_RUN_TIME_IN_SECONDS)).map(Long::parseLong).orElse(DEFAULT_RUN_TIME_IN_SECONDS);
        final Properties config = kafkaConfig(env);
        this.pvProducer = new KafkaProducer<>(config);
        this.puProducer = new KafkaProducer<>(config);
    }

    public void start() {
        // push all page updates first
        Generators.finitePageUpdates().forEach(update -> send(puProducer, "PAGE_UPDATES", update.page, update));
        // followed by stream of page views
        final Iterator<PageView> pageViews = Generators.infinitePageViews().iterator();
        final RateLimiter rateLimiter = RateLimiter.create(eventsPerSecond);
        final long runUntil = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(runTimeInSeconds);
        final LongAdder count = new LongAdder();
        while (System.currentTimeMillis() < runUntil) {
            rateLimiter.acquire();
            if (pageViews.hasNext()) {
                PageView next = pageViews.next();
                send(pvProducer, "PAGE_VIEWS", next.user, next);
                count.increment();
            }
        }

        LOGGER.info("PUBLISHED " + count.longValue() + " events");
    }

    public static <K, V> void send(KafkaProducer<K, V> producer, String topic, K key, V value) {
        send(producer, topic, key, value, System.currentTimeMillis());
    }
    public static <K, V> void send(KafkaProducer<K, V> producer, String topic, K key, V value, long timestamp) {
        final ProducerRecord<K, V> record = new ProducerRecord<>(topic, null, timestamp, key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Exception while sending kafka message", exception);
            } else {
                LOGGER.info("{} successfully sent to {}, {}, {}", value, metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void close() {
        pvProducer.close();
        puProducer.close();
    }

    public static void main(String[] args) {
        try (final PageViewProducer producer = new PageViewProducer()) {
            producer.start();
        }
    }
}
