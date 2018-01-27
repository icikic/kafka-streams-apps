package icikic.kstreams.topology;

import icikic.kstreams.config.KafkaStreamsConfig;
import icikic.kstreams.domain.Average;
import icikic.kstreams.domain.Score;
import icikic.kstreams.serde.JsonSerde;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.emptyMap;

@Configuration
@Profile("processor-api")
public class KafkaStreamsProcessorApiBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsProcessorApiBuilder.class);

    private KafkaStreamsConfig config;

    public KafkaStreamsProcessorApiBuilder(final KafkaStreamsConfig config) {
        this.config = config;
    }

    @Bean(name = "StreamsUsingProcessorAPI")
    public KafkaStreams buildKafkaStreams() {
        final Topology topology = new Topology();
        final TimeWindows windows = TimeWindows.of(config.getScoresWindowSizeInMillis()).advanceBy(config.getScoresWindowAdvanceInMillis());
        final StoreBuilder<WindowStore<Long, Average>> storeBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(config.getAveragesStore(), windows.maintainMs(), windows.segments, windows.size(), false),
                Serdes.Long(),
                new JsonSerde<>(Average.class)
        );

        topology.addSource("scores-source", new LongDeserializer(), new JsonSerde.ScoreSerde().deserializer(), config.getScoresTopic())
                .addProcessor("scores-processor", () -> new MovingAverageProcessor(windows, config.getAveragesStore()), "scores-source")
                .addStateStore(storeBuilder)
                .connectProcessorAndStateStores("scores-processor", config.getAveragesStore());

        LOGGER.debug("{}", topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, config.getProperties());
        streams.setStateListener((newState, oldState) -> LOGGER.info("{} -> {}", oldState, newState));
        return streams;
    }

    public static class MovingAverageProcessor extends AbstractProcessor<Long, Score> {

        private final String storeName;
        private final TimeWindows windows;
        private WindowStore<Long, Average> windowStore;

        MovingAverageProcessor(final TimeWindows windows, final String storeName) {
            this.storeName = storeName;
            this.windows = windows;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            this.windowStore = (WindowStore<Long, Average>) context().getStateStore(storeName);
        }

        @Override
        public void process(final Long key, final Score value) {
            if (key == null) {
                return;
            }
            final long timestamp = context().timestamp();
            final Map<Long, TimeWindow> matchedWindows = windows.windowsFor(timestamp);

            long timeFrom = Long.MAX_VALUE;
            long timeTo = Long.MIN_VALUE;

            // use range query on window store for efficient reads
            for (long windowStartMs : matchedWindows.keySet()) {
                timeFrom = windowStartMs < timeFrom ? windowStartMs : timeFrom;
                timeTo = windowStartMs > timeTo ? windowStartMs : timeTo;
            }

            try (final WindowStoreIterator<Average> iter = windowStore.fetch(key, timeFrom, timeTo)) {

                while (iter.hasNext()) {
                    final KeyValue<Long, Average> keyValue = iter.next();
                    final TimeWindow window = matchedWindows.get(keyValue.key);

                    if (window != null) {
                        Average oldAvg = keyValue.value;

                        if (oldAvg == null) {
                            oldAvg = new Average();
                        }
                        Average newAvg = oldAvg.update(value);
                        // update the store with new average
                        windowStore.put(key, newAvg, window.start());
                        //context().forward(new Windowed<>(key, window), newAvg);
                        matchedWindows.remove(keyValue.key);
                    }
                }
            }

            // create a new window for the rest of unmatched windows that do not exist yet
            for (Map.Entry<Long, TimeWindow> entry : matchedWindows.entrySet()) {
                Average oldAvg = new Average();
                Average newAvg = oldAvg.update(value);
                windowStore.put(key, newAvg, entry.getKey());
                //context().forward(new Windowed<>(key, entry.getValue()), newAvg);
            }
        }
    }
}
