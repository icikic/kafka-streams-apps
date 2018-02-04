package icikic.kstreams.pageview.topology;

import icikic.kstreams.config.KafkaConfig;
import icikic.kstreams.pageview.config.TopPagesConfig;
import icikic.kstreams.pageview.domain.PageUpdate;
import icikic.kstreams.pageview.domain.PageView;
import icikic.kstreams.pageview.domain.PageViewStats;
import icikic.kstreams.pageview.domain.PageViewWithContent;
import icikic.kstreams.serde.JsonSerde;
import icikic.kstreams.serde.ListSerde;
import icikic.kstreams.serde.PriorityQueueSerde;
import icikic.kstreams.serde.WindowedSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.serialization.Serdes.String;

@Configuration
@Import(KafkaConfig.class)
public class TopNPagesPerCountryStreamBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopNPagesPerCountryStreamBuilder.class);

    private final KafkaConfig kafkaConfig;
    private final TopPagesConfig topNConfig;

    public TopNPagesPerCountryStreamBuilder(final KafkaConfig kafkaConfig, final TopPagesConfig topNConfig) {
        this.kafkaConfig = kafkaConfig;
        this.topNConfig = topNConfig;
    }

    @Bean
    public KafkaStreams topPagesStream() {
        LOGGER.info("Top N pages config: {}", topNConfig);
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<PageView> pageViewSerde = new JsonSerde<>(PageView.class);
        final Serde<PageUpdate> pageUpdateSerde = new JsonSerde<>(PageUpdate.class);
        final Serde<List<PageView>> listSerde = new ListSerde<>(pageViewSerde);

        final KStream<String, PageView> pageViews = builder.stream(topNConfig.getPageViewTopic(), Consumed.with(String(), pageViewSerde));

        // exclude burst of views, probably robots
        long sizeMs = TimeUnit.SECONDS.toMillis(1);
        TimeWindowedKStream<String, PageView> pvByUserWindowed = pageViews.groupByKey()
                .windowedBy(TimeWindows.of(sizeMs).until(sizeMs));

        final KTable<Windowed<String>, List<PageView>> sessionsTable = pvByUserWindowed
                .aggregate(ArrayList::new,
                        (key, value, agg) -> {
                            agg.add(value);
                            return agg;
                        },
                        Materialized.with(String(), listSerde));

        final KStream<String, List<PageView>> sessionStream = sessionsTable
                .filter((key, session) -> session.size() < topNConfig.getRequestsPerSecondThreshold())
                .toStream((key, value) -> key.key());

        final KStream<String, PageView> withAllowedRate = sessionStream
                .filter((k,v) -> v != null)
                .flatMapValues(value -> value);

        final KTable<String, PageUpdate> pageUpdates = builder.table(topNConfig.getPageUpdateTopic(), Consumed.with(String(), pageUpdateSerde));

        final KStream<String, PageViewWithContent> enriched = withAllowedRate
                .selectKey((k,v) -> v.page) // try global tables
                .leftJoin(pageUpdates, PageViewWithContent::new,
                Joined.with(String(), pageViewSerde, pageUpdateSerde));

        // filter out page view of pages with content length < A
        final KStream<String, PageViewWithContent> filteredByContentLength = enriched
                .filter((k, v) -> v.update != null && v.update.content.length() > topNConfig.getPageSizeThreshold());

        final long countingTimeBucket = TimeUnit.SECONDS.toMillis(topNConfig.getTimeBucketInSeconds());
        // group by page and country
        final KTable<Windowed<PageView>, Long> viewCounts = filteredByContentLength
                .map((k, v) -> {
                    final PageView pv = v.view;
                    return KeyValue.pair(new PageView(pv.page, "", pv.country), pv);
                })
                // count the clicks per preconfigured time bucket
                .groupByKey(Serialized.with(pageViewSerde, pageViewSerde))
                .windowedBy(TimeWindows.of(countingTimeBucket))
                .count();

        //viewCounts.toStream().print(Printed.toSysOut());
        final Comparator<PageViewStats> comparator =
                (o1, o2) -> (int) (o2.count - o1.count);

        final Serde<Windowed<String>> windowedStringSerde = new WindowedSerde<>(String());
        final JsonSerde<PageViewStats> pageViewStatsSerde = new JsonSerde<>(PageViewStats.class);

        final KTable<Windowed<String>, PriorityQueue<PageViewStats>> allViewCounts = viewCounts
                .groupBy((windowedPageView, count) -> {
                            final Windowed<String> windowedCountry = new Windowed<>(windowedPageView.key().country, windowedPageView.window());
                            // add the page into the value
                            return new KeyValue<>(windowedCountry, new PageViewStats(windowedPageView.key(), count));
                        },
                        Serialized.with(new WindowedSerde<>(String()), pageViewStatsSerde))
                .aggregate(
                        () -> new PriorityQueue<>(comparator),

                        // the "add" aggregator
                        (windowedCountry, record, queue) -> {
                            queue.add(record);
                            return queue;
                        },

                        // the "remove" aggregator
                        (windowedCountry, record, queue) -> {
                            queue.remove(record);
                            return queue;
                        },

                        Materialized.with(windowedStringSerde, new PriorityQueueSerde<>(comparator, pageViewStatsSerde))
                );

        final KTable<Windowed<String>, String> topViewCounts = allViewCounts
                .mapValues(queue -> {
                    final StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < topNConfig.getN(); i++) {
                        final PageViewStats record = queue.poll();
                        if (record == null) {
                            break;
                        }
                        sb.append("('").append(record.pageView.page).append("',").append(record.count).append(")").append("\n");
                    }
                    return sb.toString();
                });

        //topViewCounts.toStream().print(Printed.toSysOut());
        topViewCounts.toStream().to(topNConfig.getTopPagesPerCountyTopic(), Produced.with(windowedStringSerde, String()));

        final Topology topology = builder.build();
        LOGGER.info("{}", topology.describe());
        return new KafkaStreams(topology, kafkaConfig.getProperties());
    }
}
