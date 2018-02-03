package icikic.kstreams.pageview.topology;

import icikic.kstreams.config.KafkaConfig;
import icikic.kstreams.pageview.config.TopPagesConfig;
import icikic.kstreams.pageview.domain.*;
import icikic.kstreams.serde.JsonSerde;
import icikic.kstreams.serde.PriorityQueueSerde;
import icikic.kstreams.serde.WindowedSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.Comparator;
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

        final StreamsBuilder builder = new StreamsBuilder();
        final JsonSerde<PageView> pageViewSerde = new JsonSerde<>(PageView.class);
        final JsonSerde<PageUpdate> pageUpdateSerde = new JsonSerde<>(PageUpdate.class);
        final JsonSerde<Session> sessionSerde = new JsonSerde<>(Session.class);

        final KStream<String, PageView> pageViews = builder.stream(topNConfig.getPageViewTopic(), Consumed.with(String(), pageViewSerde));

        // exclude burst of views, probably robots
        final SessionWindowedKStream<String, PageView> sessions = pageViews.groupByKey()
                .windowedBy(SessionWindows.with(TimeUnit.SECONDS.toMillis(1)));

        final KTable<Windowed<String>, Session> sessionsTable = sessions
                .aggregate(Session::new,
                    (key, value, agg) -> agg.addPageView(value),
                    (aggKey, aggOne, aggTwo) -> aggOne.merge(aggTwo),
                    Materialized.with(String(), sessionSerde));

        final KStream<String, Session> sessionStream = sessionsTable
                .filter((key, session) -> session.size() < topNConfig.getRequestsPerSecondThreshold())
                .toStream((key, value) -> key.key());

        final KStream<String, PageView> rapidViewsExcluded = sessionStream
                .filter((k,v) -> v != null)
                .flatMapValues(value -> value.pageViews);

        final KTable<String, PageUpdate> pageUpdates = builder.table(topNConfig.getPageUpdateTopic(), Consumed.with(String(), pageUpdateSerde));

        final KStream<String, PageViewWithContent> enriched = rapidViewsExcluded
                .selectKey((k,v) -> v.page) // try global tables
                .leftJoin(pageUpdates, PageViewWithContent::new,
                Joined.with(String(), pageViewSerde, pageUpdateSerde));

        // filter out page view of pages with content length < A
        final KStream<String, PageViewWithContent> filteredByContentLength = enriched
                .filter((k, v) -> v.update != null && v.update.content.length() > topNConfig.getPageSizeThreshold());

        // group by page and country
        final KTable<Windowed<PageView>, Long> viewCounts = filteredByContentLength
                .map((k, v) -> {
                    final PageView pv = v.view;
                    return KeyValue.pair(new PageView(pv.page, "", pv.country), pv);
                })
                // count the clicks per preconfigured time bucket
                .groupByKey(Serialized.with(pageViewSerde, pageViewSerde))
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(topNConfig.getTimeBucketInSeconds())))
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
                        sb.append(record.pageView.page);
                        sb.append("\n");
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
