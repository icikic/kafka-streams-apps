package icikic.kstreams.pageview.topology;

import icikic.kstreams.config.KafkaConfig;
import icikic.kstreams.pageview.domain.*;
import icikic.kstreams.serde.JsonSerde;
import icikic.kstreams.serde.PriorityQueueSerde;
import icikic.kstreams.serde.WindowedSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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

@Configuration
@Import(KafkaConfig.class)
public class TopNPagesPerCountryStreamBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopNPagesPerCountryStreamBuilder.class);

    private final KafkaConfig kafkaConfig;

    public TopNPagesPerCountryStreamBuilder(final KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Bean
    public KafkaStreams topNPagesStream() {

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PageView> pageViews = builder.stream("PAGE_VIEWS", Consumed.with(Serdes.String(), new JsonSerde<>(PageView.class)));
        pageViews.print(Printed.toSysOut());


        SessionWindowedKStream<String, PageView> sessions = pageViews.groupByKey().windowedBy(SessionWindows.with(TimeUnit.MINUTES.toMillis(1)));
        KTable<Windowed<String>, Session> aggregate = sessions.aggregate(Session::new,
                (key, value, agg) -> {
                    //System.out.println("WTFFFFF " + key + "/" + value);
                    return agg.addPageView(value);
                },
                (aggKey, aggOne, aggTwo) -> {
                    //System.out.println("WTFFFFF " + aggOne + "/" + aggTwo);
                    return aggOne.merge(aggTwo);
                },
                Materialized.with(Serdes.String(), new JsonSerde<>(Session.class)));

  //      aggregate.toStream().print(Printed.toSysOut());
        KStream<String, Session> objectListKStream = aggregate.filter(new Predicate<Windowed<String>, Session>() {
            @Override
            public boolean test(Windowed<String> key, Session value) {
                //System.out.println("WTFFFFF " + key + "/" + value.pageViews.size());
                return value.pageViews.size() < 10;
            }
        }).toStream((key, value) -> key.key());
        KStream<String, PageView> stringPageViewKStream = objectListKStream
                .filter((k,v) -> v !=null)
                .flatMapValues(value -> value.pageViews);

        stringPageViewKStream.print(Printed.toSysOut());

        KTable<String, PageUpdate> pageUpdates = builder.table("PAGE_UPDATES", Consumed.with(Serdes.String(), new JsonSerde<>(PageUpdate.class)));

        KStream<String, PageViewWithContent> enriched = stringPageViewKStream
                .selectKey((k,v) -> v.page) // try global tables
                .leftJoin(pageUpdates, PageViewWithContent::new,
                Joined.with(Serdes.String(), new JsonSerde<>(PageView.class), new JsonSerde<>(PageUpdate.class)));
        KStream<String, PageViewWithContent> cutShort = enriched.filter((k, v) -> {
                    System.out.println(v);
                    if (v.update != null && v.update.content.length() > 20)
                        return true;
        //        return v.update.content.length() > 0;
                    else return false;
        }
            );
        final KTable<Windowed<PageView>, Long> viewCounts = cutShort
                .map((k, v) -> {
                    PageView pv = v.view;
                    return KeyValue.pair(new PageView(pv.page, "", pv.country), pv);
                })
                // count the clicks per hour, using tumbling windows with a size of one hour
                .groupByKey(Serialized.with(new JsonSerde<>(PageView.class), new JsonSerde<>(PageView.class)))
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
                .count();

        viewCounts.toStream().print(Printed.toSysOut());
        final Comparator<PageViewStats> comparator =
                (o1, o2) -> (int) (o2.count - o1.count);

        final Serde<Windowed<String>> windowedStringSerde = new WindowedSerde<>(Serdes.String());

        final KTable<Windowed<String>, PriorityQueue<PageViewStats>> allViewCounts = viewCounts
                .groupBy(
                        // the selector
                        (windowedArticle, count) -> {
                            // project on the industry field for key
                            Windowed<String> windowedCountry =
                                    new Windowed<>(windowedArticle.key().country, windowedArticle.window());
                            // add the page into the value
                            return new KeyValue<>(windowedCountry, new PageViewStats(windowedArticle.key(), count));
                        },
                        Serialized.with(new WindowedSerde<>(Serdes.String()), new JsonSerde<>(PageViewStats.class))
                ).aggregate(
                        // the initializer
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

                        Materialized.with(windowedStringSerde, new PriorityQueueSerde<>(comparator, new JsonSerde<>(PageViewStats.class)))
                );

        final int topN = 10;
        final KTable<Windowed<String>, String> topViewCounts = allViewCounts
                .mapValues(queue -> {
                    final StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < topN; i++) {
                        final PageViewStats record = queue.poll();
                        if (record == null) {
                            break;
                        }
                        sb.append(record.pageView.page);
                        sb.append("\n");
                    }
                    return sb.toString();
                });

        topViewCounts.toStream().print(Printed.toSysOut());
        topViewCounts.toStream().to("TOP_PAGES_PER_COUNTRY", Produced.with(windowedStringSerde, Serdes.String()));


        Topology topology = builder.build();
        LOGGER.info("{}", topology.describe());
        return new KafkaStreams(topology, kafkaConfig.getProperties());
    }


    public static StreamsBuilder testStream() {

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PageView> pageViews = builder.stream("PAGE_VIEWS", Consumed.with(Serdes.String(), new JsonSerde<>(PageView.class)));
        pageViews.print(Printed.toSysOut());
        return builder;
    }
}
