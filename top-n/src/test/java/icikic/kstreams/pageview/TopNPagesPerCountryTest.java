package icikic.kstreams.pageview;

import com.google.common.collect.Sets;
import icikic.kstreams.pageview.domain.PageUpdate;
import icikic.kstreams.pageview.domain.PageView;
import icikic.kstreams.serde.JsonSerializer;
import icikic.kstreams.service.KStreamsLifecycle;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AbstractTestExecutionListener;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import scala.collection.immutable.Page;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static icikic.kstreams.pageview.producer.PageViewProducer.send;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = TopNPagesPerCountryApplication.class,
        properties = {
                "kafka.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"
        })
@TestExecutionListeners(listeners = {
        DependencyInjectionTestExecutionListener.class,
        TopNPagesPerCountryTest.class})
public class TopNPagesPerCountryTest extends AbstractTestExecutionListener  {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopNPagesPerCountryTest.class);

    private static final String TOP_PAGES_PER_COUNTRY_TOPIC = "TOP_PAGES_PER_COUNTRY";
    private static final String PAGE_UPDATES_TOPIC = "PAGE_UPDATES";
    private static final String PAGE_VIEWS_TOPIC = "PAGE_VIEWS";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, PAGE_UPDATES_TOPIC, PAGE_VIEWS_TOPIC, TOP_PAGES_PER_COUNTRY_TOPIC);

    @Autowired
    private KStreamsLifecycle service;

/*
    @Before
    public void createTopics() throws Exception {
        final Set<String> requiredTopics = Sets.newHashSet(PAGE_UPDATES_TOPIC, PAGE_VIEWS_TOPIC, TOP_PAGES_PER_COUNTRY_TOPIC);
        final AdminClient admin = KafkaAdminClient.create(Collections.singletonMap("bootstrap.servers", embeddedKafka.getBrokersAsString()));
        final ListTopicsResult result = admin.listTopics(new ListTopicsOptions().listInternal(false));
        result.names().get(5, TimeUnit.SECONDS).forEach(requiredTopics::remove);
        Set<NewTopic> newTopics = requiredTopics.stream().map(name -> new NewTopic(name, 1, (short) 1)).collect(Collectors.toSet());
        final CreateTopicsResult topics = admin.createTopics(newTopics);
        topics.all().get(5, TimeUnit.SECONDS);
        final ListTopicsResult after = admin.listTopics(new ListTopicsOptions().listInternal(false));
        LOGGER.info("Existing topics: {}", after.names().get());
    }

    @After
    public void deleteTopics() throws Exception {

        final AdminClient admin = KafkaAdminClient.create(Collections.singletonMap("bootstrap.servers", embeddedKafka.getBrokersAsString()));
        final DeleteTopicsResult topics = admin.deleteTopics(Sets.newHashSet(PAGE_UPDATES_TOPIC, PAGE_VIEWS_TOPIC, TOP_PAGES_PER_COUNTRY_TOPIC));
        topics.all().get(5, TimeUnit.SECONDS);
        LOGGER.info("All topics deleted");
    }
*/

    @Test
    public void topNPagesPerCountry() throws Exception {
        final Map<String, List<String>> expected = publishTestEvents();

        final KafkaConsumer<Windowed<String>, String> consumer = topPagesConsumer();

        final Map<String, List<String>> actual = new HashMap<>();
        final long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(100);

        boolean done = false;
        while (System.currentTimeMillis() < timeout && !done) {
            final ConsumerRecords<Windowed<String>, String> records = consumer.poll(1000);
            records.forEach(record -> actual.put(record.key().key(), Arrays.asList(record.value().split("\n"))));
            done = actual.equals(expected);
        }
        assertThat(actual, equalTo(expected));
    }

    private KafkaConsumer<Windowed<String>, String> topPagesConsumer() {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,  "top-pages-test-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final Deserializer<Windowed<String>> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
        final KafkaConsumer<Windowed<String>, String> consumer = new KafkaConsumer<>(properties,
                windowedDeserializer,
                Serdes.String().deserializer());
        consumer.subscribe(singleton(TOP_PAGES_PER_COUNTRY_TOPIC));
        return consumer;
    }

    private Map<String, List<String>> publishTestEvents() {
        final String[] users = {"kenny", "monica", "adam", "susie", "alex", "cameron"};
        final String[] countries = {"US", "GB", "ES", "NZ", "CA", "DK"};
        final String[] pages = {"/home", "/checkout", "/view-cart", "/add-to-cart", "/search", "/product"};
        final Random random = new Random();
        final Map<String, List<String>> expected = new HashMap<>();

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());

        // first, publish all page content
        try (final KafkaProducer<String, PageUpdate> puProducer = new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<PageUpdate>())) {
            Arrays.stream(pages).forEach(page -> {
                final String author  = users[random.nextInt(6)];
                final String content = RandomStringUtils.randomAscii(50);
                send(puProducer, PAGE_UPDATES_TOPIC, page, new PageUpdate(page, content, author));
            });
            // override one page to bellow threshold content length
            send(puProducer, PAGE_UPDATES_TOPIC, "/checkout", new PageUpdate("/checkout", "Checkout", users[random.nextInt(6)]));
        }

        try (final KafkaProducer<String, PageView> pvProducer = new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<PageView>())) {

            IntStream.range(0, 6).forEach(idx -> {
                final String country = countries[idx];
                final String user = users[idx];
                send(pvProducer, PAGE_VIEWS_TOPIC, user, new PageView("/home", user, country));
                send(pvProducer, PAGE_VIEWS_TOPIC, user, new PageView("/search", user, country));
                send(pvProducer, PAGE_VIEWS_TOPIC, user, new PageView("/product", user, country));
                send(pvProducer, PAGE_VIEWS_TOPIC, user, new PageView("/search", user, country));
                send(pvProducer, PAGE_VIEWS_TOPIC, user, new PageView("/product", user, country));
                send(pvProducer, PAGE_VIEWS_TOPIC, user, new PageView("/product", user, country));
                send(pvProducer, PAGE_VIEWS_TOPIC, user, new PageView("/add-to-cart", user, country));
                send(pvProducer, PAGE_VIEWS_TOPIC, user, new PageView("/checkout", user, country));

                // /checkout page content should be filtered out due to content size
                expected.put(country, Arrays.asList("/product", "/search", "/add-to-cart", "/home"));

            });

            // send page view with unknown page
            send(pvProducer, PAGE_VIEWS_TOPIC, "invalid", new PageView("/unknown", "invalid", "UK"));
            // simulate robot
            IntStream.range(0, 50).forEach(i ->
                send(pvProducer, PAGE_VIEWS_TOPIC, "crawler", new PageView("/home", "crawler", "UK"))
            );
        }
        return expected;
    }

    @Override
    public void afterTestClass(TestContext testContext) {
        final Map<String, KStreamsLifecycle> service = testContext.getApplicationContext().getBeansOfType(KStreamsLifecycle.class);
        service.values().forEach(KStreamsLifecycle::stop);
    }
}
