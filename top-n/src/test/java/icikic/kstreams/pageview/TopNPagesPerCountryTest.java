package icikic.kstreams.pageview;

import com.google.common.collect.Sets;
import icikic.kstreams.pageview.domain.PageUpdate;
import icikic.kstreams.pageview.domain.PageView;
import icikic.kstreams.pageview.service.PageViewService;
import icikic.kstreams.pageview.topology.TopNPagesPerCountryStreamBuilder;
import icikic.kstreams.serde.JsonSerializer;
import icikic.kstreams.service.KStreamsLifecycle;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AbstractTestExecutionListener;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static icikic.kstreams.pageview.producer.PageViewProducer.send;
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
    //public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true);

    @Autowired
    private KStreamsLifecycle service;

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

    // test 1: happy path
    // test 2: exclude robots
    // test 3: exclude content < 40
    // test 4: test session window


    @Test
    public void happyPath() throws IOException, InterruptedException {
//        StreamsBuilder builder = TopNPagesPerCountryStreamBuilder.testStream();
//        Properties p = new Properties();
//        p.setProperty("bootstrap.servers", embeddedKafka.getBrokersAsString());
//        p.setProperty("application.id", "pageview");
//        p.setProperty("client.id", "pageview=client");
//        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), p);
//        kafkaStreams.cleanUp();
//        kafkaStreams.start();
        Thread.sleep(10000);
        Map<String, List<String>> expected = produceEventsForTestCase1();
//        final Properties consumerProperties = new Properties();
//        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
//        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
//                "test-consumer");
//        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        final KafkaConsumer<String, PageView> consumer = new KafkaConsumer<>(consumerProperties,
//                Serdes.String().deserializer(),
//                new JsonDeserializer<>(PageView.class));
//
//        consumer.subscribe(Collections.singleton(PAGE_VIEWS_TOPIC));
//        final Map<String, List<String>> received = new HashMap<>();
//        final long timeout = System.currentTimeMillis() + 60000L;
//        boolean done = false;
//        while (System.currentTimeMillis() < timeout && !done) {
//            final ConsumerRecords<String, PageView> records = consumer.poll(1000);
//            records.forEach(System.out::println);
//
//            //done = received.equals(expected);
//        }
       // consumeOutput(expected);
        System.in.read();

    }

    private static void consumeOutput(Map<String, List<String>> expected) {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
                "top-articles-lambda-example-consumer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final Deserializer<Windowed<String>> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
        final KafkaConsumer<Windowed<String>, String> consumer = new KafkaConsumer<>(consumerProperties,
                windowedDeserializer,
                Serdes.String().deserializer());

        consumer.subscribe(Collections.singleton(TOP_PAGES_PER_COUNTRY_TOPIC));
        final Map<String, List<String>> received = new HashMap<>();
        final long timeout = System.currentTimeMillis() + 120000L;
        boolean done = false;
        while (System.currentTimeMillis() < timeout && !done) {
            final ConsumerRecords<Windowed<String>, String> records = consumer.poll(1000);
            records.forEach(record ->
                    received.put(record.key().key(), Arrays.asList(record.value().split("\n"))));


            //done = received.equals(expected);
        }

        System.out.println(received);
        assertThat(received, equalTo(expected));
    }

    private Map<String, List<String>> produceEventsForTestCase1() {
        final String[] users = {"kenny", "monica", "adam", "susie", "alex", "cameron"};//, "phil", "sam", "lauren", "joseph"};
        final String[] countries = {"US", "GB", "ES", "NZ", "CA", "DK"};
        final String[] pages = {"/index", "/news", "/contact", "/about", "/search"};
        final Random random = new Random();
        final Properties props = new Properties();
        Map<String, List<String>> expected = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        try (final KafkaProducer<String, PageView> producer = new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<PageView>());
             final KafkaProducer<String, PageUpdate> prod = new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<PageUpdate>())) {
            Arrays.stream(pages).forEach(page -> {
                final String user = users[random.nextInt(6)];
                final String content = "aaaaaaaaaabbbbbbbbbccccccccccdddddddddeeeeeeeee";
                send(prod, PAGE_UPDATES_TOPIC, page, new PageUpdate(page, content, user));
            });
            IntStream.range(0, 6).forEach(idx -> {
                final String country = countries[idx];
                final String user = users[idx];
                send(producer, PAGE_VIEWS_TOPIC, user, new PageView("/index", user, country));
                send(producer, PAGE_VIEWS_TOPIC, user, new PageView("/contact", user, country));
                send(producer, PAGE_VIEWS_TOPIC, user, new PageView("/contact", user, country));
                send(producer, PAGE_VIEWS_TOPIC, user, new PageView("/search", user, country));
                send(producer, PAGE_VIEWS_TOPIC, user, new PageView("/search", user, country));
                send(producer, PAGE_VIEWS_TOPIC, user, new PageView("/search", user, country));
                expected.put(country, Arrays.asList("/search", "/contact", "/index"));
            });
            prod.flush();
            producer.flush();

        }
        return expected;


    }
    @Override
    public void afterTestClass(TestContext testContext) {
        final Map<String, KStreamsLifecycle> service = testContext.getApplicationContext().getBeansOfType(KStreamsLifecycle.class);
        service.values().forEach(KStreamsLifecycle::stop);
    }
}
