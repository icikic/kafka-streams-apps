package icikic.kstreams;

import icikic.kstreams.config.KafkaStreamsConfig;
import icikic.kstreams.domain.Score;
import icikic.kstreams.service.ScoreService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.time.LocalDateTime.now;
import static java.time.ZoneId.systemDefault;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.streams.state.StreamsMetadata.NOT_AVAILABLE;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = KstreamsApplication.class,
        properties = {
                "kafka.streams.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
                "kafka.streams.scoresWindowSizeInSeconds=180",
                "kafka.streams.scoresWindowAdvanceInSeconds=60",
        })
@TestExecutionListeners(listeners = {
        DependencyInjectionTestExecutionListener.class,
        MovingAverageUsingStreamsDslTest.class})
@WebAppConfiguration
public class MovingAverageUsingStreamsDslTest extends AbstractTestExecutionListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(MovingAverageUsingStreamsDslTest.class);
    private static final String SCORES_TOPIC = "SCORES";
    private static final double EPSILON = 10e-6;

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, SCORES_TOPIC);

    @Autowired
    private ScoreService scoreService;
    @Autowired
    private KafkaStreamsConfig config;
    @Autowired
    private WebApplicationContext context;

    private MockMvc mvc;

    @Before
    public void setup() {
        this.mvc = webAppContextSetup(context).build();
    }

    @Test
    public void shouldReturnMovingAverage() throws Exception {
        final long windowDurationInMillis = config.getScoresWindowSizeInMillis();
        final long advanceByInMillis = config.getScoresWindowAdvanceInMillis();

        final Instant now = now().truncatedTo(MINUTES).plusSeconds(1).atZone(systemDefault()).toInstant();
        final long t0 = now.toEpochMilli() - windowDurationInMillis;
        final long t1 = t0 + advanceByInMillis;
        final long t2 = t1 + advanceByInMillis;
        final long t3 = t2 + advanceByInMillis;
        final long t4 = t3 + advanceByInMillis;
        final long t5 = t4 + advanceByInMillis;
        final long t6 = t5 + advanceByInMillis;

        final List<Score> scores = Arrays.asList(
                new Score(1, 1, 100, t0),
                new Score(2, 1, 0, t1),
                new Score(1, 2, 50, t2),
                new Score(2, 2, 50, t3));

        final Properties props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        props.setProperty(ACKS_CONFIG, "all");
        props.setProperty(LINGER_MS_CONFIG, "1");
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, "icikic.kstreams.serde.JsonSerializer");

        try (final KafkaProducer<Long, Score> producer = new KafkaProducer<>(props)) {
            scores.forEach(s -> {
                ProducerRecord<Long, Score> record = new ProducerRecord<>(SCORES_TOPIC,null, s.timestamp, s.playerId, s);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        throw new RuntimeException("Exception while sending kafka record", exception);
                    } else {
                        LOGGER.info("Successfully sent {} to {}, {}, {}", s, metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
            });

        }

        await().atMost(5, SECONDS).until(() -> !NOT_AVAILABLE.equals(
                scoreService.getKafkaStreams().allMetadataForStore(config.getAveragesStore())));

        await().atMost(5, HOURS).pollInterval(1, SECONDS).untilAsserted(() -> {
            // T0
            assertMovingAverageForPlayerAtTime(1, t0, 100);
            assertMovingAverageForPlayerAtTime(2, t0, 0);
            assertMovingAverageForAllPlayersAtTime(t0, 1);
            // T1
            assertMovingAverageForPlayerAtTime(1, t1, 100);
            assertMovingAverageForPlayerAtTime(2, t1, 0);
            assertMovingAverageForAllPlayersAtTime(t1, 2);
            // T2
            assertMovingAverageForPlayerAtTime(1, t2, 75);
            assertMovingAverageForPlayerAtTime(2, t2, 0);
            assertMovingAverageForAllPlayersAtTime(t2, 2);
            // T3
            assertMovingAverageForPlayerAtTime(1, t3, 50);
            assertMovingAverageForPlayerAtTime(2, t3, 25);
            assertMovingAverageForAllPlayersAtTime(t3, 2);
            // T4
            assertMovingAverageForPlayerAtTime(1, t4, 50);
            assertMovingAverageForPlayerAtTime(2, t4, 50);
            assertMovingAverageForAllPlayersAtTime(t4, 2);
            // T5
            assertMovingAverageForPlayerAtTime(1, t5, 0);
            assertMovingAverageForPlayerAtTime(2, t5, 50);
            assertMovingAverageForAllPlayersAtTime(t5, 1);
            // T6
            assertMovingAverageForPlayerAtTime(1, t6, 0);
            assertMovingAverageForPlayerAtTime(2, t6, 0);
            assertMovingAverageForAllPlayersAtTime(t6, 0);

        });

    }

    private void assertMovingAverageForAllPlayersAtTime(long time, int expected) throws Exception {
        mvc.perform(get("http://locahost:8080/scores/all?at={time}", time).accept(APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("$", hasSize(expected)))
                .andDo(result -> System.out.println(result.getResponse().getContentAsString()));
    }

    private void assertMovingAverageForPlayerAtTime(int player, long time, double expected) throws Exception {
        mvc.perform(get("http://locahost:8080/scores/{player}?at={time}", player, time).accept(APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("playerId", equalTo(player)))
                .andExpect(jsonPath("average", closeTo(expected, EPSILON)))
                .andDo(result -> System.out.println(result.getResponse().getContentAsString()));
    }

    @Override
    public void afterTestClass(TestContext testContext) {
        final ScoreService service = testContext.getApplicationContext().getBean(ScoreService.class);
        if (service != null) {
            service.stop();
        }
    }
}
