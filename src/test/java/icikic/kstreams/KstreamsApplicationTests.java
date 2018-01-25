package icikic.kstreams;

import icikic.kstreams.domain.Score;
import icikic.kstreams.service.ScoreService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.context.WebApplicationContext;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = KstreamsApplication.class,
                properties = {
                        "kafka.streams.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"
                })
@WebAppConfiguration
public class KstreamsApplicationTests {

	@Test
	public void contextLoads() {
	}

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, "SCORES");

	@Autowired
	private ScoreService scoreService;
	@Autowired
	private WebApplicationContext context;
	private MockMvc mvc;

	@Before
	public void setup() throws Exception {
		this.mvc = webAppContextSetup(context).build();
	}


	@Test
	public void testMe() throws Exception {
		Instant now = Instant.now();
		List<Score> scores = Arrays.asList(
			new Score(1, 1, 100, now.toEpochMilli()),
			new Score(2, 1, 0, now.toEpochMilli()),
			new Score(1, 2, 50, now.toEpochMilli() + 1),
			new Score(2, 2, 50, now.toEpochMilli() + 1));
		Properties props = new Properties();

		props.setProperty("bootstrap.servers", embeddedKafka.getBrokersAsString());
		props.setProperty("acks", "all");
		props.setProperty("retries", "0");
		props.setProperty("batch.size", "1000");
		props.setProperty("linger.ms", "1");
		props.setProperty("buffer.memory", "22554432");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		props.setProperty("value.serializer", "icikic.kstreams.serde.JsonSerializer");
		final KafkaProducer<Long, Score> producer = new KafkaProducer<>(props);

		scores.forEach(s -> {
			ProducerRecord<Long, Score> record = new ProducerRecord<>("SCORES", s.playerId, s);
			producer.send(record, (metadata, exception) -> {
				if (exception != null) {
					System.out.println("Exception while sending kafka message." + exception);
					throw new RuntimeException("Exception while sending kafka message.", exception);
				} else {
					System.out.println("Successfully sent to " +
							metadata.topic() + "," + metadata.partition() + "," + metadata.offset());
				}
			});
		});

        producer.close(1, TimeUnit.SECONDS);

        for (int i = 0; i < 5; i++) {
            try {

                MvcResult mvcResult = mvc.perform(get("http://locahost:8080/scores/1").accept(MediaType.APPLICATION_JSON_UTF8))
                        .andExpect(status().isOk())
                        .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                        .andExpect(jsonPath("playerId", equalTo(1)))
                        .andExpect(jsonPath("average", equalTo(75)))
                        .andReturn();
                System.out.println(mvcResult.getResponse().getContentAsString());
                break;
            } catch (Throwable e) {
                // ignore
                sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
        }

        scoreService.stop();
    }

    @Test
    public void testMeAgain() throws Exception {
        Instant now = Instant.now();
        List<Score> scores = Arrays.asList(
                new Score(1, 1, 100, now.toEpochMilli()),
                new Score(2, 1, 0, now.toEpochMilli()),
                new Score(1, 2, 50, now.toEpochMilli() + 1),
                new Score(2, 2, 50, now.toEpochMilli() + 1));
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", embeddedKafka.getBrokersAsString());
        props.setProperty("acks", "all");
        props.setProperty("retries", "0");
        props.setProperty("batch.size", "1000");
        props.setProperty("linger.ms", "1");
        props.setProperty("buffer.memory", "22554432");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.setProperty("value.serializer", "icikic.kstreams.serde.JsonSerializer");
        final KafkaProducer<Long, Score> producer = new KafkaProducer<>(props);

        scores.forEach(s -> {
            ProducerRecord<Long, Score> record = new ProducerRecord<>("SCORES", s.playerId, s);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("Exception while sending kafka message." + exception);
                    throw new RuntimeException("Exception while sending kafka message.", exception);
                } else {
                    System.out.println("Successfully sent to " +
                            metadata.topic() + "," + metadata.partition() + "," + metadata.offset());
                }
            });
        });

        producer.close(1, TimeUnit.SECONDS);
        Thread.sleep(10000);

        Instant to = Instant.now().minusSeconds(2000);

        for (int i = 0; i < 1; i++) {
            try {

                MvcResult mvcResult = mvc.perform(get("http://locahost:8080/scores/1?time=100000").accept(MediaType.APPLICATION_JSON_UTF8))
                        .andExpect(status().isOk())
                        .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                        //.andExpect(jsonPath("playerId", equalTo(1)))
                        .andReturn();
                System.out.println(mvcResult.getResponse().getContentAsString());
                break;
            } catch (Throwable e) {
                // ignore
                sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
        }

        scoreService.stop();
    }

}
