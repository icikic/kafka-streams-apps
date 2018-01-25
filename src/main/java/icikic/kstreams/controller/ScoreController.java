package icikic.kstreams.controller;

import icikic.kstreams.config.KafkaStreamsProperties;
import icikic.kstreams.domain.AverageScore;
import icikic.kstreams.domain.AverageScoreResponse;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.valueOf;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8_VALUE;

@RestController
@RequestMapping("/scores")
public class ScoreController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreController.class);

    private final KafkaStreams streams;
    private final HostInfo hostInfo;
    private final KafkaStreamsProperties properties;
    private final RestTemplate httpClient = new RestTemplate();

    @Autowired
    public ScoreController(final KafkaStreamsProperties properties, final KafkaStreams streams) {
        this.properties = properties;
        this.streams = streams;
        this.hostInfo = new HostInfo(properties.getApplicationHost(), properties.getApplicationPort());
    }

    @GetMapping(value = "/all", produces = APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<List<AverageScoreResponse>> getAverageScoreForAll(
            @RequestParam(value = "time", required = false) final Long time) {


        final Long t = time == null ? Instant.now().toEpochMilli() : time;
        final Long from = t - 1000*properties.getAverageWindowDurationInSeconds();
        final Long realTo = from + properties.getAverageWindowAdvanceInSeconds()*1000;
        final ReadOnlyWindowStore<Long, AverageScore> averageScoreStore = streams.store(properties.getAverageScoreTopic(), QueryableStoreTypes.<Long, AverageScore>windowStore());
        final KeyValueIterator<Windowed<Long>, AverageScore> averageScore = averageScoreStore.fetch(0L, Long.MAX_VALUE, from, realTo);
        LinkedList<AverageScoreResponse> result = new LinkedList<>();
        while (averageScore.hasNext()) {
            KeyValue<Windowed<Long>, AverageScore> next = averageScore.next();
            result.add(new AverageScoreResponse(next.key.key(), Instant.ofEpochMilli(next.key.window().start()), Instant.ofEpochMilli(next.key.window().end()), next.value));
        }

        return ResponseEntity.ok(result);

    }

    @GetMapping(value = "/{player}", produces = APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<AverageScoreResponse> getAverageScoreForPlayer(
            @PathVariable("player") final Long player,
            @RequestParam(value = "time", required = false) final Long time) {

        final StreamsMetadata metadata = streams.metadataForKey(properties.getAverageScoreTopic(), player, new LongSerializer());
        final HostInfo keyOwner = metadata.hostInfo();
        LOGGER.info("FOUND: {}", keyOwner);
        LOGGER.info("GIVEN: {}", hostInfo);
        if (!keyOwner.equals(this.hostInfo)) {
            final UriComponents uri = UriComponentsBuilder.fromHttpUrl("http://{host}:{port}/scores")
                    .host(keyOwner.host())
                    .port(keyOwner.port())
                    .pathSegment(valueOf(player)).build();
            return httpClient.getForEntity(uri.toUri(), AverageScoreResponse.class);
        } else {
            final Long t = time == null ? Instant.now().toEpochMilli() : time;
            final Long from = t - 1000*properties.getAverageWindowDurationInSeconds();
            final AverageScoreResponse average = fetchAverageForPlayer(player, properties.getAverageScoreTopic(), from, t);

            return ResponseEntity.ok(average);
        }
    }

    private AverageScoreResponse fetchAverageForPlayer(final Long key, final String storeName, long from, long to) {

        final ReadOnlyWindowStore<Long, AverageScore> averageScoreStore = streams.store(storeName, QueryableStoreTypes.<Long, AverageScore>windowStore());
        final WindowStoreIterator<AverageScore> averageScore = averageScoreStore.fetch(key, from, to);
        LinkedList<AverageScoreResponse> result = new LinkedList<>();
        while (averageScore.hasNext()) {
            KeyValue<Long, AverageScore> next = averageScore.next();
            result.add(new AverageScoreResponse(key, Instant.ofEpochMilli(next.key), Instant.ofEpochMilli(next.key).plusSeconds(properties.getAverageWindowDurationInSeconds()), next.value));
        }
        return result.isEmpty() ? new AverageScoreResponse(key, Instant.ofEpochMilli(from), Instant.ofEpochMilli(to), new AverageScore(0, BigDecimal.ZERO)) : result.getFirst();
    }

}
