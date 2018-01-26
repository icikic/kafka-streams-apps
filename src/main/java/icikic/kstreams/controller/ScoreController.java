package icikic.kstreams.controller;

import icikic.kstreams.config.KafkaStreamsConfig;
import icikic.kstreams.domain.Average;
import icikic.kstreams.domain.WindowedAverage;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
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

import java.util.LinkedList;
import java.util.List;

import static icikic.kstreams.domain.Average.ZERO;
import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.time.Instant.ofEpochMilli;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8_VALUE;

@RestController
@RequestMapping("/scores")
public class ScoreController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreController.class);

    private final KafkaStreams streams;
    private final HostInfo hostInfo;
    private final KafkaStreamsConfig config;
    private final RestTemplate httpClient;

    @Autowired
    public ScoreController(final KafkaStreamsConfig config, final KafkaStreams streams) {
        this.config     = config;
        this.streams    = streams;
        this.hostInfo   = hostInfo(config);
        this.httpClient = new RestTemplate();
    }

    @GetMapping(value = "/all", produces = APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<List<WindowedAverage>> getAverageScoreForAll(
            @RequestParam(value = "at", required = false) final Long atTime) {

        final long asOf  = atTime == null ? currentTimeMillis() : atTime;
        final long start = asOf - config.getScoresWindowSizeInSeconds() * 1000;
        final long end   = start + config.getScoresWindowAdvanceInSeconds() * 1000;
        final LinkedList<WindowedAverage> result = fetchAveragesForAllPlayers(start, end);

        return ResponseEntity.ok(result);

    }

    @GetMapping(value = "/{player}", produces = APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<WindowedAverage> getAverageScoreForPlayer(
            @PathVariable("player") final Long player,
            @RequestParam(value = "at", required = false) final Long atTime) {

        final StreamsMetadata metadata = streams.metadataForKey(config.getAveragesStore(), player, new LongSerializer());
        final HostInfo keyOwner = metadata.hostInfo();
        if (!keyOwner.equals(this.hostInfo)) {
            final UriComponents uri = UriComponentsBuilder.fromHttpUrl("http://{host}:{port}/scores")
                    .host(keyOwner.host())
                    .port(keyOwner.port())
                    .pathSegment(valueOf(player)).build();
            return httpClient.getForEntity(uri.toUri(), WindowedAverage.class);
        } else {
            final long asOf  = atTime == null ? currentTimeMillis() : atTime;
            final long start = asOf - config.getScoresWindowSizeInSeconds() * 1000;
            final long end   = start + config.getScoresWindowAdvanceInSeconds() * 1000;
            final WindowedAverage average = fetchAverageForPlayer(player, start, end);

            return ResponseEntity.ok(average);
        }
    }

    private LinkedList<WindowedAverage> fetchAveragesForAllPlayers(final long from, final long to) {

        final ReadOnlyWindowStore<Long, Average> store = streams.store(config.getAveragesStore(), QueryableStoreTypes.<Long, Average>windowStore());
        final KeyValueIterator<Windowed<Long>, Average> averages = store.fetch(0L, Long.MAX_VALUE, from, to);
        final LinkedList<WindowedAverage> result = new LinkedList<>();
        while (averages.hasNext()) {
            final KeyValue<Windowed<Long>, Average> next = averages.next();
            result.add(new WindowedAverage(next.key.key(), ofEpochMilli(next.key.window().start()), ofEpochMilli(next.key.window().end()), next.value));
        }
        return result;
    }

    private WindowedAverage fetchAverageForPlayer(final Long key, long from, long to) {

        final ReadOnlyWindowStore<Long, Average> store = streams.store(config.getAveragesStore(), QueryableStoreTypes.<Long, Average>windowStore());
        final WindowStoreIterator<Average> averages = store.fetch(key, from, to);
        final LinkedList<WindowedAverage> result = new LinkedList<>();
        while (averages.hasNext()) {
            final KeyValue<Long, Average> next = averages.next();
            result.add(new WindowedAverage(key, ofEpochMilli(next.key), ofEpochMilli(next.key).plusSeconds(config.getScoresWindowSizeInSeconds()), next.value));
        }
        return result.isEmpty() ? new WindowedAverage(key, ofEpochMilli(from), ofEpochMilli(to), ZERO) : result.getFirst();
    }

    private static HostInfo hostInfo(final KafkaStreamsConfig kafkaConfig) {
        final String appServer = (String) kafkaConfig.getProperties().get(StreamsConfig.APPLICATION_SERVER_CONFIG);
        final String[] hostAndPort = appServer.split(":");
        return new HostInfo(hostAndPort[0], parseInt(hostAndPort[1]));
    }

}
