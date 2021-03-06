package icikic.kstreams.movavg.controller;

import icikic.kstreams.config.KafkaConfig;
import icikic.kstreams.movavg.config.AverageScoreConfig;
import icikic.kstreams.movavg.domain.Average;
import icikic.kstreams.movavg.domain.WindowedAverage;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static icikic.kstreams.movavg.domain.Average.ZERO;
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
    private final AverageScoreConfig avgScoreConfig;
    private final RestTemplate httpClient;

    @Autowired
    public ScoreController(final KafkaConfig kafkaConfig,
                           final AverageScoreConfig avgScoreConfig,
                           @Qualifier("averageScoreStream") final KafkaStreams streams) {
        this.avgScoreConfig = avgScoreConfig;
        this.streams        = streams;
        this.hostInfo       = hostInfo(kafkaConfig);
        this.httpClient     = new RestTemplate();
    }

    @GetMapping(value = "/all", produces = APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<List<WindowedAverage>> getAverageScoreForAll(
            @RequestParam(value = "at", required = false) final Long atTime) {

        final long time = atTime == null ? currentTimeMillis() : atTime;
        final long start = startTime(time);
        final long end   = endTime(start);
        final List<WindowedAverage> result = fetchAveragesForAllPlayers(start, end);

        return ResponseEntity.ok(result);

    }

    @GetMapping(value = "/{player}", produces = APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<WindowedAverage> getAverageScoreForPlayer(
            @PathVariable("player") final Long player,
            @RequestParam(value = "at", required = false) final Long atTime) {

        final StreamsMetadata metadata = streams.metadataForKey(avgScoreConfig.getAverageScoreStoreName(), player, new LongSerializer());
        final HostInfo keyOwner = metadata.hostInfo();
        if (!keyOwner.equals(this.hostInfo)) {
            final UriComponents uri = UriComponentsBuilder.fromHttpUrl("http://{host}:{port}/scores")
                    .host(keyOwner.host())
                    .port(keyOwner.port())
                    .pathSegment(valueOf(player)).build();
            return httpClient.getForEntity(uri.toUri(), WindowedAverage.class);
        } else {
            final long time = atTime == null ? currentTimeMillis() : atTime;
            final long start = startTime(time);
            final long end   = endTime(start);
            final WindowedAverage average = fetchAverageForPlayer(player, start, end);

            return ResponseEntity.ok(average);
        }
    }

    private List<WindowedAverage> fetchAveragesForAllPlayers(final long from, final long to) {

        final Map<Long, LinkedList<WindowedAverage>> result = new HashMap<>();
        final ReadOnlyWindowStore<Long, Average> store = streams.store(avgScoreConfig.getAverageScoreStoreName(), QueryableStoreTypes.<Long, Average>windowStore());
        try (final KeyValueIterator<Windowed<Long>, Average> averages = store.fetch(0L, Long.MAX_VALUE, from, to)) {
            averages.forEachRemaining(kv -> {
                result.computeIfAbsent(kv.key.key(), k -> new LinkedList<>()).add(
                        new WindowedAverage(kv.key.key(), ofEpochMilli(kv.key.window().start()), ofEpochMilli(kv.key.window().end()), kv.value));
            });
        }
        return result.values().stream().map(LinkedList::getFirst).collect(Collectors.toList());
    }

    private WindowedAverage fetchAverageForPlayer(final Long key, long from, long to) {

        final LinkedList<WindowedAverage> result = new LinkedList<>();
        final ReadOnlyWindowStore<Long, Average> store = streams.store(avgScoreConfig.getAverageScoreStoreName(), QueryableStoreTypes.<Long, Average>windowStore());
        try (final WindowStoreIterator<Average> averages = store.fetch(key, from, to)) {
            averages.forEachRemaining(kv -> {
                result.add(new WindowedAverage(key, ofEpochMilli(kv.key), ofEpochMilli(kv.key).plusSeconds(avgScoreConfig.getWindowAdvanceInMillis()), kv.value));
            });
        }
        return result.isEmpty() ? new WindowedAverage(key, ofEpochMilli(from), ofEpochMilli(to), ZERO) : result.getFirst();
    }

    private static HostInfo hostInfo(final KafkaConfig kafkaConfig) {
        final String appServer = (String) kafkaConfig.getProperties().get(StreamsConfig.APPLICATION_SERVER_CONFIG);
        final String[] hostAndPort = appServer.split(":");
        return new HostInfo(hostAndPort[0], parseInt(hostAndPort[1]));
    }

    private long startTime(final long time) {
        if (time % (avgScoreConfig.getWindowAdvanceInMillis()) == 0) {
            return time - avgScoreConfig.getWindowSizeInMillis() + 1;
        }
        return time - avgScoreConfig.getWindowSizeInMillis();
    }

    private long endTime(final long start) {
        return start + avgScoreConfig.getWindowAdvanceInMillis() - 1; // store.fetch in inclusive on both boundaries, so we'll take 1ms from end boundary
    }
}
