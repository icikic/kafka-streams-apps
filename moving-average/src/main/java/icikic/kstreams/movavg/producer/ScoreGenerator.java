package icikic.kstreams.movavg.producer;

import icikic.kstreams.movavg.domain.Score;

import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.function.Supplier;

import static java.lang.System.currentTimeMillis;

/**
 */
public class ScoreGenerator implements Supplier<Score> {

    private final PrimitiveIterator.OfInt scores;
    private final PrimitiveIterator.OfInt players;
    private final PrimitiveIterator.OfLong games;

    ScoreGenerator() {
        this.scores = new Random().ints(0, 100).iterator();
        this.players = new Random().ints(0, 1000).iterator();
        this.games = new Random().longs().iterator();
    }

    @Override
    public Score get() {
        return new Score(players.next(), games.next(), scores.next(), currentTimeMillis());
    }
}
