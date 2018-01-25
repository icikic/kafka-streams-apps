package icikic.kstreams.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class Score {
    public final long playerId;
    public final long gameId;
    public final int score;
    public final long timestamp;

    public Score(@JsonProperty("playerId") final long playerId,
                 @JsonProperty("gameId") final long gameId,
                 @JsonProperty("score") final int score,
                 @JsonProperty("timestamp") final long timestamp) {

        this.playerId  = playerId;
        this.gameId    = gameId;
        this.score     = score;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Score{" +
                "playerId=" + playerId +
                ", gameId=" + gameId +
                ", score=" + score +
                ", timestamp=" + timestamp +
                '}';
    }
}
