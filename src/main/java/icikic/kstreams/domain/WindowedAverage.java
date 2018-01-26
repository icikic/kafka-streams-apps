package icikic.kstreams.domain;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.Instant;

public class AverageScoreResponse {

    private final long playerId;
    private final Instant from;
    private final Instant to;
    private final BigDecimal average;

    public AverageScoreResponse(Long playerId, Instant from, Instant to, AverageScore average) {
        this.playerId = playerId;
        this.from = from;
        this.to = to;
        this.average = average.get();
    }

    public long getPlayerId() {
        return playerId;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC")
    public Instant getFrom() {
        return from;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC")
    public Instant getTo() {
        return to;
    }

    public BigDecimal getAverage() {
        return average;
    }
}
