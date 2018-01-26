package icikic.kstreams.domain;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

import static java.math.RoundingMode.HALF_EVEN;

public class WindowedAverage {

    private final long playerId;
    private final Instant from;
    private final Instant to;
    private final BigDecimal average;

    public WindowedAverage(Long playerId, Instant from, Instant to, Average average) {
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
        return average.setScale(4, HALF_EVEN);
    }
}
