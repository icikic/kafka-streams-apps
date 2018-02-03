package icikic.kstreams.movavg.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

import static java.math.BigDecimal.valueOf;
import static java.math.MathContext.DECIMAL32;

/**
 */
public class Average {
    public static final Average ZERO = new Average(0, BigDecimal.ZERO);

    public final long count;
    public final BigDecimal avg;

    public Average() {
        this(0, BigDecimal.ZERO);
    }

    public Average(@JsonProperty("count") final long count,
                   @JsonProperty("avg") final BigDecimal avg) {

        this.count = count;
        this.avg   = avg;
    }

    public Average update(Score score) {
        final long       newCount = count + 1;
        // https://en.wikipedia.org/wiki/Moving_average#Cumulative_moving_average
        final BigDecimal newAvg   = avg.add((valueOf(score.score).subtract(avg)).divide(valueOf(newCount), DECIMAL32), DECIMAL32);
        return new Average(newCount, newAvg);
    }

    public BigDecimal get() {
        return avg;
    }

    @Override
    public String toString() {
        return get().toPlainString();
    }

}
