package icikic.kstreams.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

import static java.math.BigDecimal.valueOf;
import static java.math.MathContext.DECIMAL32;

/**
 */
public class AverageScore {
    public final long count;
    public final BigDecimal sum;

    public AverageScore() {
        this(0, BigDecimal.ZERO);
    }

    public AverageScore(@JsonProperty("count") final long count,
                        @JsonProperty("sum") final BigDecimal sum) {

        this.count = count;
        this.sum   = sum;
    }

    public AverageScore update(Score score) {
        final BigDecimal newSum   = sum.add(valueOf(score.score), DECIMAL32);
        final long       newCount = count + 1;
        return new AverageScore(newCount, newSum);
    }

    public BigDecimal get() {
        return sum.divide(valueOf(count), DECIMAL32);
    }

    @Override
    public String toString() {
        return get().toPlainString();
    }

}
