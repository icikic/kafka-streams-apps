package icikic.kstreams.config;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class ConfigUtils {
    public static Properties kafkaConfig(Map<String, String> env) {
        return env.entrySet().stream().filter(e -> e.getKey().startsWith("KAFKA_")).map(e -> {
            final String newKey = e.getKey()
                    .replace("KAFKA_", "")
                    .replace("_", ".")
                    .toLowerCase();
            return new AbstractMap.SimpleEntry<>(newKey, e.getValue());
        }).collect(toProperties(AbstractMap.SimpleEntry::getKey,
                AbstractMap.SimpleEntry::getValue));

    }

    private static <T, K, U> Collector<T, ?, Properties> toProperties(final Function<? super T, ? extends K> keyMapper,
                                                                      final Function<? super T, ? extends U> valueMapper) {
        return Collectors.toMap(keyMapper,
                valueMapper,
                throwingMerger(),
                Properties::new);
    }

    private static <T>BinaryOperator<T> throwingMerger() {
        return (u, v) -> { throw new IllegalStateException(String.format("Duplicate keys %s", u)); };
    }
}
