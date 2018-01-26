package icikic.kstreams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import icikic.kstreams.domain.Average;
import icikic.kstreams.domain.Score;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.core.ResolvableType;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class JsonSerde<T> implements Serde<T> {

    private final JsonSerializer<T>   jsonSerializer;
    private final JsonDeserializer<T> jsonDeserializer;


    public JsonSerde() {
        this(null);
    }

    @SuppressWarnings("unchecked")
    public JsonSerde(Class<T> targetType) {
        final ObjectMapper objectMapper = new ObjectMapper();

        this.jsonSerializer = new JsonSerializer<>();
        if (targetType == null) {
            targetType = (Class<T>) ResolvableType.forClass(getClass()).getSuperType().resolveGeneric(0);
        }
        this.jsonDeserializer = new JsonDeserializer<>(targetType, objectMapper);
    }

    public JsonSerde(JsonSerializer<T> jsonSerializer, JsonDeserializer<T> jsonDeserializer) {
        requireNonNull(jsonSerializer, "jsonSerializer cannot be null");
        requireNonNull(jsonDeserializer, "jsonDeserializer cannot be null");
        this.jsonSerializer   = jsonSerializer;
        this.jsonDeserializer = jsonDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.jsonSerializer.configure(configs, isKey);
        this.jsonDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.jsonSerializer.close();
        this.jsonDeserializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return this.jsonSerializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this.jsonDeserializer;
    }

    public static class ScoreSerde extends JsonSerde<Score> {
    }

    public static class AverageScoreSerde extends JsonSerde<Average> {
    }
}
