package icikic.kstreams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 */
public class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper;

    public JsonSerializer() {
        this(new ObjectMapper());
    }

    public JsonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writer().writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON", e);
        }
    }

    @Override
    public void close() {
    }
}
