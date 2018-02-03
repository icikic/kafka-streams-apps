package icikic.kstreams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 */
public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper;

    private Class<T> cls;

    public JsonDeserializer(final Class<T> cls) {
        this(cls, new ObjectMapper());
    }

    public JsonDeserializer(final Class<T> cls, final ObjectMapper objectMapper) {
        this.cls = cls;
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readerFor(cls).readValue(data);
        } catch (Exception e) {
            throw new SerializationException("Could not parse JSON", e);
        }
    }

    @Override
    public void close() {
    }
}
