package icikic.kstreams.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;

public class ListSerde<T> implements Serde<List<T>> {
    private final Serde<List<T>> inner;

    public ListSerde(final Serde<T> inner) {
        this.inner = Serdes.serdeFrom(new ListSerializer<>(inner.serializer()),
                new ListDeserializer<>(inner.deserializer()));
    }

    @Override
    public Serializer<List<T>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<List<T>> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }
}
