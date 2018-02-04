/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package icikic.kstreams.serde;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ListDeserializer<T> implements Deserializer<List<T>> {

    private final Deserializer<T> valueDeserializer;
    private final Supplier<List<T>> listSupplier;

    public ListDeserializer(final Deserializer<T> valueDeserializer) {
        this(valueDeserializer, ArrayList::new);
    }

    public ListDeserializer(final Deserializer<T> valueDeserializer, Supplier<List<T>> listSupplier) {
        this.valueDeserializer = valueDeserializer;
        this.listSupplier = listSupplier;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public List<T> deserialize(String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        final List<T> list = listSupplier.get();
        final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            final int records = dataInputStream.readInt();
            for (int i = 0; i < records; i++) {
                final byte[] valueBytes = new byte[dataInputStream.readInt()];
                dataInputStream.read(valueBytes);
                list.add(valueDeserializer.deserialize(s, valueBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize List", e);
        }
        return list;
    }

    @Override
    public void close() {

    }
}
