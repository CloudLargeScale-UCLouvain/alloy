package ch.ethz.systems.strymon.ds2.flink.nexmark.generator;

import org.apache.flink.api.common.serialization.SerializationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerializationSchema<T> implements SerializationSchema<T> {
    private static final ObjectMapper mapper = new ObjectMapper();

    public JsonSerializationSchema() {
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Could not serialize object of type " + element.getClass().toString(), e);
        }
    }
}