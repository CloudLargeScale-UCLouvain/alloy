package ch.ethz.systems.strymon.ds2.flink.nexmark.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class GenericJsonDeserializationSchema<T> implements KafkaRecordDeserializationSchema<T> {
    private final Class<T> targetType;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public GenericJsonDeserializationSchema(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<T> out) throws IOException {
        if (record.value() != null) {
            T obj = objectMapper.readValue(record.value(), targetType);
            out.collect(obj);
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(targetType);
    }
}