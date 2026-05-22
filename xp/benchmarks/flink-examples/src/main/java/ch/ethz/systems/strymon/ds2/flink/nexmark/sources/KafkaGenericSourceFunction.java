package ch.ethz.systems.strymon.ds2.flink.nexmark.sources;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.flink.configuration.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.Properties;

import java.time.Duration;
import org.apache.kafka.common.TopicPartition;

public class KafkaGenericSourceFunction<T> extends RichParallelSourceFunction<T> {
    private volatile boolean running = true;
    private transient KafkaConsumer<String, String> consumer;
    private transient ObjectMapper objectMapper;
    private Class<T> typeClass; // To hold the class type for deserialization
    private String bootstrapServer;
    private String topic;
    private String groupId;
    public KafkaGenericSourceFunction(Class<T> typeClass, String bootstrapServer, String topic, String groupId) {
        this.typeClass = typeClass;
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.groupId = groupId;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", this.bootstrapServer);
        props.setProperty("group.id", this.groupId); // add specific kafka/timestamp command
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.setProperty("auto.offset.reset", "latest"); // add specific kafka/timestamp command

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(this.topic)); // subscribe to your topic
        
        // Seek to the end of each partition
        consumer.poll(Duration.ZERO); // This is necessary to ensure consumer is assigned partitions before seeking
        for (TopicPartition partition : consumer.assignment()) {
            consumer.seekToEnd(Collections.singleton(partition));
        }
        
        objectMapper = new ObjectMapper(); // Initialize ObjectMapper here
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String json = record.value();
                T obj = objectMapper.readValue(json, typeClass); // Deserialize JSON to Auction
                ctx.collect(obj);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (consumer != null) {
            consumer.close();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (consumer != null) {
            consumer.close();
        }
    }
}