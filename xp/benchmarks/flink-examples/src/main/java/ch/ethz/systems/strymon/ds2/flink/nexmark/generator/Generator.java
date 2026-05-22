package ch.ethz.systems.strymon.ds2.flink.nexmark.generator;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.BidSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.AuctionSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.PersonSourceFunction;
public class Generator {
    public static void main(String[] args) throws Exception {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        StreamExecutionEnvironment env;
        String remoteAddress = params.get("jobmanager.rpc.address");
        if (remoteAddress == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        } else {
            env = StreamExecutionEnvironment.createRemoteEnvironment(remoteAddress.split(":")[0], Integer.parseInt(remoteAddress.split(":")[1]), "flink-examples/target/flink-examples-1.0-SNAPSHOT.jar");
        }
        String kafkaProducerAddress = params.get("kafkaProducerAddress", "kafka-edge1:9092,localhost:9094");

        final int bidRate = params.getInt("bidRate", 0);
        final int auctionRate = params.getInt("auctionRate", 0);
        final int personRate = params.getInt("personRate", 0);


        Integer parallelism = params.getInt("parallelism", 1);
        env.setParallelism(parallelism);
        //env.disableOperatorChaining();
        
        KafkaSink<Bid> bidSink = KafkaSink.<Bid>builder()
            .setBootstrapServers(kafkaProducerAddress)
            .setProperty("properties.retries", "5")
            .setProperty("metadata.max.age.ms", "3600000")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("bid")
                .setKeySerializationSchema(new SerializationSchema<Bid>() {
                    @Override
                    public byte[] serialize(Bid bid) {
                        return Long.toString(bid.auction).getBytes(StandardCharsets.UTF_8);
                    }
                })
                .setValueSerializationSchema(new JsonSerializationSchema<>())
                .build())
            .build();    
    
        // Define KafkaSink for Auction
        KafkaSink<Auction> auctionSink = KafkaSink.<Auction>builder()
            .setBootstrapServers(kafkaProducerAddress)
            .setProperty("properties.retries", "5")
            .setProperty("metadata.max.age.ms", "3600000")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("auction")
                .setKeySerializationSchema(new SerializationSchema<Auction>() {
                    @Override
                    public byte[] serialize(Auction auction) {
                        return Long.toString(auction.id).getBytes(StandardCharsets.UTF_8);
                    }
                })                
                .setValueSerializationSchema(new JsonSerializationSchema<>())
                .build())
            .build();
        KafkaSink<Person> personSink = KafkaSink.<Person>builder()
            .setBootstrapServers(kafkaProducerAddress)
            .setProperty("properties.retries", "5")
            .setProperty("metadata.max.age.ms", "3600000")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("person")
                .setKeySerializationSchema(new SerializationSchema<Person>() {
                    @Override
                    public byte[] serialize(Person person) {
                        return Long.toString(person.id).getBytes(StandardCharsets.UTF_8);
                    }
                })                
                .setValueSerializationSchema(new JsonSerializationSchema<>())
                .build())
            .build();    

        if (bidRate > 0) {
            DataStream<Bid> bidStream = env.addSource(new BidSourceFunction(bidRate))
                .setParallelism(params.getInt("p-bid-source", 1))
                .name("Bids Source")
                .uid("Bids-Source")
                .slotSharingGroup("bid");

            bidStream.sinkTo(bidSink);
        }

        if (auctionRate > 0) {
            DataStream<Auction> auctionStream = env.addSource(new AuctionSourceFunction(auctionRate))
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1))
                .slotSharingGroup("auction");                  

            auctionStream.sinkTo(auctionSink);
        }

        if (personRate > 0) {
            DataStream<Person> personStream = env.addSource(new PersonSourceFunction(personRate))
                .name("Persons source")
                .setParallelism(params.getInt("p-person-source", 1))
                .slotSharingGroup("person");                  
            personStream.sinkTo(personSink);            
        } 



        env.execute("Nexmark Kafka generator");
    }
}
