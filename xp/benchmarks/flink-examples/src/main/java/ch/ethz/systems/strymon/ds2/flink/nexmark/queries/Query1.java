/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.ethz.systems.strymon.ds2.flink.nexmark.queries;

import ch.ethz.systems.strymon.ds2.flink.nexmark.generator.JsonSerializationSchema;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sinks.DummyLatencyCountingSink;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.BidSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.GenericJsonDeserializationSchema;

import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query1 {

    private static final Logger logger  = LoggerFactory.getLogger(Query1.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int srcRate = params.getInt("srcRate", 100000);
        final float exchangeRate = params.getFloat("exchange-rate", 0.82F);

        StreamExecutionEnvironment env;
        String remoteAddress = params.get("jobmanager.rpc.address");
        if (remoteAddress == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        } else {
            env = StreamExecutionEnvironment.createRemoteEnvironment(remoteAddress.split(":")[0], Integer.parseInt(remoteAddress.split(":")[1]), "flink-examples/target/flink-examples-1.0-SNAPSHOT.jar");
        }
        String kafkaAddress = params.get("kafkaAddress", "kafka-edge1:9092,localhost:9094");
        String kafkaSinkAddress = params.get("kafkaSinkAddress", "");
        String kafkaSinkTopic = params.get("kafkaSinkTopic", "sink");
        String kafkaStartingOffset = params.get("kafkaStartOffset", "latest");
        OffsetsInitializer offsetsInitializer;
        if (kafkaStartingOffset == "latest") {
            offsetsInitializer = OffsetsInitializer.latest();
        } else {
            offsetsInitializer = OffsetsInitializer.earliest();
        }
        final int parallelism = params.getInt("parallelism", 1);

        final Integer fetchMaxWaitMs = params.getInt("fetchMaxWaitMs", 500);
        final Integer fetchMinBytes = params.getInt("fetchMinBytes", 1);
        // fetch.max.bytes default: 55Mb max.message.bytes default: 1Mb // see if necessary
        
        // We keep operator chaining
        //env.disableOperatorChaining();
        env.setParallelism(parallelism);
        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);
        
        DataStream<Bid> bids;
        if (kafkaAddress.isEmpty()) {
            bids = env.addSource(new BidSourceFunction(srcRate), "bid", TypeInformation.of(Bid.class))
                    .setParallelism(params.getInt("p-source", 1))
                    .name("Bids Source")
                    .uid("Bids-Source");
        } else {
            KafkaSource<Bid> source = KafkaSource.<Bid>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics("bid")
                .setGroupId("bid")
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(new GenericJsonDeserializationSchema<Bid>(Bid.class))
                .setProperty("fetch.max.wait.ms", fetchMaxWaitMs.toString())
                .setProperty("fetch.min.bytes", fetchMinBytes.toString())
                .build();
            bids = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "bid kafka");
        }
        // SELECT auction, DOLTOEUR(price), bidder, datetime
        DataStream<Tuple4<Long, Long, Long, Long>> mapped  = bids.map(new MapFunction<Bid, Tuple4<Long, Long, Long, Long>>() {
            @Override
            public Tuple4<Long, Long, Long, Long> map(Bid bid) throws Exception {
                return new Tuple4<>(bid.auction, dollarToEuro(bid.price, exchangeRate), bid.bidder, bid.dateTime);
            }
        }).setParallelism(params.getInt("p-map", 1))
                .name("Mapper")
                .uid("Mapper");

        if (kafkaSinkAddress.isEmpty()) {
            GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
            mapped.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                    .setParallelism(params.getInt("p-map", 1))
            .name("Latency Sink")
            .uid("Latency-Sink");
        } else {
            KafkaSink<Tuple4<Long, Long, Long, Long>> sink = KafkaSink.<Tuple4<Long, Long, Long, Long>>builder()
            .setBootstrapServers(kafkaSinkAddress)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(kafkaSinkTopic)
                .setValueSerializationSchema(new JsonSerializationSchema<Tuple4<Long, Long, Long, Long>>())
                .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
            mapped.sinkTo(sink);
        }

        // execute program
        env.execute("Nexmark Query1");
    }

    private static long dollarToEuro(long dollarPrice, float rate) {
        return (long) (rate*dollarPrice);
    }

}