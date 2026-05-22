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

import ch.ethz.systems.strymon.ds2.flink.nexmark.sinks.DummyLatencyCountingSink;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.AuctionSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.BidSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.GenericJsonDeserializationSchema;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.KafkaGenericSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.PersonSourceFunction;

import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query2 {

    private static final Logger logger  = LoggerFactory.getLogger(Query2.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int srcRate = params.getInt("srcRate", 100000);
        StreamExecutionEnvironment env;
        String remoteAddress = params.get("jobmanager.rpc.address");
        if (remoteAddress == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        } else {
            env = StreamExecutionEnvironment.createRemoteEnvironment(remoteAddress.split(":")[0], Integer.parseInt(remoteAddress.split(":")[1]), "flink-examples/target/flink-examples-1.0-SNAPSHOT.jar");
        }
        String kafkaAddress = params.get("kafkaAddress", "kafka-edge1:9092,localhost:9094");
        final int parallelism = params.getInt("parallelism", 1);

        final Integer fetchMaxWaitMs = params.getInt("fetchMaxWaitMs", 500);
        final Integer fetchMinBytes = params.getInt("fetchMinBytes", 1);
        // fetch.max.bytes default: 55Mb max.message.bytes default: 1Mb // see if necessary
        
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
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new GenericJsonDeserializationSchema<Bid>(Bid.class))
                .setProperty("fetch.max.wait.ms", fetchMaxWaitMs.toString())
                .setProperty("fetch.min.bytes", fetchMinBytes.toString())
                .build();
            bids = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "bid kafka");
        }

        // SELECT Rstream(auction, price)
        // FROM Bid [NOW]
        // WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;

        DataStream<Tuple2<Long, Long>> converted = bids
                .flatMap(new FlatMapFunction<Bid, Tuple2<Long, Long>>() {
                    @Override
                    public void flatMap(Bid bid, Collector<Tuple2<Long, Long>> out) throws Exception {
                        if(bid.auction % 1007 == 0 || bid.auction % 1020 == 0 || bid.auction % 2001 == 0 || bid.auction % 2019 == 0 || bid.auction % 2087 == 0) {
                            out.collect(new Tuple2<>(bid.auction, bid.price));
                        }
                    }
                }).setParallelism(params.getInt("p-flatMap", 1));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        converted.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-flatMap", 1));

        // execute program
        env.execute("Nexmark Query2");
    }

}