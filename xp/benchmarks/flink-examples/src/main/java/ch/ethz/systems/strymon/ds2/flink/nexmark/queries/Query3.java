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
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.AuctionSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.BidSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.GenericJsonDeserializationSchema;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.KafkaGenericSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;

public class Query3 {

    private static final Logger logger  = LoggerFactory.getLogger(Query3.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int auctionSrcRate = params.getInt("auction-srcRate", 20000);

        final int personSrcRate = params.getInt("person-srcRate", 10000);

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
        
        //env.disableOperatorChaining();
        env.setParallelism(parallelism);

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);       

        DataStream<Auction> auctions;
        if (kafkaAddress.isEmpty()) {
            auctions = env.addSource(new AuctionSourceFunction(auctionSrcRate), "auction", TypeInformation.of(Auction.class))
                    .setParallelism(params.getInt("p-auction-source", 1))
                    .name("Auctions Source")
                    .uid("Auctions-Source");
        } else {
            KafkaSource<Auction> source = KafkaSource.<Auction>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics("auction")
                .setGroupId("auction")
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(new GenericJsonDeserializationSchema<Auction>(Auction.class))
                .setProperty("fetch.max.wait.ms", fetchMaxWaitMs.toString())
                .setProperty("fetch.min.bytes", fetchMinBytes.toString())
                .build();
                auctions = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "auctions kafka");
        }

        DataStream<Person> persons;
        if (kafkaAddress.isEmpty()) {
            persons = env.addSource(new PersonSourceFunction(personSrcRate), "person", TypeInformation.of(Person.class))
                    .setParallelism(params.getInt("p-person-source", 1))
                    .name("Persons Source")
                    .uid("Persons-Source");
        } else {
            KafkaSource<Person> source = KafkaSource.<Person>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics("person")
                .setGroupId("person")
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(new GenericJsonDeserializationSchema<Person>(Person.class))
                .setProperty("fetch.max.wait.ms", fetchMaxWaitMs.toString())
                .setProperty("fetch.min.bytes", fetchMinBytes.toString())
                .build();
                persons = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "persons kafka");
        }        

        persons = persons.filter(new FilterFunction<Person>() {
                    @Override
                    public boolean filter(Person person) throws Exception {
                        return (person.state.equals("OR") || person.state.equals("ID") || person.state.equals("CA"));
                    }
                })
                .setParallelism(params.getInt("p-person-source", 1));

        // SELECT Istream(P.name, P.city, P.state, A.id)
        // FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
        // WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA')
        //TODO: set reinterpret here?
      KeyedStream<Auction, Long> keyedAuctions =
              auctions.keyBy(new KeySelector<Auction, Long>() {
                 @Override
                 public Long getKey(Auction auction) throws Exception {
                    return auction.seller;
                 }
              });

      KeyedStream<Person, Long> keyedPersons =
                persons.keyBy(new KeySelector<Person, Long>() {
                    @Override
                    public Long getKey(Person person) throws Exception {
                        return person.id;
                    }
                });
      
      DataStream<Tuple4<String, String, String, Long>> joined = keyedAuctions.connect(keyedPersons)
              .flatMap(new JoinPersonsWithAuctions()).name("Incremental join").setParallelism(params.getInt("p-join", 1));

        if (kafkaSinkAddress.isEmpty()) {              
            GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
            joined.transform("Sink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                    .setParallelism(params.getInt("p-join", 1));
        } else {
            KafkaSink<Tuple4<String, String, String, Long>> sink = KafkaSink.<Tuple4<String, String, String, Long>>builder()
            .setBootstrapServers(kafkaSinkAddress)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(kafkaSinkTopic)
                .setValueSerializationSchema(new JsonSerializationSchema<Tuple4<String, String, String, Long>>())
                .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
            joined.sinkTo(sink);            
        }
        // execute program
        env.execute("Nexmark Query3");
    }

    private static final class JoinPersonsWithAuctions extends RichCoFlatMapFunction<Auction, Person, Tuple4<String, String, String, Long>> {

        // person state: id, <name, city, state>
        private HashMap<Long, Tuple3<String, String, String>> personMap = new HashMap<>();

        // auction state: seller, List<id>
        private HashMap<Long, HashSet<Long>> auctionMap = new HashMap<>();

        @Override
        public void flatMap1(Auction auction, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
            // check if auction has a match in the person state
            if (personMap.containsKey(auction.seller)) {
                // emit and don't store
                Tuple3<String, String, String> match = personMap.get(auction.seller);
                out.collect(new Tuple4<>(match.f0, match.f1, match.f2, auction.id));
            }
            else {
                // we need to store this auction for future matches
                if (auctionMap.containsKey(auction.seller)) {
                    HashSet<Long> ids = auctionMap.get(auction.seller);
                    ids.add(auction.id);
                    auctionMap.put(auction.seller, ids);
                }
                else {
                    HashSet<Long> ids = new HashSet<>();
                    ids.add(auction.id);
                    auctionMap.put(auction.seller, ids);
                }
            }
        }

        @Override
        public void flatMap2(Person person, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
            // store person in state
            personMap.put(person.id, new Tuple3<>(person.name, person.city, person.state));

            // check if person has a match in the auction state
            if (auctionMap.containsKey(person.id)) {
                // output all matches and remove
                HashSet<Long> auctionIds = auctionMap.remove(person.id);
                for (Long auctionId : auctionIds) {
                    out.collect(new Tuple4<>(person.name, person.city, person.state, auctionId));
                }
            }
        }
    }

}
