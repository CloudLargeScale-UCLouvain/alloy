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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class Query11String {

    private static final Logger logger  = LoggerFactory.getLogger(Query11String.class);

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
        final boolean alloy = params.getBoolean("alloy", false);

        final Integer fetchMaxWaitMs = params.getInt("fetchMaxWaitMs", 500);
        final Integer fetchMinBytes = params.getInt("fetchMinBytes", 1);
        // fetch.max.bytes default: 55Mb max.message.bytes default: 1Mb // see if necessary
        
        //env.disableOperatorChaining();
        env.setParallelism(parallelism);

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);        

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStream<Bid> bids;
        if (kafkaAddress.isEmpty()) {
            bids = env.addSource(new BidSourceFunction(srcRate))
            .setParallelism(params.getInt("p-bid-source", 1))
            .assignTimestampsAndWatermarks(new BidTimestampAssigner());
        } else {
            KafkaSource<Bid> source = KafkaSource.<Bid>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics("bid")
                .setGroupId("bid")
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(new GenericJsonDeserializationSchema<Bid>(Bid.class))
                .setProperty("fetch.max.wait.ms", fetchMaxWaitMs.toString())
                .setProperty("fetch.min.bytes", fetchMinBytes.toString())
                .setProperty("metadata.max.age.ms", "3600000")
                .build();
            bids = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "bid kafka").assignTimestampsAndWatermarks(new BidTimestampAssigner());
        }

        DataStream<Tuple2<Long, Long>> windowed;
        if (alloy) {
            windowed = DataStreamUtils.reinterpretAsKeyedStream(bids, new KeySelector<Bid, String>() {
                @Override
                public String getKey(Bid b) throws Exception {
                    return Long.toString(b.bidder);
                }
            }).window(EventTimeSessionWindows.withGap(Time.seconds(10)))
            .trigger(new MaxLogEventsTrigger())
            .aggregate(new CountBidsPerSession()).setParallelism(params.getInt("p-window", 1))
            .name("Session Window");
        } else {
            windowed = bids.keyBy(new KeySelector<Bid, String>() {
                @Override
                public String getKey(Bid b) throws Exception {
                    return Long.toString(b.bidder);
                }
            }).window(EventTimeSessionWindows.withGap(Time.seconds(10)))
            .trigger(new MaxLogEventsTrigger())
            .aggregate(new CountBidsPerSession()).setParallelism(params.getInt("p-window", 1))
            .name("Session Window");
        }

        if (kafkaSinkAddress.isEmpty()) {
            GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
            windowed.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-window", 1));
        } else {
            KafkaSink<Tuple2<Long, Long>> sink = KafkaSink.<Tuple2<Long, Long>>builder()
            .setBootstrapServers(kafkaSinkAddress)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(kafkaSinkTopic)
                .setValueSerializationSchema(new JsonSerializationSchema<Tuple2<Long, Long>>())
                .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
            windowed.sinkTo(sink);             
        }


        // execute program
        env.execute("Nexmark Query11");
    }

    private static final class MaxLogEventsTrigger extends Trigger<Bid, TimeWindow> {

        private final long maxEvents = 100000L;

        private final ReducingStateDescriptor<Long> stateDesc =
                new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

        @Override
        public TriggerResult onElement(Bid element, long timestamp, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
            ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
            count.add(1L);
            if (count.get() >= maxEvents) {
                count.clear();
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
            ctx.mergePartitionedState(stateDesc);
        }

        @Override
        public void clear(TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(stateDesc).clear();
        }

        private static class Sum implements ReduceFunction<Long> {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }

        }
    }

    private static final class CountBidsPerSession implements AggregateFunction<Bid, Long, Tuple2<Long, Long>> {

        private long bidId = 0L;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Bid bid, Long accumulator) {
            bidId = bid.auction;
            return accumulator + 1;
        }

        @Override
        public Tuple2<Long, Long> getResult(Long accumulator) {
            return new Tuple2<>(bidId, accumulator);
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static final class BidTimestampAssigner implements AssignerWithPeriodicWatermarks<Bid> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Bid element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

}