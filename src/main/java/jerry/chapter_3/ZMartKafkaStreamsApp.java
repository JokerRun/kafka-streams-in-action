/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jerry.chapter_3;

import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.Purchase;
import bbejeck.model.PurchasePattern;
import bbejeck.model.RewardAccumulator;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ZMartKafkaStreamsApp {

    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsApp.class);

    public static void main(String[] args) throws Exception {
        MockDataProducer.producePurchaseData();
        Thread.sleep(3000);
        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        //==============basic==============
        KStream<String, Purchase> masked = streamsBuilder.stream("transactions", Consumed.with(stringSerde, purchaseSerde)).mapValues(p -> Purchase.builder(p).maskCreditCard().build());
        masked.to("purchases",Produced.with(stringSerde,purchaseSerde));
        masked.mapValues(p->PurchasePattern.builder(p).build()).to("parrern",Produced.with(stringSerde,purchasePatternSerde));
        masked.mapValues(p->RewardAccumulator.builder(p).build()).to("reward",Produced.with(stringSerde,rewardAccumulatorSerde));

        //==============Advance==============
        masked.filter((k,v)->v.getPrice()>5.00).selectKey((k,v)->v.getPurchaseDate().getTime()).to("filtered-purchases",Produced.with(longSerde,purchaseSerde));
        KStream<String, Purchase>[] branchs = masked.branch(((key, value) -> value.getDepartment().equalsIgnoreCase("coffee")), ((key, value) -> value.getDepartment().equalsIgnoreCase("electronics")));
        branchs[0].to("coffee",Produced.with(stringSerde,purchaseSerde));
        branchs[1].to("electronics",Produced.with(stringSerde,purchaseSerde));


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), getProperties());
        kafkaStreams.start();
        Thread.sleep(5000);

        //关闭进程
        MockDataProducer.shutdown();
        kafkaStreams.close();

    }




    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

}
