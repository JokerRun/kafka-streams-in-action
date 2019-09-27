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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStreamsYellingApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsYellingApp.class);

    public static void main(String[] args) throws Exception {
        //启动线程，生成异步数据
        MockDataProducer.produceRandomTextDataForHelloWorld();
        //主线程休眠10秒，等待模拟数据完成导入
        Thread.sleep(5000);
        //构建Topology (ProccessorFlow)
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> srcStream = streamsBuilder.stream("hello-world-input", Consumed.with(stringSerde, stringSerde));
        srcStream.mapValues(String::toUpperCase)
                .peek((k,v)-> System.out.println("====key:"+k+"====value===="+v+""))
                .to("hello-word-output",Produced.with(stringSerde,stringSerde));
        //==============封装KafkaStream所需配置==============
        Properties properties = new Properties();
        //指定Kafka服务器地址。
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //设置应用名(可用于区分group)
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "hello-world");
        StreamsConfig streamsConfig = new StreamsConfig(properties);

        //==============执行流处理==============
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        kafkaStreams.start();
        //主线程休眠10秒,查看stream线程输出情况。
        Thread.sleep(5000);


        kafkaStreams.close();
        MockDataProducer.shutdown();

    }
}