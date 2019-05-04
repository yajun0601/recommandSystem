package com.yajun.kafkaStream;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

public class Application {
    public static void main(String[] args){

        String input = "abcd";
        String output = "recommender";

        Properties proper = new Properties();
        proper.put(StreamsConfig.APPLICATION_ID_CONFIG,"logProcessor");
        proper.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.11:9092");
        proper.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,"192.168.1.11:2181");


        StreamsConfig config = new StreamsConfig(proper);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source",input)
                .addProcessor("processor",() -> new LogProcessor(),"source")
                .addSink("sink",output,"processor");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);

        kafkaStreams.start();
    }
}
