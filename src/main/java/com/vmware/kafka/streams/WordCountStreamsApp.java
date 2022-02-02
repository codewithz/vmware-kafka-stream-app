package com.vmware.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountStreamsApp {
    public static void main(String[] args) {

        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-stream-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder=new StreamsBuilder();
        //1- Stream from Kafka
        KStream<String,String> wordCountInput=builder.stream("word-count-topic");
        //2- Map Values to Lowercase
        KTable<String,Long> wordCounts= wordCountInput.mapValues(textLine->textLine.toLowerCase())
                                    //3- FlatMap Values split by Space
                                            .flatMapValues(lowerCasedTextLine-> Arrays.asList(lowerCasedTextLine.split(" ")))
                                    //4 - Select the key to apply a key [We discard the old key]
                                            .selectKey((ignoredKey,word)->word)
                                    //5 - Group By Key before Aggregation
                                            .groupByKey()
                                    //6- Count the Occurrences
                                            .count();
        //7- To in order to write the results back to Kafka

        wordCounts.toStream().to("word-count-output", (Produced<String, Long>) Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams=new KafkaStreams(builder.build(),config);
        streams.start();

        //Print the Topology
        System.out.println(streams.toString());

        //Gracefully SHutdown

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
