package com.vmware.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorStreamApp {

    public static void main(String[] args) {
        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"fav-col-stream-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        // How to enable the Exactly Once

        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE);



        StreamsBuilder builder=new StreamsBuilder();
        // 1 - Stream data from Kafka Topic
        KStream<String,String> textLines=builder.stream("favorite-color-input");
        System.out.println("KStream Created");
        KStream<String,String> usersAndColors=textLines
                                // 2- Ensure that comma is there in the value and split it
                                        .filter((key,value)->value.contains(","))
                                //3 - We select a key that will be name [in lower case ]
                                        .selectKey((key,value)->value.split(",")[1].toLowerCase())
                                //4 - We get the color from value [in lower case]
                                        .mapValues(value->value.split(",")[2].toLowerCase())
                                //5 - We filter undesired colors
                                        .filter((user,color)-> Arrays.asList("green","blue","red").contains(color));

        usersAndColors.to("users-key-and-color");

        Serde<String> stringSerde=Serdes.String();
        Serde<Long> longSerde=Serdes.Long();

        KTable<String,String> usersAndColorsTable=builder.table("users-key-and-color");

        KTable <String,Long> favoriteColors=usersAndColorsTable
                .groupBy((user,color)-> new KeyValue<>(color,color))
                .count(Materialized.<String,Long, KeyValueStore<Bytes,byte[]>>as("CountsByColors")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        favoriteColors.toStream().to("favorite-color-output", Produced.with(stringSerde,longSerde));

        KafkaStreams streams=new KafkaStreams(builder.build(),config);
        streams.start();
        System.out.println("Stream Started");
      //  streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
