package com.kgromov.service;

import com.kgromov.config.KafkaSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaStreamService {
    private final KafkaSettings kafkaSettings;
    private final JavaSparkContext sparkContext;

    public void read() {
        Map<String, Object> kafkaParams = Map.of(
                "bootstrap.servers", kafkaSettings.getServer(),
                "key.deserializer", StringDeserializer.class,
                "value.deserializer", StringDeserializer.class,
//                "group.id", kafkaSettings.getGroup(),
                "auto.offset.reset", "latest",
                "enable.auto.commit", false
        );
        JavaStreamingContext streamingContext = new JavaStreamingContext(
                new StreamingContext(sparkContext.sc(), Duration.apply(60_000))
        );
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(List.of(kafkaSettings.getTopic()), kafkaParams)
                );

        stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()))
                .foreachRDD((VoidFunction<JavaPairRDD<String, String>>) rdd ->
                        log.info("Read from topic {}: {}", kafkaSettings.getTopic(), rdd.collectAsMap())
                );
    }
}
