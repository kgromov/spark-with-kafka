package com.kgromov.config;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class KafkaConfig {
    private final SparkSession sparkSession;

    @Bean
    @ConfigurationProperties(prefix = "spark.kafka")
    public KafkaSettings kafkaSettings() {
        return new KafkaSettings();
    }

    @Bean("kafkaStream")
    public DataStreamReader kafkaStream(KafkaSettings kafkaSettings) {
        return  sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaSettings.getServer())
                .option("subscribe", "topic1");
    }
}
