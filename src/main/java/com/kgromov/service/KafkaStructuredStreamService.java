package com.kgromov.service;

import com.kgromov.config.KafkaSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaStructuredStreamService {
    private final KafkaSettings kafkaSettings;
    private final DataStreamReader kafkaStream;

   /* Scala version
   val ds1 = spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "topicA")
            .load()
    val query1 = ds1.collect.foreach(println)
            .writeStream
            .format("console")
            .start()
    val query2 = ds1.writeStream
            .format("console")
            .start();
       ds1.printSchema()
       query1.awaitTermination();
       query2.awaitTermination();*/

    public void read() throws Exception{
        Dataset<Row> dataset = kafkaStream.load();
        Dataset<Row> selectExpr = dataset.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
          /*  List<String> headers = selectExpr.select("key").as("headers").toJSON().collectAsList();
            List<String> payload = selectExpr.select("value").as("message").toJSON().collectAsList();
            log.info("Message from topic = {}: headers = {}, payload = {}", kafkaSettings.getTopicName(), headers, payload);*/

        selectExpr.select("key").as("headers")
                .writeStream()
                .format("console")
                .outputMode("append")
                .start()
                .awaitTermination();

        selectExpr.select("value").as("message")
                .writeStream()
                .foreachBatch((df, v2) -> {
                    List<String> message = selectExpr.select("value").as("message").toJSON().collectAsList();
                    log.info("Message from topic = {}: message = {}", kafkaSettings.getTopic(), message);
                })
                .start();
    }

    public void write(Dataset<Row> dataset) throws Exception {
        // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
//        dataset.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
        dataset.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .format("kafka")
                .outputMode("append")
                .option("kafka.bootstrap.servers", kafkaSettings.getServer())
                .option("topic", kafkaSettings.getTopic())
                .start()
                .awaitTermination();
    }
}
