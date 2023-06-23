package com.kgromov.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class SparkConfig {
    private final Environment environment;

    @PostConstruct
    public void init() {
        String sparkHome = environment.getProperty("SPARK_HOME");
        System.setProperty("hadoop.home.dir", sparkHome);
        System.setProperty("HADOOP_HOME", sparkHome);
        log.info("SPARK_HOME = {}", sparkHome);
        log.info("HADOOP_HOME = {}", environment.getProperty("HADOOP_HOME"));
    }

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
                .setMaster("local")
                .setSparkHome(environment.getProperty("SPARK_HOME"))
                .setAppName("Spark converter")
//                .set("spark.sql.datetime.java8API.enabled", "true")
                ;
    }

    @Bean
    public JavaSparkContext sparkContext() {
        return new JavaSparkContext(sparkConf());
    }

    @Bean
    public SparkSession sparkSession(JavaSparkContext sparkContext) {
        return SparkSession
                .builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();
    }
}
