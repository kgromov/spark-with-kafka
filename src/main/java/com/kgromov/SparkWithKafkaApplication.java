package com.kgromov;

import com.kgromov.service.KafkaStreamService;
import com.kgromov.service.KafkaStructuredStreamService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@Slf4j
public class SparkWithKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkWithKafkaApplication.class, args);
    }


    @Bean
    ApplicationRunner applicationRunner(KafkaStructuredStreamService structuredStreamService,
                                        KafkaStreamService kafkaStreamService) {
        return args -> {
//            kafkaStreamService.read();
            structuredStreamService.read();
        };
    }

}
