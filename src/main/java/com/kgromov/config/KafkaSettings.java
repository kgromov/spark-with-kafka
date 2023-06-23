package com.kgromov.config;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class KafkaSettings {
    private String server;
    private String topic;
    private String group;
}
