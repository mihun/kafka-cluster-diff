package com.kafka.controller;

import com.kafka.consumer.configuration.ConsumerConfiguration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ConsumerController {

    private final ConsumerConfiguration consumerConfiguration;

    @Autowired
    public ConsumerController(ConsumerConfiguration consumerConfiguration) {
        this.consumerConfiguration = consumerConfiguration;
    }

    public KafkaConsumer createBackupConsumer(){
        return new KafkaConsumer(consumerConfiguration.getBackupConsumerProperties());
    }

    public KafkaConsumer createSourceConsumer(){
        return new KafkaConsumer(consumerConfiguration.getSourceConsumerProperties());
    }

}
