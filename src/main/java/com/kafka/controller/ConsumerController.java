package com.kafka.controller;

import com.kafka.consumer.configuration.ConsumerConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class ConsumerController {

    private final ConsumerConfiguration consumerConfiguration;

    @Autowired
    public ConsumerController(ConsumerConfiguration consumerConfiguration) {
        this.consumerConfiguration = consumerConfiguration;
    }

    public List<ConsumerRecord> readMessage(KafkaConsumer consumer){
        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        return readData(consumer, consumerRecords);
    }

    private List<ConsumerRecord> readData(KafkaConsumer consumer, List<ConsumerRecord> consumerRecords){
        ConsumerRecords records = consumer.poll(200);
        if (records.isEmpty()) {
            return consumerRecords;
        }
        for (Object record : records) {
            consumerRecords.add( (ConsumerRecord) record);
        }
        return readData(consumer, consumerRecords);
    }



    public KafkaConsumer createBackupConsumer(){
        return new KafkaConsumer(consumerConfiguration.getBackupConsumerProperties());
    }

    public KafkaConsumer createProductionConsumer(){
        return new KafkaConsumer(consumerConfiguration.getProductionConsumerProperties());
    }

}
