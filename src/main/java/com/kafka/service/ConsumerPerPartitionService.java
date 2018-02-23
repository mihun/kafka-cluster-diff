package com.kafka.service;

import com.kafka.controller.ConsumerController;
import com.kafka.controller.TopicPartitionController;
import com.kafka.output.OutputManager;
import com.kafka.spring.StaticContextHolder;
import com.kafka.validation.RecordValidator;
import com.kafka.validation.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;

@Slf4j
public class ConsumerPerPartitionService implements Runnable{

    private TopicPartition topicPartition;
    private KafkaConsumer backupConsumer;
    private KafkaConsumer productionConsumer;
    private ConsumerController consumerController;
    private RecordValidator recordValidator;
    private OutputManager outputManager;
    private TopicPartitionController topicPartitionController;


    public ConsumerPerPartitionService() {
        consumerController = StaticContextHolder.getBean(ConsumerController.class);
        backupConsumer = consumerController.createBackupConsumer();
        productionConsumer = consumerController.createProductionConsumer();
        recordValidator = StaticContextHolder.getBean(RecordValidator.class);
        outputManager = StaticContextHolder.getBean(OutputManager.class);
        topicPartitionController = StaticContextHolder.getBean(TopicPartitionController.class);
    }

    @Override
    public void run() {
        while ((topicPartition = topicPartitionController.getTopicPartitionFromQueue()) != null) {
            List<ConsumerRecord> backupConsumerRecords = processConsumer(backupConsumer);
            List<ConsumerRecord> productionConsumerRecords = processConsumer(productionConsumer);
            ValidationResult result = recordValidator.validate(backupConsumerRecords, productionConsumerRecords);
            outputManager.storeResult(topicPartition, result);
        }
    }

    private List<ConsumerRecord> processConsumer(KafkaConsumer consumer){
        consumer.assign(Collections.singletonList(topicPartition));
        return consumerController.readMessage(consumer);
    }
}
