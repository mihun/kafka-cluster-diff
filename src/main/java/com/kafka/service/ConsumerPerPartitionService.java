package com.kafka.service;

import com.kafka.consumer.ConsumerValidationProcessor;
import com.kafka.controller.ConsumerController;
import com.kafka.controller.TopicPartitionController;
import com.kafka.output.OutputManager;
import com.kafka.spring.StaticContextHolder;
import com.kafka.validation.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;

@Slf4j
public class ConsumerPerPartitionService implements Runnable{

    private ConsumerValidationProcessor consumerValidationProcessor;
    private OutputManager outputManager;
    private TopicPartitionController topicPartitionController;
    private ConsumerController consumerController;


    public ConsumerPerPartitionService() {
        consumerValidationProcessor = StaticContextHolder.getBean(ConsumerValidationProcessor.class);
        outputManager = StaticContextHolder.getBean(OutputManager.class);
        topicPartitionController = StaticContextHolder.getBean(TopicPartitionController.class);
        consumerController = StaticContextHolder.getBean(ConsumerController.class);
    }

    @Override
    public void run() {
        TopicPartition topicPartition;
        KafkaConsumer backupConsumer = consumerController.createBackupConsumer();
        KafkaConsumer sourceConsumer = consumerController.createSourceConsumer();
        while ((topicPartition = topicPartitionController.getTopicPartitionFromQueue()) != null) {
            backupConsumer.assign(Collections.singletonList(topicPartition));
            sourceConsumer.assign(Collections.singletonList(topicPartition));

            ValidationResult result = consumerValidationProcessor.process(backupConsumer, sourceConsumer, topicPartition);
            outputManager.storeResult(topicPartition, result);
        }
    }

}
