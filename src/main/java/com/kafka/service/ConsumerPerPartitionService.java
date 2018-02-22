package com.kafka.service;

import com.kafka.consumer.ConsumerController;
import com.kafka.consumer.configuration.ConsumerConfiguration;
import com.kafka.out.OutputManager;
import com.kafka.spring.StaticContextHolder;
import com.kafka.validation.RecordValidator;
import com.kafka.validation.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ConsumerPerPartitionService implements Runnable{

    private TopicPartition topicPartition;
    private KafkaConsumer backupConsumer;
    private KafkaConsumer productionConsumer;
    private ConsumerController consumerController;
    private RecordValidator recordValidator;


    public ConsumerPerPartitionService() {
        consumerController = StaticContextHolder.getBean(ConsumerController.class);
        backupConsumer = consumerController.create(ConsumerConfiguration.backupConsumerProperties);
        productionConsumer = consumerController.create(ConsumerConfiguration.productionConsumerProperties);
        recordValidator = StaticContextHolder.getBean(RecordValidator.class);
    }

    @Override
    public void run() {
        try {
            while ((topicPartition = KafkaApplicationService.blockingQueue.poll(500, TimeUnit.MILLISECONDS)) != null) {
                List<ConsumerRecord> backupConsumerRecords = processConsumer(backupConsumer);
                List<ConsumerRecord> productionConsumerRecords = processConsumer(productionConsumer);
                ValidationResult result = recordValidator.validate(backupConsumerRecords, productionConsumerRecords);
                OutputManager.storeResult(topicPartition, result);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private List<ConsumerRecord> processConsumer(KafkaConsumer consumer){
        consumer.assign(Collections.singletonList(topicPartition));
        return consumerController.readMessage(consumer);
    }
}
