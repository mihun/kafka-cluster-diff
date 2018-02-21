package com.kafka.service;


import com.kafka.consumer.ConsumerController;
import com.kafka.consumer.concurrent.ConsumerHolder;
import com.kafka.consumer.concurrent.ConsumerSemaphore;
import com.kafka.spring.StaticContextHolder;
import com.kafka.validation.RecordValidator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;

public class AtomicPartitionService implements Runnable {

    private TopicPartition topicPartition;
    private ConsumerSemaphore consumerSemaphore;
    private ConsumerController consumerController;
    private RecordValidator recordValidator;

    public AtomicPartitionService(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
        this.consumerSemaphore = StaticContextHolder.getBean(ConsumerSemaphore.class);
        this.consumerController = StaticContextHolder.getBean(ConsumerController.class);
        this.recordValidator = StaticContextHolder.getBean(RecordValidator.class);

    }
    @Override
    public void run() {
        try {
            consumerSemaphore.getSemaphore().acquire();

            int holderIndex = consumerSemaphore.captureAvailableHolder();
            ConsumerHolder consumerHolder = consumerSemaphore.getConsumerHolders()[holderIndex];

            List<ConsumerRecord> backupConsumerRecords = processConsumer(consumerHolder.getBackupConsumer());
            List<ConsumerRecord> productionConsumerRecords = processConsumer(consumerHolder.getProductionConsumer());

            recordValidator.validate(backupConsumerRecords, productionConsumerRecords);

            consumerSemaphore.releaseHolder(holderIndex);

            consumerSemaphore.getSemaphore().release();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private List<ConsumerRecord> processConsumer(KafkaConsumer consumer){
        consumer.assign(Collections.singletonList(topicPartition));
        return consumerController.readMessage(consumer);
    }
}
