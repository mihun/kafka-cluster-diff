package com.kafka.consumer;

import com.kafka.consumer.configuration.CustomConfiguration;
import com.kafka.controller.TopicPartitionController;
import com.kafka.spring.StaticContextHolder;
import com.kafka.validation.RecordValidator;
import com.kafka.validation.ValidationResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ConsumerUnitChain {

    private RecordValidator recordValidator;

    private ConsumerUnit backupConsumerUnit;
    private ConsumerUnit sourceConsumerUnit;
    private int bufferSize;
    private long pollTimeout;
    private TopicPartition topicPartition;
    private Long lastOffset;

    public ConsumerUnitChain(KafkaConsumer backupConsumer, KafkaConsumer sourceConsumer, TopicPartition topicPartition) {
        CustomConfiguration customConfiguration = StaticContextHolder.getBean(CustomConfiguration.class);
        this.recordValidator = StaticContextHolder.getBean(RecordValidator.class);
        this.backupConsumerUnit = new ConsumerUnit(backupConsumer);
        this.sourceConsumerUnit = new ConsumerUnit(sourceConsumer);
        bufferSize = customConfiguration.getBufferSize();
        pollTimeout = customConfiguration.getPollTimeout();
        this.topicPartition = topicPartition;
        TopicPartitionController topicPartitionController = StaticContextHolder.getBean(TopicPartitionController.class);
        lastOffset = topicPartitionController.getLastOffset(topicPartition);
    }

    public ConsumerUnitChain prepare(){
        backupConsumerUnit.initOrContinue();
        sourceConsumerUnit.initOrContinue();
        return this;
    }

    public ConsumerUnitChain start(){
        backupConsumerUnit.processData();
        sourceConsumerUnit.processData();
        return this;
    }

    public ConsumerUnitChain buffer(){
        backupConsumerUnit.buffer();
        sourceConsumerUnit.buffer();
        return this;
    }

    public ValidationResult validate(){
        if (backupConsumerUnit.getCurrentRecords().size() > sourceConsumerUnit.getCurrentRecords().size()){
            return ValidationResult.INCONSISTENT_PARTITION_SIZE;
        }
        return recordValidator.validate(backupConsumerUnit.getCurrentRecords(), sourceConsumerUnit.getCurrentRecords());
    }

    public boolean isFinished(){
        return backupConsumerUnit.isFinished();
    }


    private class ConsumerUnit {
        private KafkaConsumer consumer;

        private List<ConsumerRecord> currentRecords;
        private List<ConsumerRecord> remainingRecords;

        private boolean isFinished =false;

        ConsumerUnit(KafkaConsumer consumer) {
            this.consumer = consumer;
        }

        List<ConsumerRecord> getCurrentRecords() {
            return currentRecords;
        }


        void processData(){
            if (currentRecords.size() >= bufferSize) {
                return;
            }


            ConsumerRecords records = consumer.poll(pollTimeout);
            if (records.isEmpty()) {
                if (consumer.position(topicPartition) < lastOffset){
                    processData();
                } else {
                    isFinished = true;
                    return;
                }
            }

            for (Object record : records) {
                currentRecords.add( (ConsumerRecord) record);
            }
            if (currentRecords.size() < bufferSize){
                processData();
            }
        }

        void buffer(){
            if (currentRecords.size() >= bufferSize){
                remainingRecords = currentRecords.stream().skip(bufferSize).collect(Collectors.toList());
                currentRecords = currentRecords.stream().limit(bufferSize).collect(Collectors.toList());
            } else {
                remainingRecords = new ArrayList<>();
            }
        }

        boolean isFinished() {
            return isFinished;
        }

        void initOrContinue(){
            if (remainingRecords != null) {
                currentRecords = remainingRecords;
                remainingRecords = null;
            } else if (currentRecords == null){
                currentRecords = new ArrayList<>();
            }
            isFinished = false;
        }
    }

}
