package com.kafka.consumer;

import com.kafka.consumer.configuration.CustomConfiguration;
import com.kafka.spring.StaticContextHolder;
import com.kafka.validation.RecordValidator;
import com.kafka.validation.ValidationResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ConsumerUnitChain {

    private RecordValidator recordValidator;

    private ConsumerUnit backupConsumerUnit;
    private ConsumerUnit sourceConsumerUnit;
    private int bufferSize;
    private long pollTimeout;
    private TopicPartition topicPartition;

    public ConsumerUnitChain(KafkaConsumer backupConsumer, KafkaConsumer sourceConsumer, TopicPartition topicPartition) {
        CustomConfiguration customConfiguration = StaticContextHolder.getBean(CustomConfiguration.class);
        this.recordValidator = StaticContextHolder.getBean(RecordValidator.class);
        this.backupConsumerUnit = new ConsumerUnit(backupConsumer, topicPartition);
        this.sourceConsumerUnit = new ConsumerUnit(sourceConsumer, topicPartition);
        bufferSize = customConfiguration.getBufferSize();
        pollTimeout = customConfiguration.getPollTimeout();
        this.topicPartition = topicPartition;
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
        return recordValidator.validate(backupConsumerUnit.getCurrentRecords(), sourceConsumerUnit.getCurrentRecords());
    }

    public boolean isFinished(){
        return backupConsumerUnit.isFinished();
    }


    private class ConsumerUnit {
        private KafkaConsumer consumer;

        private List<ConsumerRecord> currentRecords;
        private List<ConsumerRecord> remainingRecords;
        private Long lastOffset;

        private boolean isFinished =false;

        ConsumerUnit(KafkaConsumer consumer, TopicPartition topicPartition) {
            this.consumer = consumer;
            lastOffset = (Long) consumer.endOffsets(Collections.singletonList(topicPartition)).get(topicPartition);
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
