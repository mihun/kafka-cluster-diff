package com.kafka.validation;

import com.kafka.controller.TopicPartitionController;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Component
public class PartitionConsistenceValidator {

    private final TopicPartitionController topicPartitionController;

    @Autowired
    public PartitionConsistenceValidator(TopicPartitionController topicPartitionController) {
        this.topicPartitionController = topicPartitionController;
    }

    public boolean validate(KafkaConsumer sourceConsumer, TopicPartition topicPartition) {
        Long backupLastOffset = topicPartitionController.getLastOffset(topicPartition);
        Long sourceLastOffset = (Long) sourceConsumer.endOffsets(Collections.singletonList(topicPartition)).get(topicPartition);
        return (backupLastOffset <= sourceLastOffset);
    }
}
