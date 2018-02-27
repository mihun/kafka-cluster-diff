package com.kafka.controller;

import com.kafka.consumer.configuration.CustomConfiguration;
import com.kafka.validation.TopicConsistenceValidator;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Component
public class TopicPartitionController {

    private BlockingQueue<TopicPartition> blockingQueue;
    private Map<TopicPartition, Long> topicPartitionEndOffsets;

    private final CustomConfiguration customConfiguration;
    private final ConsumerController consumerController;
    private final TopicConsistenceValidator topicConsistenceValidator;

    @Autowired
    public TopicPartitionController(CustomConfiguration customConfiguration, ConsumerController consumerController, TopicConsistenceValidator topicConsistenceValidator) {
        this.customConfiguration = customConfiguration;
        this.consumerController = consumerController;
        this.topicConsistenceValidator = topicConsistenceValidator;
    }

    public int collectAllTopicPartitions() {
        KafkaConsumer backupConsumer = consumerController.createBackupConsumer();
        KafkaConsumer sourceConsumer = consumerController.createSourceConsumer();

        Map<String, List<PartitionInfo>> allBackupTopics = backupConsumer.listTopics();
        allBackupTopics.keySet().removeAll(customConfiguration.getExcludeTopicList());

        Map<String, List<PartitionInfo>> allSourceTopics = sourceConsumer.listTopics();
        allSourceTopics.keySet().removeAll(customConfiguration.getExcludeTopicList());

        topicConsistenceValidator.validate(allBackupTopics.keySet(), allSourceTopics.keySet());

        Set<TopicPartition> topicPartitions = new HashSet<>(allBackupTopics.size());
        allBackupTopics
                .values()
                .stream()
                .flatMap(Collection::stream)
                .forEach(partitionInfo -> topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition())));
        blockingQueue = new ArrayBlockingQueue<>(topicPartitions.size(), true, topicPartitions);
        topicPartitionEndOffsets = Collections.unmodifiableMap(new HashMap<>(backupConsumer.endOffsets(topicPartitions)));
        return topicPartitions.size();
    }

    public TopicPartition getTopicPartitionFromQueue(){
        try {
            return blockingQueue.poll(500, TimeUnit.MILLISECONDS);
        }  catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public Long getLastOffset(TopicPartition topicPartition) {
        return topicPartitionEndOffsets.get(topicPartition);
    }



}
