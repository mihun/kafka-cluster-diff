package com.kafka.service;

import com.kafka.consumer.configuration.ConsumerConfiguration;
import com.kafka.consumer.ConsumerController;
import com.kafka.validation.TopicConsistenceValidator;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Set;


@Service
public class KafkaApplicationService {

    private final TopicConsistenceValidator topicConsistenceValidator;
    private final ConsumerController consumerController;

    @Autowired
    public KafkaApplicationService(TopicConsistenceValidator topicConsistenceValidator, ConsumerController consumerController) {
        this.topicConsistenceValidator = topicConsistenceValidator;
        this.consumerController = consumerController;
    }

    public void run() {
        KafkaConsumer backupConsumer = consumerController.create(ConsumerConfiguration.backupConsumerProperties);
        KafkaConsumer productionConsumer = consumerController.create(ConsumerConfiguration.productionConsumerProperties);

        topicConsistenceValidator.validate(backupConsumer, productionConsumer);

        Set<TopicPartition> topicPartitions = consumerController.collectAllTopicPartitions(backupConsumer);

        for (TopicPartition topicPartition : topicPartitions) {
            Thread thread = new Thread(new AtomicPartitionService(topicPartition));
            thread.setName(topicPartition.toString());
            thread.start();
        }
    }
}
