package com.kafka.service;

import com.kafka.consumer.configuration.Constants;
import com.kafka.consumer.configuration.ConsumerConfiguration;
import com.kafka.consumer.ConsumerController;
import com.kafka.out.OutputManager;
import com.kafka.validation.TopicConsistenceValidator;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Service
public class KafkaApplicationService {

    public static BlockingQueue<TopicPartition> blockingQueue;

    private final TopicConsistenceValidator topicConsistenceValidator;
    private final ConsumerController consumerController;

    @Autowired
    public KafkaApplicationService(TopicConsistenceValidator topicConsistenceValidator, ConsumerController consumerController) {
        this.topicConsistenceValidator = topicConsistenceValidator;
        this.consumerController = consumerController;
    }

    public void run()  {
        KafkaConsumer backupConsumer = consumerController.create(ConsumerConfiguration.backupConsumerProperties);
        KafkaConsumer productionConsumer = consumerController.create(ConsumerConfiguration.productionConsumerProperties);

        topicConsistenceValidator.validate(backupConsumer, productionConsumer);

        Set<TopicPartition> topicPartitions = consumerController.collectAllTopicPartitions(backupConsumer);
        OutputManager.storeNumberOfPartitions(topicPartitions.size());
        blockingQueue = new ArrayBlockingQueue<>(topicPartitions.size(), true, topicPartitions);
        Thread[] threads = new Thread[Constants.NUMBER_OF_THREADS];
        for (int i = 0; i < Constants.NUMBER_OF_THREADS; i++) {
            Thread thread = new Thread(new ConsumerPerPartitionService());
            threads[i] = thread;
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        OutputManager.print();
    }

    @Scheduled(fixedDelay = 10000)
    public void check() {
        OutputManager.printProgress();
    }
}
