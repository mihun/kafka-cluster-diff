package com.kafka.service;

import com.kafka.controller.ThreadController;
import com.kafka.controller.TopicPartitionController;
import com.kafka.exception.InconsistentTopicException;
import com.kafka.output.OutputManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class KafkaApplicationService {

    private final OutputManager outputManager;
    private final TopicPartitionController topicPartitionController;
    private final ThreadController threadController;

    @Autowired
    public KafkaApplicationService(OutputManager outputManager,
                                   TopicPartitionController topicPartitionController,
                                   ThreadController threadController) {
        this.outputManager = outputManager;
        this.topicPartitionController = topicPartitionController;
        this.threadController = threadController;
    }

    public void run()  {
        try {
            int topicPartitionsSize = topicPartitionController.collectAllTopicPartitions();
            outputManager.storeNumberOfPartitions(topicPartitionsSize);


            Thread[] threads = threadController.initAndRunThreads();
            threadController.joinToThreads(threads);

            outputManager.print();
        } catch (InconsistentTopicException e){
            outputManager.printException(e);
        }

    }

    @Scheduled(fixedDelay = 10000)
    public void check() {
        outputManager.printProgress();
    }
}
