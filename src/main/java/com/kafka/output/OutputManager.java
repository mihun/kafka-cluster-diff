package com.kafka.output;

import com.kafka.validation.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class OutputManager {
    private Map<TopicPartition, ValidationResult> failed = new ConcurrentHashMap<>();
    private Set<TopicPartition> successful = Collections.newSetFromMap(new ConcurrentHashMap<TopicPartition, Boolean>());
    private int numberOfPartitions;

    public void storeResult(TopicPartition topicPartition, ValidationResult validationResult) {
        switch (validationResult) {
            case INCONSISTENT_PARTITION_SIZE:
            case DEFECT_DATA:
                failed.put(topicPartition, validationResult);
                return;
            case SUCCESSFUL:
                successful.add(topicPartition);
        }
    }

    public void storeNumberOfPartitions(int numberOfPartitions) {
        this.numberOfPartitions = numberOfPartitions;
    }

    public void print() {
        printSuccessful();
        printFailed();
        printProgress();
    }

    public void printFailed() {
        if (failed.size() > 0) {
            log.error("FAILED TopicPartition Result:");
            for (TopicPartition topicPartition : failed.keySet()) {
                log.error("Topic-Partition: [{}], Reason: [{}]", topicPartition, failed.get(topicPartition));
            }
        }
    }

    public void printSuccessful() {
        log.info("SUCCESSFUL TopicPartition Result:");
        log.info("{}", successful);
    }

    public void printProgress() {
        if (numberOfPartitions > 0) {
            int successfulPartitions = successful.size();
            int failedPartitions = failed.size();
            log.info(" Processed {}% SUCCESSFUL:{} | FAILED:{}", (failedPartitions + successfulPartitions) * 100 / numberOfPartitions, successfulPartitions, failedPartitions);
        }
    }

    public void printException(Exception e) {
        log.error("{} : {}", e.getClass().getName(), e.getMessage());
    }
}
