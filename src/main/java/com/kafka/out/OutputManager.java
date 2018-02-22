package com.kafka.out;

import com.kafka.validation.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class OutputManager {
    private static Map<TopicPartition, ValidationResult> failed = new ConcurrentHashMap<>();
    private static Set<TopicPartition> successful = Collections.newSetFromMap(new ConcurrentHashMap<TopicPartition, Boolean>());
    private static int numberOfPartitions = 1;

    public static void storeResult(TopicPartition topicPartition, ValidationResult validationResult){
        switch(validationResult){
            case INCONSISTENT_PARTITION_SIZE:
            case DEFECT_DATA:
                failed.put(topicPartition, validationResult);
                return;
            case SUCCESSFUL:
                successful.add(topicPartition);

        }
    }

    public static void storeNumberOfPartitions(int numberOfPartitions){
        OutputManager.numberOfPartitions = numberOfPartitions;
    }


    public static void print(){
        printSuccessful();
        printFailed();
        printProgress();
    }

    public static void printFailed(){
        if (failed.size() > 0) {
            log.error("FAILED TopicPartition Result:");
            for (TopicPartition topicPartition : failed.keySet()) {
                log.error("Topic-Partition: [{}], Reason: [{}]", topicPartition, failed.get(topicPartition));
            }
        }
    }

    public static void printSuccessful(){
        log.info("SUCCESSFUL TopicPartition Result:");
        for (TopicPartition topicPartition : successful) {
            log.info("Topic-Partition [{}]", topicPartition);
        }
    }

    public static void printProgress(){
        int successfulPartitions = successful.size();
        int failedPartitions = failed.size();
        log.info(" Processed {}% SUCCESSFUL:{} | FAILED:{}", (failedPartitions + successfulPartitions)*100/numberOfPartitions, successfulPartitions, failedPartitions);
    }


}
