package com.kafka.consumer;

import com.kafka.consumer.configuration.ConsumerConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class ConsumerController {

    public KafkaConsumer create(Properties properties){
        return new KafkaConsumer(properties);
    }

    public List<ConsumerRecord> readMessage(KafkaConsumer consumer){
        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        return readData(consumer, consumerRecords);
    }

    private List<ConsumerRecord> readData(KafkaConsumer consumer, List<ConsumerRecord> consumerRecords){
        ConsumerRecords records = consumer.poll(200);
        if (records.isEmpty()) {
            return consumerRecords;
        }
        for (Object record : records) {
            consumerRecords.add( (ConsumerRecord) record);
        }
        return readData(consumer, consumerRecords);
    }

    public Set<TopicPartition> collectAllTopicPartitions(KafkaConsumer consumer) {
        Map<String, List<PartitionInfo>> allTopics = consumer.listTopics();
        Set<TopicPartition> topicPartitions = new HashSet<>(allTopics.size());
        allTopics.keySet().removeAll(ConsumerConfiguration.excludeTopicList);
        allTopics
                .values()
                .stream()
                .flatMap(Collection::stream)
                .forEach(partitionInfo -> topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition())));
        return topicPartitions;
    }

}
