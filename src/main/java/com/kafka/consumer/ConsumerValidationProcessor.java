package com.kafka.consumer;

import com.kafka.validation.PartitionConsistenceValidator;
import com.kafka.validation.ValidationResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class ConsumerValidationProcessor {

    private final PartitionConsistenceValidator partitionConsistenceValidator;

    @Autowired
    public ConsumerValidationProcessor(PartitionConsistenceValidator partitionConsistenceValidator) {
        this.partitionConsistenceValidator = partitionConsistenceValidator;
    }

    public ValidationResult process(KafkaConsumer backupConsumer, KafkaConsumer sourceConsumer, TopicPartition topicPartition){

        boolean isPartitionConsistence = partitionConsistenceValidator.validate(sourceConsumer, topicPartition);
        if (!isPartitionConsistence){
            return ValidationResult.INCONSISTENT_PARTITION_SIZE;
        }
        ConsumerUnitChain consumerUnitChain = new ConsumerUnitChain(backupConsumer, sourceConsumer, topicPartition);
        return processConsumerUnitChain(consumerUnitChain);
    }

    private ValidationResult processConsumerUnitChain(ConsumerUnitChain consumerUnitChain){
        ValidationResult result =
                consumerUnitChain
                        .prepare()
                        .start()
                        .buffer()
                        .validate();

        switch (result){
            case SUCCESSFUL_BUFFER_PORTION:
                return consumerUnitChain.isFinished() ? ValidationResult.SUCCESSFUL : processConsumerUnitChain(consumerUnitChain);
            default:
                return result;
        }
    }
}
