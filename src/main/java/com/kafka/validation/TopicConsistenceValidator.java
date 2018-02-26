package com.kafka.validation;

import com.kafka.exception.InconsistentTopicException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TopicConsistenceValidator {

    public void validate(KafkaConsumer backupConsumer, KafkaConsumer sourceConsumer){
        int backupTopicSize = backupConsumer.listTopics().size();
        int sourceTopicSize = sourceConsumer.listTopics().size();
        if (backupTopicSize != sourceTopicSize)
            throw new InconsistentTopicException(backupTopicSize, sourceTopicSize);
        else
            log.info("TopicConsistence is successful with size {}", backupTopicSize );
    }
}
