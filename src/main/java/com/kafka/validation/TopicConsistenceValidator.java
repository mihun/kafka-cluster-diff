package com.kafka.validation;

import com.kafka.exception.InconsistentTopicException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TopicConsistenceValidator {

    public void validate(KafkaConsumer backupConsumer, KafkaConsumer productionConsumer){
        int backupTopicSize = backupConsumer.listTopics().size();
        int productionTopicSize = productionConsumer.listTopics().size();
        if (backupTopicSize != productionTopicSize)
            throw new InconsistentTopicException(backupTopicSize, productionTopicSize);
        else
            log.info("TopicConsistence is successful with size {}", backupTopicSize );
    }
}
