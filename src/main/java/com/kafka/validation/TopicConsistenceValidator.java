package com.kafka.validation;

import com.kafka.exception.InconsistentTopicException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Set;

@Slf4j
@Component
public class TopicConsistenceValidator {

    public void validate(Set<String> backupTopics, Set<String> sourceTopics){
        if (backupTopics.size() != sourceTopics.size())
            throw new InconsistentTopicException(backupTopics.size(), sourceTopics.size());
        else
            log.info("TopicConsistence is successful with size {}", backupTopics.size() );
    }
}
