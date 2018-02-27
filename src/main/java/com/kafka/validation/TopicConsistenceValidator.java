package com.kafka.validation;

import com.google.common.collect.Sets;
import com.kafka.exception.InconsistentTopicException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import java.util.Set;

@Slf4j
@Component
public class TopicConsistenceValidator {

    public void validate(Set<String> backupTopics, Set<String> sourceTopics) throws InconsistentTopicException {

        Sets.SetView<String> topicDifference = Sets.symmetricDifference(backupTopics, sourceTopics);
        if (CollectionUtils.isNotEmpty(topicDifference))
            throw new InconsistentTopicException(topicDifference.toString());
        else
            log.info("Topic Consistence is successful with size {}", backupTopics.size() );
    }
}
