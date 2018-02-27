package com.kafka.exception;

public class InconsistentTopicException extends Exception {
    public InconsistentTopicException(String topicDifference) {
        super(String.format("Topic difference is: %s", topicDifference));
    }
}
