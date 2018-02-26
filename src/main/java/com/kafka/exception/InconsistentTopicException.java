package com.kafka.exception;

public class InconsistentTopicException extends RuntimeException {
    public InconsistentTopicException(int backupTopicSize, int sourceTopicSize) {
        super(String.format("Topic amount are different: backupTopicSize [%d] and sourceTopicSize [%d]",
                backupTopicSize, sourceTopicSize));
    }
}
