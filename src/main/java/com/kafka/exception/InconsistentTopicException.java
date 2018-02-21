package com.kafka.exception;

public class InconsistentTopicException extends RuntimeException {
    public InconsistentTopicException(int backupTopicSize, int productionTopicSize) {
        super(String.format("Topic amount are different: backupTopicSize [%d] and productionTopicSize [%d]",
                backupTopicSize, productionTopicSize));
    }
}
