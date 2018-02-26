package com.kafka.consumer.configuration;

import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class ConsumerConfiguration {

    private Properties backupConsumerProperties;
    private Properties sourceConsumerProperties;

    public Properties getBackupConsumerProperties() {
        return backupConsumerProperties;
    }

    public Properties getSourceConsumerProperties() {
        return sourceConsumerProperties;
    }

    public void setBackupConsumerProperties(Properties backupConsumerProperties) {
        this.backupConsumerProperties = backupConsumerProperties;
    }

    public void setSourceConsumerProperties(Properties sourceConsumerProperties) {
        this.sourceConsumerProperties = sourceConsumerProperties;
    }
}
