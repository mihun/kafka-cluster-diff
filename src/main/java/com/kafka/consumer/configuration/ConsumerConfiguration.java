package com.kafka.consumer.configuration;

import com.kafka.consumer.reader.PropertiesReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

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

    @Autowired
    public ConsumerConfiguration(PropertiesReader propertiesReader) {
        Properties backupConsumerProperties = propertiesReader.readProperties("backup-consumer.properties");
        backupConsumerProperties.put(Constants.GROUP_ID_PROPERTY, UUID.randomUUID().toString());
        this.backupConsumerProperties = backupConsumerProperties;

        Properties sourceConsumerProperties = propertiesReader.readProperties("source-consumer.properties");
        sourceConsumerProperties.put(Constants.GROUP_ID_PROPERTY, UUID.randomUUID().toString());
        this.sourceConsumerProperties = sourceConsumerProperties;
    }
}
