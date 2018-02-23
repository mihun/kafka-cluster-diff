package com.kafka.consumer.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

@Component
public class ConsumerConfiguration {

    private Properties backupConsumerProperties;
    private Properties productionConsumerProperties;

    public Properties getBackupConsumerProperties() {
        return backupConsumerProperties;
    }

    public Properties getProductionConsumerProperties() {
        return productionConsumerProperties;
    }

    @Autowired
    public ConsumerConfiguration(PropertiesReader propertiesReader) {
        Properties backupConsumerProperties = propertiesReader.readProperties("backup-consumer.properties");
        backupConsumerProperties.put(Constants.GROUP_ID_PROPERTY, UUID.randomUUID().toString());
        this.backupConsumerProperties = backupConsumerProperties;

        Properties productionConsumerProperties = propertiesReader.readProperties("source-consumer.properties");
        productionConsumerProperties.put(Constants.GROUP_ID_PROPERTY, UUID.randomUUID().toString());
        this.productionConsumerProperties = productionConsumerProperties;
    }
}
