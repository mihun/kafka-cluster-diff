package com.kafka.consumer.concurrent;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerHolder {

    private KafkaConsumer backupConsumer;
    private KafkaConsumer productionConsumer;
    private boolean isAvailable;

    public ConsumerHolder(KafkaConsumer backupConsumer, KafkaConsumer productionConsumer) {
        this.backupConsumer = backupConsumer;
        this.productionConsumer = productionConsumer;
        this.isAvailable = true;
    }

    public KafkaConsumer getBackupConsumer() {
        return backupConsumer;
    }

    public KafkaConsumer getProductionConsumer() {
        return productionConsumer;
    }

    public boolean isAvailable() {
        return isAvailable;
    }

    public void setAvailable(boolean available) {
        isAvailable = available;
    }
}
