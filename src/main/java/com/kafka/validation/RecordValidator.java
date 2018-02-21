package com.kafka.validation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class RecordValidator {

    public void validate(List<ConsumerRecord> backupConsumerRecords, List<ConsumerRecord> productionConsumerRecords){
        checkConsistence(backupConsumerRecords, productionConsumerRecords);
        checkRecords(backupConsumerRecords, productionConsumerRecords);
    }

    private void checkConsistence(List<ConsumerRecord> backupConsumerRecords, List<ConsumerRecord> productionConsumerRecords) {
        int backupRecordsSize = backupConsumerRecords.size();
        int productionRecordsSize = productionConsumerRecords.size();
        if (backupRecordsSize > productionRecordsSize){
            log.error("backupRecords {} more than  productionRecords {}", backupRecordsSize,  productionRecordsSize);
        } else if (backupRecordsSize < productionRecordsSize) {
            log.info("backupRecords {} less than  productionRecords {}", backupRecordsSize,  productionRecordsSize);
        }
    }

    private void checkRecords(List<ConsumerRecord> backupConsumerRecords, List<ConsumerRecord> productionConsumerRecords) {
        for (int i = 0; i < backupConsumerRecords.size(); i++) {
            if (i >= productionConsumerRecords.size()) return;
            checkRecord(backupConsumerRecords.get(i), productionConsumerRecords.get(i));
        }
    }

    private void checkRecord(ConsumerRecord backupRecord, ConsumerRecord productionRecord) {
        int backupSerializedValueSize = backupRecord.serializedValueSize();
        int productionSerializedValueSize = productionRecord.serializedValueSize();
        if (backupSerializedValueSize != productionSerializedValueSize){
            log.error("backupRecord {} is not equals to productionRecord {}", backupRecord, productionRecord);
        }
    }
}
