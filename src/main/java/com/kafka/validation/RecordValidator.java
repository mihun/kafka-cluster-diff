package com.kafka.validation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class RecordValidator {

    public ValidationResult validate(List<ConsumerRecord> backupConsumerRecords, List<ConsumerRecord> productionConsumerRecords){
        boolean isConsistentRecordsSize = checkConsistence(backupConsumerRecords, productionConsumerRecords);
        if (!isConsistentRecordsSize) return ValidationResult.INCONSISTENT_PARTITION_SIZE;
        return checkRecords(backupConsumerRecords, productionConsumerRecords);
    }

    private boolean checkConsistence(List<ConsumerRecord> backupConsumerRecords, List<ConsumerRecord> productionConsumerRecords) {
        int backupRecordsSize = backupConsumerRecords.size();
        int productionRecordsSize = productionConsumerRecords.size();
        return backupRecordsSize < productionRecordsSize;
    }

    private ValidationResult checkRecords(List<ConsumerRecord> backupConsumerRecords, List<ConsumerRecord> productionConsumerRecords) {
        for (int i = 0; i < backupConsumerRecords.size(); i++) {
            if (!isRecordSame(backupConsumerRecords.get(i), productionConsumerRecords.get(i))){
                return ValidationResult.DEFECT_DATA;
            }
        }
        return ValidationResult.SUCCESSFUL;
    }

    private boolean isRecordSame(ConsumerRecord backupRecord, ConsumerRecord productionRecord) {
        return backupRecord.serializedValueSize() == productionRecord.serializedValueSize()
                && backupRecord.serializedKeySize() == backupRecord.serializedKeySize();
    }
}
