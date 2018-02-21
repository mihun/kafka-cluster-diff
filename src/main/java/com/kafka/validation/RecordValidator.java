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
        int diffCount = checkRecords(backupConsumerRecords, productionConsumerRecords);
        if (diffCount > 0) {
            log.error("[{}] There are [{}] different records from [{}]", Thread.currentThread().getName(), diffCount, backupConsumerRecords.size() );
        }
    }

    private void checkConsistence(List<ConsumerRecord> backupConsumerRecords, List<ConsumerRecord> productionConsumerRecords) {
        int backupRecordsSize = backupConsumerRecords.size();
        int productionRecordsSize = productionConsumerRecords.size();
        if (backupRecordsSize > productionRecordsSize){
            log.error("[{}] backupRecords {} more than  productionRecords {}", Thread.currentThread().getName(), backupRecordsSize,  productionRecordsSize);
        } else if (backupRecordsSize < productionRecordsSize) {
            log.info("[{}] backupRecords {} less than  productionRecords {}", Thread.currentThread().getName(), backupRecordsSize,  productionRecordsSize);
        }
    }

    private int checkRecords(List<ConsumerRecord> backupConsumerRecords, List<ConsumerRecord> productionConsumerRecords) {
        int diffRecordCount = 0;
        int actualSizeToCheck = Math.min(backupConsumerRecords.size(), productionConsumerRecords.size());
        for (int i = 0; i < actualSizeToCheck; i++) {
            if (!isRecordSame(backupConsumerRecords.get(i), productionConsumerRecords.get(i))){
                diffRecordCount++;
            }
        }
        return diffRecordCount;
    }

    private boolean isRecordSame(ConsumerRecord backupRecord, ConsumerRecord productionRecord) {
        int backupSerializedValueSize = backupRecord.serializedValueSize();
        int productionSerializedValueSize = productionRecord.serializedValueSize();
        return backupSerializedValueSize == productionSerializedValueSize;
    }
}
