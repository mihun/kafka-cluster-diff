package com.kafka.validation;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class RecordValidator {

    public ValidationResult validate(List<ConsumerRecord> backupConsumerRecords, List<ConsumerRecord> sourceConsumerRecords) {
        if (CollectionUtils.isEmpty(backupConsumerRecords)){
            return ValidationResult.SUCCESSFUL;
        }
        for (int i = 0; i < backupConsumerRecords.size(); i++) {
            if (!isRecordSame(backupConsumerRecords.get(i), sourceConsumerRecords.get(i))){
                return ValidationResult.DEFECT_DATA;
            }
        }
        return ValidationResult.SUCCESSFUL_BUFFER_PORTION;
    }

    private boolean isRecordSame(ConsumerRecord backupRecord, ConsumerRecord sourceRecord) {
        return backupRecord.serializedValueSize() == sourceRecord.serializedValueSize()
                && backupRecord.serializedKeySize() == sourceRecord.serializedKeySize();
    }
}
