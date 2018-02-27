package com.kafka.validation;

import org.junit.Before;
import org.junit.Test;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("test")
public class RecordValidatorTest {

    private List<ConsumerRecord> backupConsumerRecords = new ArrayList<>();
    private List<ConsumerRecord> sourceConsumerRecords = new ArrayList<>();

    @Before
    public void setUp() {
        backupConsumerRecords = new ArrayList<>();
        sourceConsumerRecords = new ArrayList<>();

    }

    @Autowired
    RecordValidator recordValidator;

    @Test
    public void validate_same() {


        for (int i = 0; i < 1000; i++) {
            backupConsumerRecords.add(new ConsumerRecord("topic", 0, i, String.valueOf(i).getBytes(), String.valueOf(i).getBytes()));
            sourceConsumerRecords.add(new ConsumerRecord("topic", 0, i, String.valueOf(i).getBytes(), String.valueOf(i).getBytes()));
        }

        Assert.assertEquals(ValidationResult.SUCCESSFUL_BUFFER_PORTION, recordValidator.validate(backupConsumerRecords, sourceConsumerRecords));

        backupConsumerRecords = backupConsumerRecords.stream().limit(500).collect(Collectors.toList());
        Assert.assertEquals(ValidationResult.SUCCESSFUL_BUFFER_PORTION, recordValidator.validate(backupConsumerRecords, sourceConsumerRecords));
    }

    @Test
    public void validate_different() {
        for (int i = 0; i < 100; i++) {
            backupConsumerRecords.add(new ConsumerRecord("topic", 0, i, String.valueOf(i).getBytes(), String.valueOf(i).getBytes()));
            sourceConsumerRecords.add(new ConsumerRecord("topic", 0, i, String.valueOf(i).getBytes(), String.valueOf(i).getBytes()));
        }
        backupConsumerRecords.add(new ConsumerRecord("topic", 0, 100, String.valueOf(100).getBytes(), String.valueOf(100).getBytes()));
        Assert.assertEquals(ValidationResult.INCONSISTENT_PARTITION_SIZE, recordValidator.validate(backupConsumerRecords, sourceConsumerRecords));
    }


    @Test
    public void validate_wrongValue() {
        for (int i = 0; i < 10; i++) {
            backupConsumerRecords.add(new ConsumerRecord("topic", 0, i, String.valueOf(i).getBytes(), String.valueOf(i).getBytes()));
            sourceConsumerRecords.add(new ConsumerRecord("topic", 0, i, String.valueOf(i).getBytes(), (String.valueOf(i+10000) + "testData").getBytes()));
        }
        Assert.assertEquals(ValidationResult.DEFECT_DATA, recordValidator.validate(backupConsumerRecords, sourceConsumerRecords));
    }

    @Test
    public void validate_wrongKey() {
        for (int i = 0; i < 10; i++) {
            backupConsumerRecords.add(new ConsumerRecord("topic", 0, i, String.valueOf(i).getBytes(), String.valueOf(i).getBytes()));
            sourceConsumerRecords.add(new ConsumerRecord("topic", 0, i, String.valueOf(i + 10000).getBytes(), String.valueOf(i).getBytes()));
        }
        Assert.assertEquals(ValidationResult.DEFECT_DATA, recordValidator.validate(backupConsumerRecords, sourceConsumerRecords));
    }

    @Test
    public void validate_successful() {
        for (int i = 0; i < 10; i++) {
            sourceConsumerRecords.add(new ConsumerRecord("topic", 0, i, String.valueOf(i).getBytes(), String.valueOf(i).getBytes()));
        }
        Assert.assertEquals(ValidationResult.SUCCESSFUL, recordValidator.validate(backupConsumerRecords, sourceConsumerRecords));
    }
}