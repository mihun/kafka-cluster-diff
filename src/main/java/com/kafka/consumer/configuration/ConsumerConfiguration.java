package com.kafka.consumer.configuration;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.stereotype.Component;
import java.util.Properties;


@Getter
@Setter
@NoArgsConstructor
@Component
public class ConsumerConfiguration {
    private Properties backupConsumerProperties;
    private Properties sourceConsumerProperties;
}