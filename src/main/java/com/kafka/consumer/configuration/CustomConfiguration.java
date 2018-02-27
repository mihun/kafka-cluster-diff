package com.kafka.consumer.configuration;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.stereotype.Component;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@Component
public class CustomConfiguration {
    private int numberOfThreads;
    private List excludeTopicList;
    private int bufferSize;
    private long pollTimeout;
}
