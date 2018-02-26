package com.kafka.consumer.configuration;

import com.kafka.consumer.reader.PropertiesReader;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CustomConfiguration {

    private int numberOfThreads;
    private List<Object> excludeTopicList;
    private int bufferSize;
    private long pollTimeout;

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public List<Object> getExcludeTopicList() {
        return excludeTopicList;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }

    @Autowired
    public CustomConfiguration(PropertiesReader propertiesReader) {
        PropertiesConfiguration properties = propertiesReader.readPropertiesConfiguration("custom.properties");
        numberOfThreads = properties.getInt(Constants.THREADS_PROPERTY);
        excludeTopicList = properties.getList(Constants.EXCLUDE_TOPICS_PROPERTY);
        bufferSize = properties.getInt(Constants.BUFFER_SIZE_PROPERTY);
        pollTimeout = properties.getLong(Constants.POLL_TIMEOUT_PROPERTY);
    }

}
