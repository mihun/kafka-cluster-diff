package com.kafka.consumer.configuration;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CustomConfiguration {

    private int numberOfThreads;
    private List<Object> excludeTopicList;

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public List<Object> getExcludeTopicList() {
        return excludeTopicList;
    }

    @Autowired
    public CustomConfiguration(PropertiesReader propertiesReader) {
        PropertiesConfiguration properties = propertiesReader.readPropertiesConfiguration("custom.properties");
        numberOfThreads = properties.getInt(Constants.THREADS_PROPERTY);
        excludeTopicList = properties.getList(Constants.EXCLUDE_TOPICS_PROPERTY);
    }

}
