package com.kafka.consumer.configuration;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CustomConfiguration {

    private int numberOfThreads;
    private List excludeTopicList;
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

    public void setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
    }

    public void setExcludeTopicList(List excludeTopicList) {
        this.excludeTopicList = excludeTopicList;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

}
