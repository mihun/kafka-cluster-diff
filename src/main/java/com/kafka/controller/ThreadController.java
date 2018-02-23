package com.kafka.controller;

import com.kafka.consumer.configuration.CustomConfiguration;
import com.kafka.service.ConsumerPerPartitionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ThreadController {

    private final CustomConfiguration customConfiguration;

    @Autowired
    public ThreadController(CustomConfiguration customConfiguration) {
        this.customConfiguration = customConfiguration;
    }

    public Thread[] initAndRunThreads(){
        int numberOfThreads = customConfiguration.getNumberOfThreads();
        Thread[] threads = new Thread[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            Thread thread = new Thread(new ConsumerPerPartitionService());
            threads[i] = thread;
            thread.start();
        }
        return threads;
    }

    public void joinToThreads(Thread[] threads) {
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }
}
