package com.kafka.consumer.concurrent;


import com.kafka.consumer.configuration.Constants;
import com.kafka.consumer.ConsumerController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;

@Component
public class ConsumerSemaphore{

    private final Semaphore semaphore = new Semaphore(Constants.NUMBER_OF_THREADS, true);
    private final ConsumerHolder[] consumerHolders = new ConsumerHolder[Constants.NUMBER_OF_THREADS];

    @Autowired
    public ConsumerSemaphore(ConsumerController consumerController) {
        for (int i = 0; i < consumerHolders.length; i++) {
            consumerHolders[i] = consumerController.createConsumerHolder();
        }
    }

    public int captureAvailableHolder(){
        synchronized (consumerHolders){
            for (int i = 0; i < consumerHolders.length; i++)
                if (consumerHolders[i].isAvailable()) {
                    consumerHolders[i].setAvailable(false);
                    return i;
                }
                throw new RuntimeException("There are not available holders");
        }
    }

    public void releaseHolder(int holderIndex){
        synchronized (consumerHolders) {
            consumerHolders[holderIndex].setAvailable(true);
        }
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }

    public ConsumerHolder[] getConsumerHolders() {
        return consumerHolders;
    }
}
