package com.demo.kafka.mains.prod;

import com.demo.kafka.core.producers.producers.MessageProducer;

import java.util.concurrent.CountDownLatch;

public class MessageProducerDemo {

    private static final String INPUT_TOPIC = "serde-topic";


    public static void main(String[] args) {

        int numPartitions = 2;
        int numInstances = 1;
        int numRecords = 10;

        CountDownLatch prePopulateLatch = new CountDownLatch(1);

        MessageProducer producerThread = new MessageProducer(INPUT_TOPIC, false, null,
                true, numRecords, -1, prePopulateLatch);
        producerThread.start();

    }
}
