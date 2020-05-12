package com.demo.kafka.mains.prod;

import com.demo.kafka.core.producers.AvroProducer;

import java.util.concurrent.CountDownLatch;

public class AvroProducerDemo {

    private static final String INPUT_TOPIC = "avro-topic";


    public static void main(String[] args) {

        int numPartitions = 2;
        int numInstances = 1;
        int numRecords = 10;

        CountDownLatch prePopulateLatch = new CountDownLatch(1);

        AvroProducer producerThread = new AvroProducer(INPUT_TOPIC, false, null,
                true, numRecords, -1, prePopulateLatch);
        producerThread.start();

    }
}
