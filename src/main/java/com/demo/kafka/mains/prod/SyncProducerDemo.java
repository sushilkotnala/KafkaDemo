package com.demo.kafka.mains.prod;

import com.demo.kafka.core.producers.producers.CustomProducer;
import java.util.concurrent.CountDownLatch;


public class SyncProducerDemo {

    private static final String INPUT_TOPIC = "demo-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    public static void main(String[] args) {

//        if (args.length != 3) {
//            throw new IllegalArgumentException("Should accept 3 parameters: " +
//                    "[number of partitions], [number of instances], [number of records]");
//        }

        int numPartitions = 3;
        int numInstances = 1;
        int numRecords = 10;

        CountDownLatch prePopulateLatch = new CountDownLatch(2);

        CustomProducer producerThread = new CustomProducer(INPUT_TOPIC, false, null,
                true, numRecords, -1, prePopulateLatch);
        producerThread.start();

    }

}
