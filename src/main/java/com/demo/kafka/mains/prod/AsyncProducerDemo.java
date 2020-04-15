package com.demo.kafka.mains.prod;

import com.demo.kafka.core.producers.producers.CustomProducer;

import java.util.concurrent.CountDownLatch;


public class AsyncProducerDemo {

    private static final String INPUT_TOPIC = "demo-topic2";
    private static final String OUTPUT_TOPIC = "output-topic";

    public static void main(String[] args) {

        int numPartitions = 3;
        int numInstances = 1;
        int numRecords = 10;

        CountDownLatch prePopulateLatch = new CountDownLatch(2);

        CustomProducer producerThread = new CustomProducer(INPUT_TOPIC, true, null,
                true, numRecords, -1, prePopulateLatch);
        producerThread.start();

        try{
            Thread.sleep(5 * 1000);
            prePopulateLatch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
