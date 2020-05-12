package com.demo.kafka.mains.cons;

import com.demo.kafka.core.consumers.AvroConsumer;
import com.demo.kafka.core.consumers.MessageConsumer;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AvroConsumerDemo {

    private static final String INPUT_TOPIC = "avro-topic";

    public static void main (String[] args) {

        int numPartitions = 3;
        int numInstances = 1;
        int numRecords = 10;

        CountDownLatch consumeLatch = new CountDownLatch(1);

        AvroConsumer consumerThread = new AvroConsumer(INPUT_TOPIC, "avro-1", Optional.empty(),
                true, numRecords, consumeLatch);
        consumerThread.start();

        try{
            if (!consumeLatch.await(2, TimeUnit.MINUTES)) {
                throw new TimeoutException("Timeout after 2 minutes waiting for output data consumption");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        consumerThread.shutdown();
    }
}
