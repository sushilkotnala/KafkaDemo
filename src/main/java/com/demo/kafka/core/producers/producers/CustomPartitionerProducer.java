package com.demo.kafka.core.producers.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class CustomPartitionerProducer extends Thread  {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private int numRecords;
    private final CountDownLatch latch;

    public CustomPartitionerProducer(final String topic,
                           final Boolean isAsync,
                           final String transactionalId,
                           final boolean enableIdempotency,
                           final int numRecords,
                           final int transactionTimeoutMs,
                           final CountDownLatch latch) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.demo.kafka.custom.partitoner.CustomPartitioner");
        props.put("acks", "all");

        //Custom properties
        props.put("message.type", "CONF-MESS:");
        props.put("reserved.partition.percentage", "0.3");

        this.producer = new KafkaProducer(props);
        this.topic = topic;
        this.isAsync = isAsync;
        this.numRecords = numRecords;
        this.latch = latch;
    }

    public KafkaProducer get() {
        return producer;
    }


    @Override
    public void run() {
        for (int i=1 ; i<=10 ; i++) {
            try {
                String key = "CONF-MESS:"+i;
                String value = "This is confidential message "+i;
                RecordMetadata metaData = producer.send(new ProducerRecord<>(topic, key, value)).get();
                System.out.println("Key ="+key + " Value =" + value + "  Offset = " + metaData.offset() + " Partition = " + metaData.partition() );
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }


        for (int i=11 ; i<=20 ; i++)
            try {
                String key = "Message:"+i;
                String value = "This is normal message "+i;
                RecordMetadata metaData = producer.send(new ProducerRecord<>(topic, key, value)).get();
                System.out.println("Key ="+key + " Value =" + value + "  Offset = " + metaData.offset() + " Partition = " + metaData.partition() );
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
    }
}
