package com.demo.kafka.core.producers.producers;

import com.demo.kafka.dto.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class MessageProducer extends Thread {

    private final KafkaProducer<String, Message> producer;
    private final String topic;
    private final Boolean isAsync;
    private int numRecords;
    private final CountDownLatch latch;

    public MessageProducer(final String topic,
                                     final Boolean isAsync,
                                     final String transactionalId,
                                     final boolean enableIdempotency,
                                     final int numRecords,
                                     final int transactionTimeoutMs,
                                     final CountDownLatch latch) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //custom serilizer
        props.put("value.serializer", "com.demo.kafka.custom.serde.MessageSerializer");
        props.put("acks", "all");

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
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Message sp1 = new Message(101, "General instructions", df.parse("2020-04-01"));
            Message sp2 = new Message(102, "Invite for Town hall", df.parse("2020-01-01"));

            producer.send(new ProducerRecord<String, Message>(topic, "SUP", sp1)).get();
            producer.send(new ProducerRecord<String, Message>(topic, "SUP", sp2)).get();
        } catch (ParseException e){
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("SupplierProducer Completed.");
    }
}
