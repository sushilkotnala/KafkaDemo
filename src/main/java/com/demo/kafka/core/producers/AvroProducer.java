package com.demo.kafka.core.producers;

import com.demo.kafka.dto.Person;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AvroProducer extends Thread  {

    private final KafkaProducer<String, Person> producer;
    private final String topic;
    private final Boolean isAsync;
    private int numRecords;
    private final CountDownLatch latch;

    public AvroProducer(final String topic,
                                     final Boolean isAsync,
                                     final String transactionalId,
                                     final boolean enableIdempotency,
                                     final int numRecords,
                                     final int transactionTimeoutMs,
                                     final CountDownLatch latch) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");


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
        try{
            Person kenny = new Person(125747, "Kenny", "Armstrong", "kenny@linuxacademy.com");
            producer.send(new ProducerRecord<String, Person>(topic, kenny.getId()+"", kenny)).get();

            Person terry = new Person(943257, "Terry", "Cox", "terry@linuxacademy.com");
            producer.send(new ProducerRecord<String, Person>(topic, terry.getId()+"", terry)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
