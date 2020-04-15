package com.demo.kafka.mains.prod;

import com.demo.kafka.core.producers.producers.CustomPartitionerProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class CustPartitionerProdDemo {

    private static final String INPUT_TOPIC = "cust-part";

    public static void main (String[] args) throws ExecutionException, InterruptedException {

        int numPartitions = 4;
        short replicationFactor =1;
        int numInstances = 1;

        int numRecords = 10;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        AdminClient adminClient = AdminClient.create(props);

        deleteTopic(adminClient, Arrays.asList(INPUT_TOPIC));
        createTopic(adminClient, INPUT_TOPIC, numPartitions, replicationFactor);

        CountDownLatch prePopulateLatch = new CountDownLatch(1);

        CustomPartitionerProducer producerThread = new CustomPartitionerProducer(INPUT_TOPIC, false, null,
                true, numRecords, -1, prePopulateLatch);
        producerThread.start();
    }

    private static void createTopic(final AdminClient adminClient, final String topicName, final int numPartitions,
                                    final short replicationFactor) {

        final List<NewTopic> newTopics = Arrays.asList(
                new NewTopic(topicName, numPartitions, replicationFactor));
        try {
            adminClient.createTopics(newTopics).all().get();
            System.out.println("Created new topics: " + newTopics);

        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                e.printStackTrace();
            }
            System.out.println("Metadata of the old topics are not cleared yet...");
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    private static void deleteTopic(final AdminClient adminClient, final List<String> topicsToDelete)
            throws InterruptedException, ExecutionException {
        try {
            adminClient.deleteTopics(topicsToDelete).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw e;
            }
            System.out.println("Encountered exception during topic deletion: " + e.getCause());
        }
        System.out.println("Deleted old topics: " + topicsToDelete);
        //Buffer time to make sure it is done in cluster
        Thread.sleep(2000);
    }

}
