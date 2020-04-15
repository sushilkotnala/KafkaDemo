package com.demo.kafka.custom.partitoner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private String messageType;
    private Double reservedPartitionPercentage;

    @Override
    public void configure(Map<String, ?> configs) {
        messageType = configs.get("message.type").toString();
        reservedPartitionPercentage = new Double(configs.get("reserved.partition.percentage").toString());
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int reservedPartitions = (int)Math.abs(numPartitions * reservedPartitionPercentage);
        int partitionName=0;

        if ( (keyBytes == null) || (!(key instanceof String)) )
            throw new InvalidRecordException("All messages are having same key");

        //Confidential message
        if ( ((String)key).startsWith(messageType) ) {
            partitionName = Utils.toPositive(Utils.murmur2(valueBytes)) % reservedPartitions;
        } else {
            partitionName = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions-reservedPartitions) +
                    reservedPartitions ;
        }

        System.out.println("Key = " + (String)key + " Partition = " + partitionName +
                " numPartitions: " + numPartitions +
                " reservedPartitionPercentage: " + reservedPartitionPercentage );
        return partitionName;
    }

    @Override
    public void close() {

    }


}
