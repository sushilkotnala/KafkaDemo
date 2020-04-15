package com.demo.kafka.custom.serde;

import com.demo.kafka.dto.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

public class MessageDeserializer implements Deserializer<Message> {

    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Message deserialize(String topic, byte[] bytes) {
        try {
            if (bytes == null){
                System.out.println("Null recieved at deserialize");
                return null;
            }
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            int id = buf.getInt();

            int sizeOfText = buf.getInt();
            byte[] textBytes = new byte[sizeOfText];
            buf.get(textBytes);
            String deserializedText = new String(textBytes, encoding);

            int sizeOfDate = buf.getInt();
            byte[] dateBytes = new byte[sizeOfDate];
            buf.get(dateBytes);
            String dateString = new String(dateBytes,encoding);

            DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

            return new Message(id,deserializedText,df.parse(dateString));

        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to Supplier");
        }
    }

    @Override
    public void close() {

    }
}
