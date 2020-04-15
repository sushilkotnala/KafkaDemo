package com.demo.kafka.custom.serde;

import com.demo.kafka.dto.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;


public class MessageSerializer implements Serializer<Message> {

    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Message message) {

        int sizeOfId;
        int sizeOfText;
        int sizeOfDate;
        byte[] serializedId;
        byte[] serializedText;
        byte[] serializedDate;

        try {
            if (message == null)
                return null;

            serializedText = message.getMessageText().getBytes(encoding);
            sizeOfText = serializedText.length;
            serializedDate = message.getMessageTime().toString().getBytes(encoding);
            sizeOfDate = serializedDate.length;

            ByteBuffer buf = ByteBuffer.allocate(message.getMessageId()+4+sizeOfText+4+sizeOfDate);
            buf.putInt(message.getMessageId());
            buf.putInt(sizeOfText);
            buf.put(serializedText);
            buf.putInt(sizeOfDate);
            buf.put(serializedDate);

            return buf.array();

        } catch (Exception e) {
            throw new SerializationException("Error when serializing Message to byte[]");
        }

    }

    @Override
    public void close() {

    }
}
