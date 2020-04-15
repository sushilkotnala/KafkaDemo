package com.demo.kafka.dto;

import java.util.Date;

public class Message {
    private int messageId;
    private String messageText;
    private Date messageTime;

    public Message(int messageId, String messageText, Date messageTime) {
        this.messageId = messageId;
        this.messageText = messageText;
        this.messageTime = messageTime;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public String getMessageText() {
        return messageText;
    }

    public void setMessageText(String messageText) {
        this.messageText = messageText;
    }

    public Date getMessageTime() {
        return messageTime;
    }

    public void setMessageTime(Date messageTime) {
        this.messageTime = messageTime;
    }
}
