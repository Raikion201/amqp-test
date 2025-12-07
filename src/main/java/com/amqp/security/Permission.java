package com.amqp.security;

public enum Permission {
    CONFIGURE,  // Create/delete exchanges, queues, bindings
    WRITE,      // Publish messages to exchanges
    READ        // Consume messages from queues
}
