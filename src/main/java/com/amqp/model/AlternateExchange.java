package com.amqp.model;

public class AlternateExchange {
    private final String name;

    public AlternateExchange(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "AlternateExchange{name='" + name + "'}";
    }
}
