package com.amqp.amqp;

public abstract class AmqpMethod {
    private final short classId;
    private final short methodId;
    
    protected AmqpMethod(short classId, short methodId) {
        this.classId = classId;
        this.methodId = methodId;
    }
    
    public short getClassId() {
        return classId;
    }
    
    public short getMethodId() {
        return methodId;
    }
    
    public enum Class {
        CONNECTION(10),
        CHANNEL(20),
        EXCHANGE(40),
        QUEUE(50),
        BASIC(60),
        CONFIRM(85);
        
        private final short value;
        
        Class(int value) {
            this.value = (short) value;
        }
        
        public short getValue() {
            return value;
        }
    }
    
    public enum ConnectionMethod {
        START(10),
        START_OK(11),
        SECURE(20),
        SECURE_OK(21),
        TUNE(30),
        TUNE_OK(31),
        OPEN(40),
        OPEN_OK(41),
        CLOSE(50),
        CLOSE_OK(51);
        
        private final short value;
        
        ConnectionMethod(int value) {
            this.value = (short) value;
        }
        
        public short getValue() {
            return value;
        }
    }
    
    public enum ChannelMethod {
        OPEN(10),
        OPEN_OK(11),
        FLOW(20),
        FLOW_OK(21),
        CLOSE(40),
        CLOSE_OK(41);
        
        private final short value;
        
        ChannelMethod(int value) {
            this.value = (short) value;
        }
        
        public short getValue() {
            return value;
        }
    }
    
    public enum ExchangeMethod {
        DECLARE(10),
        DECLARE_OK(11),
        DELETE(20),
        DELETE_OK(21),
        BIND(30),
        BIND_OK(31),
        UNBIND(40),
        UNBIND_OK(51);
        
        private final short value;
        
        ExchangeMethod(int value) {
            this.value = (short) value;
        }
        
        public short getValue() {
            return value;
        }
    }
    
    public enum QueueMethod {
        DECLARE(10),
        DECLARE_OK(11),
        BIND(20),
        BIND_OK(21),
        PURGE(30),
        PURGE_OK(31),
        DELETE(40),
        DELETE_OK(41),
        UNBIND(50),
        UNBIND_OK(51);
        
        private final short value;
        
        QueueMethod(int value) {
            this.value = (short) value;
        }
        
        public short getValue() {
            return value;
        }
    }
    
    public enum BasicMethod {
        QOS(10),
        QOS_OK(11),
        CONSUME(20),
        CONSUME_OK(21),
        CANCEL(30),
        CANCEL_OK(31),
        PUBLISH(40),
        RETURN(50),
        DELIVER(60),
        GET(70),
        GET_OK(71),
        GET_EMPTY(72),
        ACK(80),
        REJECT(90),
        RECOVER_ASYNC(100),
        RECOVER(110),
        RECOVER_OK(111),
        NACK(120);
        
        private final short value;
        
        BasicMethod(int value) {
            this.value = (short) value;
        }
        
        public short getValue() {
            return value;
        }
    }
}