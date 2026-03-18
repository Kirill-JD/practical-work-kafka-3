package ru.ycan.handler.impl;

import ru.ycan.handler.Handler;
import ru.ycan.kafka.KafkaProcess;

public class HandlerImpl implements Handler {
    private static final KafkaProcess KAFKA_PROCESS = new KafkaProcess();
    @Override
    public void startProcess() {
        KAFKA_PROCESS.start();
    }
}
