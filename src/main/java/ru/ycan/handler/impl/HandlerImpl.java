package ru.ycan.handler.impl;

import ru.ycan.handler.Handler;
import ru.ycan.kafka.KafkaProcess;

public class HandlerImpl implements Handler {
    private static final KafkaProcess CENSORSHIP_STATE_STORE = new KafkaProcess();
    @Override
    public void startProcess() {
        CENSORSHIP_STATE_STORE.start();
    }
}
