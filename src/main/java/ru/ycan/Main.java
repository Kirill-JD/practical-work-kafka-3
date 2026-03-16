package ru.ycan;

import ru.ycan.handler.Handler;
import ru.ycan.handler.impl.HandlerImpl;

public class Main {
    public static void main(String... args) {
        Handler handler = new HandlerImpl();
        handler.startProcess();
    }
}