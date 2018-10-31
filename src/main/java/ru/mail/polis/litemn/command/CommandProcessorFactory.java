package ru.mail.polis.litemn.command;

import java.util.concurrent.ExecutorService;

public class CommandProcessorFactory {

    private CommandProcessorFactory() {
    }

    public static <T> CommandProcessor<T> newCommandProcessor(ExecutorService executorService) {
        return new SequenceCommandProcessor<>(executorService);
    }
}
