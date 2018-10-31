package ru.mail.polis.litemn.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SequenceCommandProcessor<T> implements CommandProcessor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceCommandProcessor.class);

    private final ExecutorService executorService;

    SequenceCommandProcessor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public List<T> process(List<Command<T>> commands) {
        List<T> result = new ArrayList<>(commands.size());
        try {
            List<Future<T>> futures = executorService.invokeAll(commands);
            for (Future<T> future : futures) {
                T value = future.get();
                result.add(value);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.error("Error to process commands", e);
        }
        return result;
    }
}
