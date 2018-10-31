package ru.mail.polis.litemn.command;

import java.util.List;

public interface CommandProcessor<T> {
    List<T> process(List<Command<T>> commands);
}
