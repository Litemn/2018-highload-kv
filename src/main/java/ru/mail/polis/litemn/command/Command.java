package ru.mail.polis.litemn.command;

import one.nio.http.HttpClient;

import java.util.concurrent.Callable;

abstract class Command<T> implements Callable<T> {

    private final HttpClient client;
    private final String id;

    Command(HttpClient client, String id) {
        this.client = client;
        this.id = id;
    }

    HttpClient getClient() {
        return client;
    }

    String getId() {
        return id;
    }
}
