package ru.mail.polis.litemn.command;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.mail.polis.litemn.KVServiceImpl.ENTITY_PATH;
import static ru.mail.polis.litemn.KVServiceImpl.INTERNAL_HEADER;

public class PutCommand extends Command<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PutCommand.class);

    private final byte[] value;

    public PutCommand(HttpClient client, String id, byte[] value) {
        super(client, id);
        this.value = value;
    }

    @Override
    public Boolean call() {
        try {
            Response response = getClient().put(ENTITY_PATH + "?id=" + getId(), value, INTERNAL_HEADER);
            return (response.getStatus() == 201);
        } catch (Exception e) {
            LOGGER.error("Error in put command for " + getId(), e);
            return false;
        }
    }
}
