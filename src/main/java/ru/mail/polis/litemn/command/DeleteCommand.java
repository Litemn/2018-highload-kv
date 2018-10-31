package ru.mail.polis.litemn.command;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.mail.polis.litemn.KVServiceImpl.ENTITY_PATH;
import static ru.mail.polis.litemn.KVServiceImpl.INTERNAL_HEADER;

public class DeleteCommand extends Command<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteCommand.class);

    public DeleteCommand(HttpClient client, String id) {
        super(client, id);
    }

    @Override
    public Boolean call() {
        try {
            Response response = getClient().delete(ENTITY_PATH + "?id=" + getId(), INTERNAL_HEADER);
            return (response.getStatus() == 202);
        } catch (Exception e) {
            LOGGER.error("Error in del command for " + getId(), e);
            return false;
        }
    }
}
