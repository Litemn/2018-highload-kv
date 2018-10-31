package ru.mail.polis.litemn.command;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.litemn.StorageValue;

import static one.nio.serial.Serializer.deserialize;
import static ru.mail.polis.litemn.KVServiceImpl.ENTITY_PATH;
import static ru.mail.polis.litemn.KVServiceImpl.INTERNAL_HEADER;

public class GetCommand extends Command<StorageValue> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetCommand.class);

    public GetCommand(HttpClient client, String id) {
        super(client, id);
    }

    @Override
    public StorageValue call() {
        try {
            Response response = getClient().get(ENTITY_PATH + "?id=" + getId(), INTERNAL_HEADER);
            byte[] body = response.getBody();
            return (StorageValue) deserialize(body);
        } catch (Exception e) {
            LOGGER.error("Error in get command for " + getId(), e);
            return StorageValue.error();
        }
    }
}
