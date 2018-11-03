package ru.mail.polis.litemn;

import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.litemn.command.CommandProcessor;
import ru.mail.polis.litemn.command.CommandProcessorFactory;
import ru.mail.polis.litemn.command.DeleteCommand;
import ru.mail.polis.litemn.command.GetCommand;
import ru.mail.polis.litemn.command.PutCommand;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
import static one.nio.serial.Serializer.serialize;

/**
 * one-nio KV service impl
 */
public class KVServiceImpl extends HttpServer implements KVService {
    public static final String ENTITY_PATH = "/v0/entity";
    public static final String STATUS_PATH = "/v0/status";
    public static final String INTERNAL_HEADER = "X-Internal-header: true";
    private static final Logger LOGGER = LoggerFactory.getLogger(KVServiceImpl.class);
    @NotNull
    private final Map<String, HttpClient> clients;
    @NotNull
    private final List<String> hosts;
    @NotNull
    private final ExecutorService executor;
    @NotNull
    private final KVDaoRocksDB dao;
    @NotNull
    private final RF quorum;
    private final CommandProcessor<Boolean> processor;
    private final CommandProcessor<StorageValue> getProcessor;

    public KVServiceImpl(int port, @NotNull KVDao dao, @NotNull final Set<String> topology) throws IOException {
        super(create(port));
        this.dao = (KVDaoRocksDB) dao;
        this.quorum = RF.quorum(topology.size());
        clients = new HashMap<>(topology.size() - 1);
        hosts = new ArrayList<>(topology.size() - 1);
        for (String host : topology) {
            int indexOf = host.lastIndexOf(':');
            if (indexOf == -1) {
                throw new IllegalArgumentException("Wrong host of server " + host);
            }
            if (Integer.valueOf(host.substring(indexOf + 1)) == port) {
                continue;
            }
            hosts.add(host);
            clients.put(host, new HttpClient(new ConnectionString(host)));
        }
        hosts.sort(String::compareTo);
        executor = Executors.newWorkStealingPool();
        processor = CommandProcessorFactory.newCommandProcessor(executor);
        getProcessor = CommandProcessorFactory.newCommandProcessor(executor);
    }

    private static HttpServerConfig create(int port) {
        AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    @Path(value = STATUS_PATH)
    public Response status(Request request) {
        if (request.getMethod() != Request.METHOD_GET) {
            LOGGER.debug("Bad request method {} for status", request.getMethod());
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        LOGGER.debug("Response status for host {}", request.getHost());
        return Response.ok(Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        RF rf = quorum;
        boolean internal = request.getHeader(INTERNAL_HEADER) != null;
        LOGGER.debug("Request: {}", request);
        if (!request.getPath().equals(ENTITY_PATH)) {
            LOGGER.debug("Bad path {}", request.getPath());
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            LOGGER.debug("Missing id param for {}", request.getPath());
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        String replicas = request.getParameter("replicas=");
        if (replicas != null) {
            try {
                rf = RF.from(replicas);
            } catch (IllegalArgumentException e) {
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
                return;
            }
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                if (internal) {
                    getInternal(session, id);
                } else {
                    get(session, id, rf);
                }
                break;
            case Request.METHOD_PUT:
                put(request, session, id, internal, rf);
                break;
            case Request.METHOD_DELETE:
                delete(session, id, internal, rf);
                break;
            default:
                LOGGER.debug("Bad request method {} for {}", request.getMethod(), request.getPath());
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        }
    }

    private void getInternal(final @NotNull HttpSession session, final @NotNull String id) throws IOException {
        try {
            LOGGER.debug("Request get entity by id {}", id);
            StorageValue value = dao.getInternal(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name())));
            if (value.getState() == StorageValue.State.ABSENT || value.getState() == StorageValue.State.REMOVED) {
                LOGGER.debug("Entity by id {} not found", id);
                session.sendResponse(new Response(Response.NOT_FOUND, serialize(value)));
            } else if (value.getState() == StorageValue.State.EXISTS) {
                session.sendResponse(new Response(Response.OK, serialize(value)));
            } else if (value.getState() == StorageValue.State.ERROR) {
                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, serialize(value)));
            }
        } catch (Exception e) {
            LOGGER.error("Fail to process get request by id {}", id);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private void get(final @NotNull HttpSession session,
                     final @NotNull String id,
                     @NotNull RF rf) throws IOException {
        try {
            LOGGER.debug("Request get entity by id {}", id);
            StorageValue value;
            List<StorageValue> results = getProcessor.process(
                    getHosts(rf).stream().map(h -> new GetCommand(clients.get(h), id)).collect(toList())
            );
            results.add(dao.getInternal(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name()))));
            value = results.stream()
                    .filter(s -> s.getState() != StorageValue.State.ABSENT)
                    .max(Comparator.comparingLong(StorageValue::getTime))
                    .orElseGet(StorageValue::absent);
            long ackCount = results.stream().filter(s -> s.getState() != StorageValue.State.ERROR).count();
            switch (value.getState()) {
                case EXISTS:
                    if (ackCount < rf.getAck()) {
                        session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                    } else {
                        session.sendResponse(new Response(Response.OK, value.getValue()));
                    }
                    return;
                case ABSENT:
                case REMOVED:
                    LOGGER.debug("Entity by id {} not found", id);
                    session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
                    return;
                case ERROR:
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                    return;
                default:
                    throw new IllegalStateException();
            }

        } catch (Exception e) {
            LOGGER.error("Fail to process get request by id " + id, e);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private void delete(final @NotNull HttpSession session,
                        final @NotNull String id,
                        final boolean internal,
                        final @NotNull RF rf) throws IOException {
        try {
            LOGGER.debug("Remove entity by id {}", id);
            if (!internal && !hosts.isEmpty()) {
                List<Boolean> result = processor.process(
                        getHosts(rf).stream().map(h -> new DeleteCommand(clients.get(h), id)).collect(toList())
                );
                result.add(dao.removeInternal(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name()))));
                if (result.stream().filter(Predicate.isEqual(true)).count() < rf.getAck()) {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                } else {
                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                }
            } else {
                dao.remove(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name())));
                session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
            }
        } catch (Exception e) {
            LOGGER.error("Error when remove by id " + id, e);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private void put(
            final @NotNull Request request,
            final @NotNull HttpSession session,
            final @NotNull String id,
            final boolean internal,
            final RF rf) throws IOException {
        try {
            byte[] body = request.getBody();
            LOGGER.debug("Put entity by id {}", id);
            if (!internal) {
                List<Boolean> result = processor.process(
                        getHosts(rf).stream().map(h -> new PutCommand(clients.get(h), id, body)).collect(toList()));
                result.add(dao.upsertInternal(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name())), body));
                if (result.stream().filter(Predicate.isEqual(true)).count() < rf.getAck()) {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                } else {
                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                }
            } else {
                dao.upsert(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name())), body);
                session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
            }
        } catch (Exception e) {
            LOGGER.error("Error when put by id " + id, e);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private List<String> getHosts(RF rf) {
        return hosts.subList(0, rf.getFrom() - 1);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdownNow();
    }
}
