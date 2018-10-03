package ru.mail.polis.litemn;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;

/**
 * one-nio KV service impl
 */
public class KVServiceImpl extends HttpServer implements KVService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVServiceImpl.class);
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    @NotNull
    private final KVDao dao;

    public KVServiceImpl(int port, @NotNull KVDao dao) throws IOException {
        super(create(port));
        this.dao = dao;
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
        LOGGER.debug("Request: method={}, path={}, host={} ", request.getMethod(), request.getPath(), request.getHost());
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
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                get(session, id);
                break;
            case Request.METHOD_PUT:
                put(request, session, id);
                break;
            case Request.METHOD_DELETE:
                delete(session, id);
                break;
            default:
                LOGGER.debug("Bad request method {} for {}", request.getMethod(), request.getPath());
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        }
    }

    private void get(final @NotNull HttpSession session, final @NotNull String id) throws IOException {
        try {
            LOGGER.debug("Request get entity by id {}", id);
            byte[] bytes = dao.get(id.getBytes(Charset.forName("UTF-8")));
            session.sendResponse(new Response(Response.OK, bytes));
        } catch (NoSuchElementException e) {
            LOGGER.debug("Entity by id {} not found", id);
            session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
        } catch (Exception e) {
            LOGGER.error("Fail to process get request by id {}", id);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private void delete(final @NotNull HttpSession session, final @NotNull String id) throws IOException {
        try {
            LOGGER.debug("Remove entity by id {}", id);
            dao.remove(id.getBytes(Charset.forName("UTF-8")));
            session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
        } catch (Exception e) {
            LOGGER.error("Error when remove by id {}", id);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private void put(
            final @NotNull Request request,
            final @NotNull HttpSession session,
            final @NotNull String id) throws IOException {
        try {
            LOGGER.debug("Put entity by id {}", id);
            dao.upsert(id.getBytes(Charset.forName("UTF-8")), request.getBody());
            session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
        } catch (Exception e) {
            LOGGER.error("Error when put by id {}", id);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }
}
