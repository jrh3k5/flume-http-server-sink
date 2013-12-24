package com.github.jrh3k5.flume.rest;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.glassfish.jersey.client.ClientConfig;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

/**
 * This is a sink that exposes the events it receives via an HTTP server.
 * <p />
 * It takes the following configuration parameters:
 * <ul>
 * <li><b>http.server.port</b>: The port on which the HTTP server that exposes its received events listens for requests (default 1337)</li>
 * <li><b>http.server.address.bind</b>: The host to which the HTTP server binds itself (default 0.0.0.0)</li>
 * <li><b>batchSize</b>: The size of the batches that the sink should pull events out of the channel and make available via the HTTP server (default 1000)</li>
 * 
 * </ul>
 * 
 * @author Joshua Hyde
 */

public class ServerSink extends AbstractSink implements Configurable {
    private int serverPort;
    private String bindAddress;
    private Client client;
    private int batchSize;
    private FlumeSinkServer server;
    private WebTarget eventsTarget;

    @Override
    public void configure(Context context) {
        serverPort = context.getInteger("server.http.port", 1337);
        batchSize = context.getInteger("batchSize", 1000);
        bindAddress = context.getString("http.server.address.bind", "0.0.0.0");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        final Channel channel = getChannel();
        final Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            final List<Event> toSend = new ArrayList<Event>();
            for (int i = 0; i < batchSize; i++) {
                final Event event = channel.take();
                if (event == null) {
                    status = Status.BACKOFF;
                    break;
                }
                toSend.add(event);
            }
            eventsTarget.request(MediaType.WILDCARD_TYPE).post(Entity.entity(toSend, MediaType.APPLICATION_JSON));
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
        } finally {
            transaction.close();
        }
        return status;
    }

    @Override
    public synchronized void start() {
        super.start();

        server = new FlumeSinkServer(bindAddress, serverPort);
        try {
            server.start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start the Flume sink server.", e);
        }

        final Configuration clientConfig = new ClientConfig(JacksonJsonProvider.class);
        client = ClientBuilder.newClient(clientConfig);
        eventsTarget = client.target(String.format("http://%s:%d", bindAddress, serverPort)).path("events");
    }

    @Override
    public synchronized void stop() {
        try {
            server.stop();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to stop the Flume sink server.", e);
        }

        super.stop();
    }
}
