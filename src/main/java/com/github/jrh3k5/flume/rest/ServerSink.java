package com.github.jrh3k5.flume.rest;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
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

public class ServerSink extends AbstractSink implements Configurable {
    private int serverPort = 1337;
    private Client client;
    private int batchSize = 1000;
    private FlumeSinkServer server;

    @Override
    public synchronized void start() {
        super.start();

        try {
            server.start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start the Flume sink server.", e);
        }
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
            client.target("http://localhost:1337").path("events").request(MediaType.WILDCARD_TYPE).post(Entity.entity(toSend, MediaType.APPLICATION_JSON));
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
        } finally {
            transaction.close();
        }
        return status;
    }

    public void configure(Context context) {
        server = new FlumeSinkServer(serverPort);

        final Configuration clientConfig = new ClientConfig(JacksonJsonProvider.class);
        client = ClientBuilder.newClient(clientConfig);
    }
}
