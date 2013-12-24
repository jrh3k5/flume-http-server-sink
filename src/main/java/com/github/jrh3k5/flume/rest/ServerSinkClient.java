package com.github.jrh3k5.flume.rest;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.glassfish.jersey.client.ClientConfig;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

public class ServerSinkClient implements Closeable {
    private final int serverPort;
    private final Client client;

    public ServerSinkClient(int serverPort) {
        final Configuration clientConfig = new ClientConfig(JacksonJsonProvider.class);
        client = ClientBuilder.newClient(clientConfig);
        this.serverPort = serverPort;
    }

    public List<Event> getEvents() {
        final Event[] events = client.target(String.format("http://localhost:%d", serverPort)).path("events").request(MediaType.APPLICATION_JSON_TYPE).get(SimpleEvent[].class);
        return Arrays.asList(events);
    }

    public void clearEvents() {
        client.target(String.format("http://localhost:%d", serverPort)).path("events").request().delete();
    }

    public void close() throws IOException {
        client.close();
    }
}
