/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jrh3k5.flume.sink.http.server;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.glassfish.jersey.client.ClientConfig;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

/**
 * This is a client object used to interact with the HTTP server exposed by the {@link ServerSink}.
 * <p />
 * All instances of this class should {@link #close() closed} when done with them.
 * 
 * @author Joshua Hyde
 * @see ServerSink
 */

public class ServerSinkClient implements Closeable {
    private final Client client;
    private final WebTarget eventsTarget;

    /**
     * Create a client that communicates with an instance hosted on the local machine.
     * 
     * @param serverPort
     *            The port on which the HTTP server is listening for requests.
     */
    public ServerSinkClient(int serverPort) {
        this("localhost", serverPort);
    }

    /**
     * Create a client that communicates with an instance hosted on a specified machine.
     * 
     * @param host
     *            The host on which the HTTP server is hosted.
     * @param serverPort
     *            The port on which the HTTP server is listening for requests.
     */
    public ServerSinkClient(String host, int serverPort) {
        final Configuration clientConfig = new ClientConfig(JacksonJsonProvider.class);
        client = ClientBuilder.newClient(clientConfig);

        this.eventsTarget = client.target(String.format("http://%s:%d", host, serverPort)).path("events");
    }

    /**
     * Clear events stored in the HTTP server.
     */
    public void clearEvents() {
        eventsTarget.request().delete();
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    /**
     * Retrieve all events currently stored in the HTTP server.
     * 
     * @return A {@link List} of the events stored in the server.
     */
    public List<Event> getEvents() {
        final Event[] events = eventsTarget.request(MediaType.APPLICATION_JSON_TYPE).get(SimpleEvent[].class);
        return Arrays.asList(events);
    }
}
