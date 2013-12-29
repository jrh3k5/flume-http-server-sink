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
package com.github.jrh3k5.flume.rest;

import static org.fest.assertions.Assertions.assertThat;

import java.net.URI;
import java.util.Collections;
import java.util.UUID;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.glassfish.jersey.client.ClientConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

/**
 * Integration tests for {@link FlumeSinkServer}.
 * 
 * @author Joshua Hyde
 */

public class FlumeSinkServerITest {
    private final String bindAddress = "0.0.0.0";
    private final int serverPort = 7894;
    private final FlumeSinkServer server = new FlumeSinkServer(bindAddress, serverPort);
    private Client client;

    /**
     * Set up the client for each test.
     * 
     * @throws Exception
     *             If any errors occur during the setup.
     */
    @Before
    public void setUpClient() throws Exception {
        final Configuration clientConfig = new ClientConfig(JacksonJsonProvider.class);
        client = ClientBuilder.newClient(clientConfig);

        // Clear any events that may have been stored by previous tests
        client.target(server.getBaseUri()).path("events").request().delete();
    }

    /**
     * Close the client after each test.
     * 
     * @throws Exception
     *             If any errors occur during the closure of the client.
     */
    @After
    public void closeClient() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    /**
     * Stop the server.
     * 
     * @throws Exception
     *             If any errors occur during the stopping of the server.
     */
    @After
    public void stopServer() throws Exception {
        server.stop();
    }

    /**
     * Test that the location header from posting events resolves to the correct location.
     * 
     * @throws Exception
     *             If any errors occur during the test run.
     */
    @Test
    public void testPostLocation() throws Exception {
        final SimpleEvent toPost = new SimpleEvent();
        toPost.setHeaders(Collections.singletonMap("a-header", UUID.randomUUID().toString()));
        toPost.setBody(UUID.randomUUID().toString().getBytes("utf-8"));

        final Response postResponse = client.target(server.getBaseUri()).path("events").request().post(Entity.entity(new Event[] { toPost }, MediaType.APPLICATION_JSON_TYPE));
        assertThat(postResponse.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());

        final URI postLocation = postResponse.getLocation();
        assertThat(postLocation).isNotNull();

        final Event[] gottenEvents = client.target(postLocation).request(MediaType.APPLICATION_JSON_TYPE).get(SimpleEvent[].class);
        assertThat(gottenEvents).hasSize(1);
        assertThat(gottenEvents[0].getHeaders()).isEqualTo(toPost.getHeaders());
        assertThat(gottenEvents[0].getBody()).isEqualTo(toPost.getBody());
    }
}
