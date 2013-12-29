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

import java.net.URI;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * This class manages the embedded server hosting the HTTP interface into events taken by the {@link ServerSink}.
 * 
 * @author Joshua Hyde
 */

public class FlumeSinkServer {
    private final HttpServer server;
    private final URI baseUri;

    /**
     * Create a server.
     * 
     * @param bindAddress
     *            The address or host to which the server is to bind.
     * @param serverPort
     *            The port on which the HTTP server will listen for requests.
     */
    public FlumeSinkServer(String bindAddress, int serverPort) {
        this.baseUri = URI.create(String.format("http://%s:%d", bindAddress, serverPort));
        final ResourceConfig resourceConfig = new ResourceConfig().packages(FlumeSinkServerResource.class.getPackage().getName());
        server = GrizzlyHttpServerFactory.createHttpServer(baseUri, resourceConfig);
    }

    /**
     * Get the base URI at which the server is accepting requests.
     * 
     * @return A {@link URI} representing the base URI.
     */
    public URI getBaseUri() {
        return baseUri;
    }

    /**
     * Start the server.
     * 
     * @throws Exception
     *             If any errors occur during the startup of the server.
     */
    public void start() throws Exception {
        server.start();
    }

    /**
     * Stop the server.
     * 
     * @throws Exception
     *             If any errors occur during the shutdown of the server.
     */
    public void stop() throws Exception {
        server.shutdown();
    }
}
