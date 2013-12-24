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

    /**
     * Create a server.
     * 
     * @param bindAddress
     *            The address or host to which the server is to bind.
     * @param serverPort
     *            The port on which the HTTP server will listen for requests.
     */
    public FlumeSinkServer(String bindAddress, int serverPort) {
        final ResourceConfig rc = new ResourceConfig().packages(FlumeSinkServerResource.class.getPackage().getName());
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(String.format("http://%s:%d", bindAddress, serverPort)), rc);
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
