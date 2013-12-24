package com.github.jrh3k5.flume.rest;

import java.net.URI;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

public class FlumeSinkServer {
    private final HttpServer server;

    public FlumeSinkServer(int serverPort) {
        final ResourceConfig rc = new ResourceConfig().packages(FlumeSinkServerResource.class.getPackage().getName());
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(String.format("http://127.0.0.1:%d", serverPort)), rc);
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.shutdown();
    }
}
