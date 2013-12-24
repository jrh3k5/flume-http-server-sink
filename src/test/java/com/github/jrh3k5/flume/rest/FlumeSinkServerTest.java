package com.github.jrh3k5.flume.rest;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.net.URI;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for {@link FlumeSinkServer}.
 * 
 * @author Joshua Hyde
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FlumeSinkServer.class, GrizzlyHttpServerFactory.class, ResourceConfig.class })
public class FlumeSinkServerTest {
    @Mock
    private HttpServer httpServer;
    private FlumeSinkServer sinkServer;

    /**
     * Set up the creation of the server.
     * 
     * @throws Exception
     */
    @Before
    public void setUpServer() throws Exception {
        final String bindAddress = "a.server";
        final int serverPort = 748392;

        final String resourcePackageName = FlumeSinkServerResource.class.getPackage().getName();
        final ResourceConfig resourceConfig = mock(ResourceConfig.class);
        whenNew(ResourceConfig.class).withNoArguments().thenReturn(resourceConfig);
        when(resourceConfig.packages(resourcePackageName)).thenReturn(resourceConfig);

        mockStatic(GrizzlyHttpServerFactory.class);
        when(GrizzlyHttpServerFactory.createHttpServer(URI.create(String.format("http://%s:%d", bindAddress, serverPort)), resourceConfig)).thenReturn(httpServer);

        sinkServer = new FlumeSinkServer(bindAddress, serverPort);

        verify(resourceConfig).packages(resourcePackageName);
    }

    /**
     * Test the starting of the server.
     * 
     * @throws Exception
     *             If any errors occur during the test run.
     */
    @Test
    public void testStart() throws Exception {
        sinkServer.start();
        verify(httpServer).start();
    }

    /**
     * Test the stopping of the server.
     * 
     * @throws Exception
     *             If any errors occur during the test run.
     */
    @Test
    public void testStop() throws Exception {
        sinkServer.stop();
        verify(httpServer).shutdown();
    }
}
