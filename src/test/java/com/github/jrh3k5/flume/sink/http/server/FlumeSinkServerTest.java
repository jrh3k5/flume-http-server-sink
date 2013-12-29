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

import static org.fest.assertions.Assertions.assertThat;
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

import com.github.jrh3k5.flume.sink.http.server.FlumeSinkServer;
import com.github.jrh3k5.flume.sink.http.server.FlumeSinkServerResource;

/**
 * Unit tests for {@link FlumeSinkServer}.
 * 
 * @author Joshua Hyde
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FlumeSinkServer.class, GrizzlyHttpServerFactory.class, ResourceConfig.class })
public class FlumeSinkServerTest {
    private final String bindAddress = "a.server";
    private final int serverPort = 748392;
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
     * Test the retrieval of the base URI.
     * 
     * @throws Exception
     *             If any errors occur during the test run.
     */
    @Test
    public void testGetBaseUri() throws Exception {
        assertThat(sinkServer.getBaseUri()).isEqualTo(URI.create(String.format("http://%s:%d", bindAddress, serverPort)));
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
