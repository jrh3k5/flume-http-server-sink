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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Unit tests for {@link FlumeSinkServerResource}.
 * 
 * @author Joshua Hyde
 */

@RunWith(MockitoJUnitRunner.class)
public class FlumeSinkServerResourceTest {
    private final FlumeSinkServerResource resource = new FlumeSinkServerResource();
    private final URI baseUri = URI.create("http://localhost:8080");
    @Mock
    private UriInfo uriInfo;

    /**
     * Set up the {@link UriInfo} object for each test.
     */
    @Before
    public void setUpUriInfo() {
        when(uriInfo.getBaseUri()).thenReturn(baseUri);
    }

    /**
     * Test the deletion of events.
     * 
     * @throws Exception
     *             If any errors occur during the test run.
     */
    @Test
    public void testDeleteEvents() throws Exception {
        final SimpleEvent stored = mock(SimpleEvent.class);
        resource.storeEvents(new SimpleEvent[] { stored }, uriInfo);

        assertThat(resource.getEvents().getEntity()).isEqualTo(new Event[] { stored });
        resource.deleteEvents();
        assertThat((Object[]) resource.getEvents().getEntity()).isEmpty();
    }

    /**
     * Test the retrieval of events.
     * 
     * @throws Exception
     *             If any errors occur during the test run.
     */
    @Test
    public void testGetEvents() throws Exception {
        final SimpleEvent stored = mock(SimpleEvent.class);
        resource.storeEvents(new SimpleEvent[] { stored }, uriInfo);

        final Response getResponse = resource.getEvents();
        assertThat(getResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        assertThat(getResponse.getEntity()).isEqualTo(new Event[] { stored });
    }

    /**
     * Test the storage of events.
     * 
     * @throws Exception
     *             If any errors occur during the test run.
     */
    @Test
    public void testStoreEvents() throws Exception {
        final SimpleEvent stored = mock(SimpleEvent.class);
        final Response postResponse = resource.storeEvents(new SimpleEvent[] { stored }, uriInfo);
        assertThat(postResponse.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
        assertThat(postResponse.getLocation()).isEqualTo(baseUri.resolve("events"));
    }
}
