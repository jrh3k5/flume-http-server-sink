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
