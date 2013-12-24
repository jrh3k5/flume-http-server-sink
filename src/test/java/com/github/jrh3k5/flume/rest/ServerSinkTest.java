package com.github.jrh3k5.flume.rest;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.SimpleEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ServerSinkTest {
    private final MemoryChannel channel = new MemoryChannel();
    private final ServerSink serverSink = new ServerSink();
    private final Context context = new Context();
    private final ServerSinkClient sinkClient = new ServerSinkClient(1337);

    @Before
    public void startSink() throws Exception {
        channel.configure(context);
        channel.start();

        serverSink.configure(context);
        serverSink.setChannel(channel);
        serverSink.start();
    }

    @After
    public void closeSink() throws Exception {
        sinkClient.close();

        serverSink.stop();
        channel.stop();
    }

    /**
     * Test the retrieval of events from the sink.
     * 
     * @throws Exception
     *             If any errors occur during the test run.
     */
    @Test
    public void testGetEvents() throws Exception {
        final Event toPut = new SimpleEvent();
        toPut.setHeaders(Collections.singletonMap("testGetEvents", UUID.randomUUID().toString()));
        toPut.setBody(UUID.randomUUID().toString().getBytes("utf-8"));

        // Store the event in the channel so that the sink sees it
        final Transaction putTransaction = channel.getTransaction();
        try {
            putTransaction.begin();
            channel.put(toPut);
            putTransaction.commit();
        } finally {
            putTransaction.close();
        }

        // Make sure the event is pulled
        serverSink.process();

        final List<Event> storedEvents = sinkClient.getEvents();
        assertThat(storedEvents).hasSize(1);
        final Event gotten = storedEvents.get(0);
        assertThat(gotten.getHeaders()).isEqualTo(toPut.getHeaders());
        assertThat(gotten.getBody()).isEqualTo(toPut.getBody());

        sinkClient.clearEvents();

        assertThat(sinkClient.getEvents()).isEmpty();
    }
}
