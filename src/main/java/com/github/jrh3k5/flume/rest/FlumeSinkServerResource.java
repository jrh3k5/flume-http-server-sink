package com.github.jrh3k5.flume.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;

/**
 * A JAX-RS resource that accepts and exposes Flume events via JSON.
 * <p />
 * Events - accepted and retrieved - follow this form:
 * 
 * <pre>
 * [
 *   {
 *     "headers": {
 *       "header_one" : "Header #1",
 *       "header_two" : "Header #2"
 *     }
 *     "body" : "&lt;base64-encoded form of the body&gt;"
 *   }
 *   ...
 * ]
 * </pre>
 * 
 * @author Joshua Hyde
 * 
 */

@Path("/")
public class FlumeSinkServerResource {
    private static final List<Event> EVENTS = new ArrayList<Event>();
    private final ReadWriteLock eventsLock = new ReentrantReadWriteLock();

    /**
     * Delete all stored events.
     * 
     * @return A {@link Response} indicating the status of the deletion attempt.
     * @throws Exception
     *             If any errors occur during the deletion.
     */
    @DELETE
    @Path("events")
    public Response deleteEvents() throws Exception {
        final Lock writeLock = eventsLock.writeLock();
        writeLock.lock();
        try {
            EVENTS.clear();
            return Response.ok().build();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Get all stored events.
     * 
     * @return A {@link Response} containing an array of the stored events.
     * @throws Exception
     *             If any errors occur during the retrieval.
     */
    @GET
    @Path("events")
    @Produces("application/json")
    public Response getEvents() throws Exception {
        final Lock readLock = eventsLock.readLock();
        readLock.lock();
        try {
            return Response.ok(EVENTS.toArray()).build();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Store events in the server.
     * 
     * @param incomingEvents
     *            An array of {@link SimpleEvent} objects representing the events to store.
     * @param uriInfo
     *            A {@link UriInfo} object representing the URI information of the current request.
     * @return A {@link Response} indicating the state of the storage.
     * @throws Exception
     *             If any errors occur during the storage of data.
     */
    @POST
    @Path("events")
    @Consumes("application/json")
    public Response storeEvents(SimpleEvent[] incomingEvents, @Context UriInfo uriInfo) throws Exception {
        final Lock writeLock = eventsLock.writeLock();
        writeLock.lock();
        try {
            for (SimpleEvent incomingEvent : incomingEvents) {
                EVENTS.add(incomingEvent);
            }
            return Response.created(uriInfo.getBaseUri().resolve("events")).build();
        } finally {
            writeLock.unlock();
        }
    }
}
