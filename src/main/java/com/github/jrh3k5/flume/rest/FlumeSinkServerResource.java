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

@Path("/")
public class FlumeSinkServerResource {
    private static final List<Event> EVENTS = new ArrayList<Event>();
    private final ReadWriteLock eventsLock = new ReentrantReadWriteLock();

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
}
