# flume-http-server-sink

A Flume sink that exposes events through an HTTP server.

Use the [ServerSink](./src/main/java/com/github/jrh3k5/flume/rest/ServerSink.java) in your agent configuration to use this sink.

## Retrieving Events

Events that are pulled from the Flume channel are retrievable in JSON format over HTTP. Refer to the Javadoc for [FlumeSinkServerResource](./src/main/java/com/github/jrh3k5/flume/rest/FlumeSinkServerResource.java) for more information on the contract.
