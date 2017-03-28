# akka-java-persistence
Examples of Event Sourcing and CQRS
using [Akka Persistence](http://doc.akka.io/docs/akka/2.4.17/java/persistence.html)
and [Akka Persistence Query](http://doc.akka.io/docs/akka/2.4.17/java/persistence-query.html)
in Java.

To Cassandra is required to run these examples. Install Cassandra and run it, the examples will automatically
connect and create the necessary tables.

Run the ExampleEventSourcing class to trigger the creation of some events. This example shows commands that
are processed, which results in the creation and persistence of the associated events. This example stops after
running for about one minute.

Run the ExampleCqrs class to trigger retrieving the events from the event log. This example must be manually
stopped. Currently, this example only retrieves the events. A future version may show persisting the events
in a queryable form.

Both ExampleEventSourcing and ExampleCqrs may be run together. They both may be run multiple times.