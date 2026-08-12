---
title: "NATS Transport"
description: "Configure the NATS JetStream transport in Mocha, including shared stream ownership, consumer provisioning, subject naming, request/reply over core NATS, and scheduled messages."
---

The NATS transport connects Mocha to a NATS server running JetStream. It provisions streams and durable consumers, acknowledges messages, supports request/reply over core NATS, and delegates scheduled delivery to the broker.

# Set up the NATS transport

By the end of this section, you will have a Mocha bus publishing and consuming over JetStream.

## Install the package

```bash
dotnet add package Mocha.Transport.Nats
```

## Register with .NET Aspire

The most common setup uses the Aspire NATS component for connection management:

```bash
dotnet add package Aspire.NATS.Net
```

```csharp
using Mocha;
using Mocha.Transport.Nats;

var builder = WebApplication.CreateBuilder(args);

// Aspire registers INatsConnection from the "nats" connection resource
builder.AddNatsClient("nats");

builder.Services
    .AddMessageBus()
    .AddEventHandler<OrderPlacedEventHandler>()
    .AddNats(nats => nats.ServiceName("order-service"));

var app = builder.Build();
app.Run();
```

`.AddNats()` picks up the `INatsConnection` from dependency injection and uses it for both publishing and consuming. NATS.Net owns reconnection, so the transport has no connection manager of its own.

## Register with a manual connection

```csharp
using Mocha;
using Mocha.Transport.Nats;
using NATS.Client.Core;

builder.Services.AddSingleton<INatsConnection>(_ => new NatsConnection(new NatsOpts
{
    Url = "nats://localhost:4222",

    // NATS.Net drops messages when a subscriber falls behind. Request/reply responses arrive on a
    // core subscription, so leaving the default in place can silently lose them.
    SubPendingChannelFullMode = BoundedChannelFullMode.Wait
}));

builder.Services
    .AddMessageBus()
    .AddEventHandler<OrderPlacedEventHandler>()
    .AddNats(nats => nats.ServiceName("order-service"));
```

The transport logs a warning at start-up if the connection is left on the dropping default.

## Use a custom connection provider

Supply your own provider when the connection is not resolvable from dependency injection, or when the host and port reported in endpoint addresses need to differ from the connection string:

```csharp
.AddNats(nats => nats.ConnectionProvider(services =>
    new NatsConnectionProvider(services.GetRequiredKeyedService<INatsConnection>("primary"))))
```

Only the first server in a multi-server connection string is used for endpoint addresses, because the transport base address has to be a single stable value.

## Verify it works

Start the bus and publish an event. The transport provisions its stream and one durable consumer per handler before any endpoint starts, so a successful start-up means the topology is in place:

```csharp
await bus.PublishAsync(new OrderPlaced(orderId, "Mechanical Keyboard"));
```

# How Mocha concepts map onto JetStream

| Mocha                          | JetStream                                      |
| ------------------------------ | ---------------------------------------------- |
| Exchange, fan-out              | Stream `Subjects` with `*` and `>` wildcards   |
| Queue                          | Durable pull consumer                          |
| Binding, routing key list      | `ConsumerConfig.FilterSubjects`                |
| Competing consumers            | Several instances sharing one durable consumer |
| Concurrency                    | Parallel handling bounded by `MaxConcurrency`  |
| Prefetch                       | Local buffer, also bounded by `MaxConcurrency` |
| Back pressure across instances | `MaxAckPending`                                |
| Ack, nack                      | `AckAsync`, `NakAsync`                         |
| Retry backoff                  | `MaxDeliver` and `ConsumerConfig.Backoff`      |
| Reply endpoints                | Core NATS request/reply, not JetStream         |

`Send` and `Publish` converge on a single subject. Subscribers select what they receive through consumer filter subjects, so there is no equivalent of the exchange-to-exchange-to-queue convention chain the RabbitMQ transport builds.

# Naming

Streams are containers; all routing happens by subject, and the stream name never appears on the publish path.

| Concept          | Derived from                                 | Example                          |
| ---------------- | -------------------------------------------- | -------------------------------- |
| Stream           | The transport's service name                 | `ORDER_SERVICE`                  |
| Subject          | The message type's namespace and name        | `contracts.orders.order-created` |
| Durable consumer | The host's service name and the handler name | `order-service_order-created`    |

Endpoint names contain dots, which are the natural subject separator. Dots are illegal in stream and consumer names, along with `*`, `>`, whitespace and path separators, so derived names replace them with underscores. NATS uses these names as storage directory names and recommends keeping them under 32 characters; the transport logs a warning at start-up when a derived name is longer.

## Two service names control different things

These are separate settings and it matters which one you set:

- `nats.ServiceName("order-service")` names the **convention stream** only.
- The **host** service name scopes **durable consumer names**. It comes from the messaging host builder, else the `SERVICE_NAME` environment variable, else `OTEL_SERVICE_NAME`, else the entry assembly name.

Two services that end up with the same host service name derive the same durable name for a handler of the same type, share one durable, and compete for messages instead of each receiving a copy. The transport logs a warning when the two names disagree.

The simplest way to set the host name is the `SERVICE_NAME` environment variable, which most deployments already set. To set it in code:

```csharp
var bus = builder.Services.AddMessageBus();

// Scopes durable consumer names
bus.ConfigureMessageBus(mocha => mocha.Host(host => host.ServiceName("order-service")));

// Names the convention stream
bus.AddEventHandler<OrderPlacedEventHandler>()
    .AddNats(nats => nats.ServiceName("order-service"));
```

# Streams are shared

Subjects are derived from the message type, so every service that touches a message type derives the same subject. JetStream requires a stream's subjects to be disjoint from every other stream's, which means one subject belongs to exactly one stream no matter how many services publish to or consume from it.

The transport therefore claims only what nothing else has claimed:

1. For each subject the service publishes, it asks the server whether a stream already captures it.
2. Subjects that are already captured are bound to the owning stream, and nothing is declared for them.
3. Only the remaining subjects go into this service's own convention stream.

The first service to start creates the stream; later services bind to it. Two services subscribing to the same event both start cleanly, and each gets its own durable consumer on the shared stream. If two services race, the loser yields and binds to the winner's stream.

Because a convention stream is shared, this also means:

- Its **retention, storage, limits and replicas are set by whichever service created it**. A service adding subjects to an existing stream never rewrites those, and the server rejects an attempt to change storage outright.
- Its **subject list only ever grows**. A service that rolls out publishing fewer subjects will not strip the ones its peers still publish to.

> [!WARNING]
> Deriving stream names from the service name means `order-service` and `order.service` both produce `ORDER_SERVICE`. Two services whose names differ only by a separator will share a stream by accident rather than by subject ownership.

## Declaring a stream under the derived name

Declaring a stream named the same as the one the service name derives is the common case, and the two are folded together: the subjects the convention stream would have claimed are added to the declaration, while the retention, storage and limits you declared are kept. Handlers you did not put on a named endpoint still get their subjects captured.

A declared stream is never silently discarded. If any of its subjects are already owned by another stream, start-up fails naming both the subject and the owning stream, because JetStream requires stream subjects to be disjoint:

```
Stream 'ORDER_SERVICE' cannot be provisioned because its subjects overlap a stream
that already exists: 'orders_error' is already captured by stream 'ORDER_FAULTS'.
```

Resolve it by removing the overlapping subject from the declaration, deleting the stream that owns it, or dropping the declaration and letting the transport bind to the existing stream.

# Handling a family of messages on one endpoint

A handler bound to an interface or base type does not receive its implementations by default. A publish resolves its subject from the **concrete runtime type**, so `PublishAsync<IOrderCommand>(command)` and `PublishAsync(command)` behave identically: both go to the concrete type's subject. The generic argument does not select the subject.

To funnel a family onto one endpoint, name the concrete subjects:

```csharp
nats.Endpoint("order-commands")
    .Handler<OrderCommandHandler>()          // IEventHandler<IOrderCommand>
    .Subject("contracts.orders.cancel-order")
    .Subject("contracts.orders.hold-order")
    // Ordered delivery comes from the single durable; ordered handling needs this.
    .MaxConcurrency(1);
```

Every implementation needs its own `Subject` call. They cannot be discovered automatically, because message types are completed after topology is discovered, so their base types are not yet known when subject filters are built.

The handler still receives each message typed as the interface: the envelope carries its enclosed types, and the receive pipeline selects the handler from those. Because one durable on one stream delivers in order, this is also the only arrangement that orders a whole family relative to itself, which several consumers cannot do.

# Which stream does a consumer read from?

A JetStream consumer must be created on the stream that captures its subject, and that stream may belong to another service entirely. RabbitMQ has no equivalent constraint: a subscriber declares a queue and binds it to an exchange without knowing anything else about the publisher.

The transport resolves this at start-up by asking the server which stream captures each subscribed subject, so subscribers keep declaring _what_ they consume rather than _where it lives_. Failures are start-up errors rather than silence:

- No stream captures the subject, meaning the publishing service has not been deployed or its stream was never provisioned.
- Several streams capture it, so the choice would be arbitrary.
- One consumer's subjects are spread across different streams, which a single consumer cannot read.

Declaring the stream removes the start-up ordering dependency entirely, because whichever service starts first provisions it:

```csharp
.AddNats(nats => nats
    .ServiceName("shipping-service")
    .DeclareStream("ORDER_SERVICE")
        .Subject("contracts.orders.>"));
```

A declared stream and the convention stream coexist. Declaring the publisher's stream does not stop this service getting a stream for the subjects it publishes itself.

# Publishing to an uncaptured subject

A JetStream publish waits for an acknowledgement from the stream capturing the subject. If no stream captures it, the call does not fail immediately, it times out. The transport therefore verifies subject coverage while starting and fails with the offending subject named, which is considerably easier to diagnose than a timeout in production.

# Control auto-provisioning

Auto-provisioning is a development convenience. In production, prefer topology managed by whoever operates the cluster, so that retention and limits are a deliberate decision rather than a consequence of which pod started first:

```csharp
.AddNats(nats => nats.ServiceName("order-service").AutoProvision(false))
```

With auto-provisioning off, the transport creates nothing and binds to the streams that already exist. Subject verification still runs, so a missing stream is a start-up failure rather than a publish timeout.

Auto-provisioning can also be overridden per resource:

```csharp
.AddNats(nats => nats
    .ServiceName("order-service")
    .AutoProvision(false)
    .DeclareStream("ORDER_SERVICE")
        .Subject("contracts.orders.>")
        .AutoProvision(true))
```

# Configure endpoints

An endpoint is the receive side: a durable consumer and the subjects it filters. Naming it explicitly keeps the durable name off the handler type name:

```csharp
.AddNats(nats => nats
    .ServiceName("order-service")
    .Endpoint("order-processing")
    .Handler<OrderPlacedEventHandler>()
    .MaxConcurrency(10))
```

| Method                       | Effect                                                               |
| ---------------------------- | -------------------------------------------------------------------- |
| `Handler<T>` / `Consumer<T>` | Places a handler on this endpoint                                    |
| `Receives<T>`                | Binds a message type without naming a handler                        |
| `Subject`                    | Adds a subject filter beyond those derived from handlers             |
| `ConsumerName`               | Sets the durable name, which defaults to the sanitised endpoint name |
| `FromStream`                 | Reads from a named stream instead of resolving one at start-up       |
| `MaxConcurrency`             | Bounds parallel handling and the local buffer                        |

# Declare topology resources

`DeclareStream` and `DeclareConsumer` configure JetStream resources directly, for settings the endpoint API does not expose:

```csharp
.AddNats(nats =>
{
    nats.ServiceName("order-service")
        .Endpoint("order-processing")
        .Handler<OrderPlacedEventHandler>();

    nats.DeclareStream("ORDER_SERVICE")
        .Subject("contracts.orders.>")
        .Retention(StreamConfigRetention.Interest)
        .MaxAge(TimeSpan.FromDays(7))
        .MaxMessages(1_000_000)
        .Replicas(3);

    nats.DeclareConsumer("order-processing")
        .AckWait(TimeSpan.FromSeconds(30))
        .MaxDeliver(5)
        .Backoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
})
```

Naming a consumer that an endpoint already derives folds the two together: the filter subjects the endpoint contributes are kept, and the settings declared here win. That is the intended way to reach `AckWait`, `MaxDeliver`, `Backoff` and `AckProgressEvery` for a convention endpoint.

# Concurrency

`MaxConcurrency` bounds how many messages one instance handles at a time, and how many it buffers locally. It is deliberately not mapped onto `MaxAckPending`, which is the server-side ceiling shared by every instance reading the durable; lowering that to one instance's concurrency would starve the others.

Buffering matters here. A pulled message counts as delivered the moment it reaches the local buffer, so its acknowledgement deadline is already running while it waits for a free handler. Buffering far more than can be handled concurrently would expire the tail of the buffer and have it redelivered before it was ever handled.

# Deduplication

Deduplication is scoped to the stream, not to the subject, and it cannot be turned off from the client: a zero `DuplicateWindow` is omitted from the request and the server applies its own default.

The transport therefore writes a `Nats-Msg-Id` qualified by destination subject and carries the message identifier separately. Without that, republishing a message inside the same stream, which is exactly what dead-lettering does, is discarded as a duplicate and the publish still reports success.

# Long-running handlers

A handler that runs longer than the consumer's `AckWait` is redelivered while it is still working. JetStream can extend the deadline, which has no RabbitMQ equivalent:

```csharp
nats.DeclareConsumer("order-processing")
    .AckWait(TimeSpan.FromSeconds(30))
    .AckProgressEvery(TimeSpan.FromSeconds(10));
```

This is off by default because it costs a background task per in-flight message, and most handlers finish well inside the deadline.

# Scheduled messages

JetStream holds scheduled messages itself, so the transport does not run a scheduler:

```csharp
.AddNats(nats => nats.ServiceName("order-service").EnableScheduling());
```

Enabling scheduling turns on per-message TTL and message schedules on the stream and captures an extra scheduling subject alongside each subject. The second subject is required because the server refuses a schedule whose target is the subject it was published to.

`MessageEnvelope.ScheduledTime` maps to a message schedule, requiring server 2.12, and `DeliverBy` maps to a per-message TTL, requiring server 2.11. Dispatching either to a server too old to support it fails with an explicit error rather than silently.

> [!CAUTION]
> Scheduled messages cannot be cancelled. Once JetStream holds one there is no supported way to withdraw it by identifier, so `CancelScheduledMessageAsync` throws for a NATS-issued token rather than reporting a cancellation that did not happen. Use a transport with its own scheduled message store if cancellation is required.

# Failure handling

A handler that fails is negatively acknowledged and redelivered according to the consumer's `MaxDeliver` and `Backoff`. Once Mocha's resilience policy gives up, the message is republished to the endpoint's error subject, which the transport verifies is captured by a stream at start-up.

If a consume loop fails outright, for example because its consumer was deleted on the server, the transport logs the failure and restarts the loop rather than leaving the service alive but deaf.

# Shutdown

Stopping drains rather than aborting: no new messages are pulled, but everything already buffered is handled and acknowledged, bounded by the host shutdown timeout. Handlers only observe cancellation once that timeout expires, and their messages are then released for redelivery.

# Next steps

- [Transports Overview](./index.md) - Understand the transport abstraction and lifecycle.
- [Handlers and Consumers](../handlers-and-consumers.md) - Learn about handler types and consumer configuration.
- [Reliability](../reliability.md) - Configure dead-letter routing, outbox, inbox, and fault handling.

> **Runnable example:** [Nats](https://github.com/ChilliCream/graphql-platform/tree/main/src/Mocha/src/Examples/Transports/Nats)
