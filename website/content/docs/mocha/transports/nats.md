---
title: "NATS Transport"
description: "Configure the NATS JetStream transport in Mocha, including stream and consumer provisioning, subject naming, request/reply over core NATS, and scheduled messages."
---

The NATS transport connects Mocha to a NATS server running JetStream. It provisions streams and durable consumers, acknowledges messages, supports request/reply over core NATS, and delegates scheduled delivery to the broker.

# Set up the NATS transport

By the end of this section, you will have a Mocha bus publishing and consuming over JetStream.

## Install the package

```bash
dotnet add package Mocha.Transport.Nats
```

## Register the transport

The transport resolves an `INatsConnection` from dependency injection:

```csharp
using Mocha;
using Mocha.Transport.Nats;
using NATS.Client.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<INatsConnection>(_ => new NatsConnection(new NatsOpts
{
    Url = "nats://localhost:4222",

    // NATS.Net drops messages when a subscriber falls behind. Request/reply responses arrive on a
    // core subscription, so leaving the default in place can silently lose them.
    SubPendingChannelFullMode = BoundedChannelFullMode.Wait
}));

builder.Services
    .AddMessageBus()
    .AddOrderService()
    .AddNats(nats => nats.ServiceName("order-service"));
```

That is enough to run. The transport derives the stream and its subjects from the routes the bus discovers, provisions the stream and one durable consumer per handler, and settles every delivery.

The transport logs a warning at start-up if the connection is left on the dropping default.

# How Mocha concepts map onto JetStream

| Mocha | JetStream |
| --- | --- |
| Exchange, fan-out | Stream `Subjects` with `*` and `>` wildcards |
| Queue | Durable pull consumer |
| Binding, routing key list | `ConsumerConfig.FilterSubjects` |
| Competing consumers | Several instances sharing one durable consumer |
| Prefetch, concurrency | `MaxAckPending` and `ConsumeAsync` options |
| Ack, nack, terminate | `AckAsync`, `NakAsync`, `AckTerminateAsync` |
| Retry backoff | `MaxDeliver` and `ConsumerConfig.Backoff` |
| Reply endpoints | Core NATS request/reply, not JetStream |

`Send` and `Publish` converge on a single subject. Subscribers select what they receive through consumer filter subjects, so there is no equivalent of the exchange-to-exchange-to-queue convention chain the RabbitMQ transport builds.

# Naming

Streams are containers; all routing happens by subject, and the stream name never appears on the publish path.

| Concept | Example |
| --- | --- |
| Stream | `ORDER_SERVICE` |
| Subject | `order-service.order-created` |
| Durable consumer | `order-service_order-created` |

Endpoint names contain dots, which are the natural subject separator. Dots are illegal in stream and consumer names, along with `*`, `>`, whitespace and path separators, so durable names replace them with underscores. NATS recommends keeping these names under 32 characters because they become storage directory names, and the transport emits a diagnostic when a sanitised name exceeds that.

# Which stream does a consumer read from?

A JetStream consumer must be created on the stream that captures its subject, and that stream belongs to the *publishing* service. RabbitMQ has no equivalent constraint: a subscriber declares a queue and binds it to an exchange without knowing anything else about the publisher.

The transport resolves this at start-up by asking the server which stream captures each subscribed subject, so subscribers keep declaring *what* they consume rather than *where it lives*. Failures are start-up errors rather than silence:

- No stream captures the subject, meaning the publishing service has not been deployed or its stream was never provisioned.
- Several streams capture it, so the choice would be arbitrary.
- One consumer's subjects are spread across different streams, which a single consumer cannot read.

Declaring the stream removes the start-up ordering dependency entirely, because whichever service starts first provisions it:

```csharp
.AddNats(nats => nats
    .ServiceName("shipping-service")
    .DeclareStream("ORDER_SERVICE")
        .Subject("order-service.>"));
```

# Publishing to an uncaptured subject

A JetStream publish waits for an acknowledgement from the stream capturing the subject. If no stream captures it, the call does not fail immediately, it times out. The transport therefore verifies subject coverage while starting and fails with the offending subject named, which is considerably easier to diagnose than a timeout in production.

# Deduplication

Deduplication is scoped to the stream, not to the subject, and the window cannot be disabled from the client: `DuplicateWindow` is omitted from the request when set to zero, and the server then applies its own default.

The transport therefore writes a `Nats-Msg-Id` qualified by destination subject and carries the message identifier separately. Without that, republishing a message inside the same stream, which is exactly what dead-lettering does, is discarded as a duplicate of the original and the publish still reports success.

# Long-running handlers

A handler that runs longer than the consumer's `AckWait` is redelivered while it is still working. JetStream can extend the deadline, which has no RabbitMQ equivalent:

```csharp
.DeclareConsumer("order-service_order-created")
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

# Shutdown

Stopping drains rather than aborting: no new messages are pulled, but everything already buffered is handled and acknowledged, bounded by the host shutdown timeout.
