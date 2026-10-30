# Learnings

## Day 1

### Batch vs streaming

- **Batch:** collect data, then process it in one large group on a schedule. Example: load all of yesterday's journeys at 2am. It is simple and cheap, but the results are only as fresh as the last run.
- **Streaming:** process each piece of data soon after it happens. Example: see a tube delay within seconds. The results are fresh, but there are more moving parts to run and monitor.
- **Which to choose:** it depends on how fresh the data needs to be. A yearly report can use batch. A live "is my line delayed?" dashboard needs streaming.

### Kafka terms

- **Message:** one piece of data in Kafka, such as one tube line's status at one moment.
- **Topic:** a named stream that holds related messages, such as `line_status`. It works like a log: new messages are added to the end.
- **Producer:** a program that writes messages to a topic. Ours will call the TfL API and send what it gets back.
- **Consumer:** a program that reads messages from a topic. Ours will load them into Snowflake.
- **Offset:** a message's position number within a topic partition (a topic is split into one or more partitions). A consumer saves its offset, so after a crash it can carry on from where it stopped.
- **Dead-letter topic:** a separate topic for messages that could not be processed, such as broken JSON. One bad message doesn't block the rest, and it can be inspected later. Kafka doesn't do this automatically: the consumer (or a tool such as Kafka Connect) has to send it there.

### Why Kafka sits in the middle

Without Kafka, the producer would write directly to Snowflake, so a problem on one side would become a problem for the other. Kafka sits between them as a buffer:

- **Decoupling:** the producer and consumer don't need to know about each other. Either one can change without affecting the other.
- **Surviving failures:** if Snowflake or the consumer goes down, the producer keeps writing. Messages wait in Kafka until the consumer returns, up to the topic's retention limit.
- **Handling different speeds:** the consumer can read and load at its own pace, even when data arrives faster than it can be loaded.
- **Replay:** messages stay in Kafka after they are read, until retention removes them. A consumer can go back to an earlier offset and reprocess them, for example after fixing a bug.
- **Many readers:** several consumers can read the same topic independently without affecting each other.

### TfL API key

The TfL API key (`TFL_APP_KEY`) is optional. If it is set, we send it as the `app_key` parameter. If it is not set, we call the API anonymously, which has a lower rate limit. Our producer calls the API about twice a minute, so anonymous access is fine.

## Day 2

### Docker terms

- **Image:** a read-only package holding a program and everything it needs to run. It works like a recipe. Example: `apache/kafka:4.3.2`.
- **Container:** a running copy of an image, like a dish made from the recipe. You can delete a container and make a new one from the same image.
- **Docker Compose:** a tool that starts several containers together from one file, `docker-compose.yml`. One command starts or stops the whole setup.
- **Ports:** numbered doors that programs use to connect to each other. A container's ports are closed to the laptop until you open them. `127.0.0.1:8080:8080` connects port 8080 on the laptop to port 8080 in the container, and the `127.0.0.1` part means only this laptop can use it.
- **Volumes:** storage that Docker keeps outside the container. Deleting a container deletes the files inside it, but not its volume, so our messages survive a restart. `docker compose down` keeps volumes. Adding `-v` deletes them, along with the data.

### Kafka terms

- **Broker:** the Kafka server. It stores messages and hands them to consumers. Real systems run several for safety. We run one, which is enough for learning.
- **Partitions:** a topic is split into one or more partitions, and each one is a separate log. More partitions let several consumers in a group share the work. Order is only guaranteed within one partition. `bike_points` has 3 partitions and `line_status` has 1.
- **Retention:** how long Kafka keeps messages before deleting them, even if nobody has read them. Ours is 7 days.

### Why two listeners

When a client first connects, Kafka replies with the address to use from then on. The right address depends on where the client is:

- A program on my laptop must use `localhost:9092`. Docker's name `kafka` means nothing on the laptop.
- A program in another container must use `kafka:29092`. Inside a container, `localhost` means that container itself, not Kafka.

With two listeners, each kind of client gets an address that works for it.

### Why Docker for Kafka

- **Easy setup:** Kafka needs Java and some configuration. Docker packages all of that, so nothing has to be installed on Windows.
- **Same every time:** the pinned version and the compose file give the same setup on any machine.
- **Easy cleanup:** one command stops everything and leaves the laptop as it was.

## Day 3

### Producer terms

- **Producer:** a program that writes messages to Kafka. Ours calls the TfL API every 30 seconds and sends one message per tube line.
- **Message key:** a label sent with a message. Ours is the line id, such as `bakerloo`. Kafka uses the key to choose the partition.
- **Delivery callback:** a function Kafka's client calls later to report whether a message was stored. Sending only puts a message in a queue, so without a callback a failure could go unnoticed.
- **Flush:** wait until the messages in the queue have been sent and confirmed. Without a flush on exit, messages still in the queue would be lost.
- **Dead letter topic:** a separate topic for data we could not process. If TfL sends something we can't parse, we save the raw data and the error in `dead_letter` so it can be inspected later, and the good data keeps flowing.
- **Backoff:** waiting longer after each failed try. We wait 2, then 4, 8 and 16 seconds. If an API is struggling, this gives it time to recover instead of adding to the load. Errors that retrying can't fix, such as a rejected key, are not retried.

### Why the key keeps a line's messages in order

Kafka only guarantees order within one partition. Messages with the same key go to the same partition, as long as the number of partitions stays the same, so all `bakerloo` messages stay in the order they were sent.

`line_status` has just one partition today, so all its messages are already in order. The key means that each line would stay in order if we ever added partitions, although messages from different lines could then mix. Changing the partition count also changes where a key goes, so older and newer messages for a line could end up in different partitions.

We also turn on `enable.idempotence`, which stops the client's own retries from duplicating or reordering messages.

**IPv4 on Windows:** `localhost` can mean IPv6 (`[::1]`) or IPv4 (`127.0.0.1`), and Windows tries IPv6 first, but our Docker port is IPv4 only, so the producer sets `broker.address.family` to `v4` to avoid `Connect to ipv6#[::1]:9092 failed` errors.
