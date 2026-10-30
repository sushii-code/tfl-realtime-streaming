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
