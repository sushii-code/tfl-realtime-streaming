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

## Day 4

### Consumer terms

- **Consumer:** a program that reads messages from Kafka. Ours reads `line_status` and saves the messages as files. Reading doesn't remove a message from Kafka.
- **Consumer group:** a team of consumers sharing the work of reading a topic, identified by a group id. Each partition is read by one member at a time. Ours is `line_status_landing`, with one member. Another group could read the same topic separately, from its own position.
- **Offset:** a message's position number within a partition, as on Day 1. Each group keeps its own position for each partition.
- **Committing offsets:** telling Kafka "this group has finished everything before this position". After a restart, the group carries on from there. The first time, there is no position yet, so `auto.offset.reset` set to `earliest` starts from the oldest message.

### At-least-once vs exactly-once

- **At-least-once:** every message is handled, but some may be handled twice. Nothing is lost.
- **Exactly-once:** every message is handled exactly one time. This is much harder to achieve when writing to something outside Kafka, such as files, because saving a file and committing an offset can't happen as one single step.

We use at-least-once, so the same message can appear in more than one file. `kafka_partition` and `kafka_offset` together identify each message, so duplicates can be removed later.

### Why we commit only after writing

Think about a crash between the two steps:

- **Write, then commit (what we do):** the file is saved but the commit is lost. After a restart, those messages are read and written again. We get duplicates, which can be removed.
- **Commit, then write:** the commit is saved but the file is not. After a restart, Kafka thinks those messages are done, so they are lost for good.

Duplicates can be fixed, but lost data can't. This is why `enable.auto.commit` is `false`: automatic commits happen on a timer, whether or not the file has been written.

### Files and cleaning

- **JSON Lines:** a text file with one complete JSON object on each line. Tools can read it line by line, so a huge file doesn't have to fit in memory, and it is easy to load into Snowflake.
- **Landing zone:** the folder where raw data first arrives, before any real transformation. If a later step has a bug, we can rerun it from these files without calling the API again. Ours is `data/landing/line_status/`, split into a `date=YYYY-MM-DD` folder for each day. It is not sent to GitHub, because data files don't belong in a code repository.
- **Why we remove `$type`:** TfL's API adds `$type` fields that name its internal code types, such as `Tfl.Api.Presentation.Entities.Line`. They say nothing about the tube, they repeat at every level, and they make every record bigger.

## Day 5

### Where data lives in Snowflake

- **Warehouse:** the computing power that runs queries and loads. It doesn't store any data. Ours is `TFL_WH`, the smallest size.
- **Database:** the top-level container for data. Ours is `TFL_DB`.
- **Schema:** a group of related tables and other objects inside a database, like a folder. Ours is `RAW`, for data that hasn't been transformed yet. Its full name is `TFL_DB.RAW`.
- **Stage:** a storage area for files waiting to be loaded. They aren't in a table yet, so they can't be queried like one. `PUT` uploads files to our stage, `LINE_STATUS_STAGE`.
- **VARIANT:** a column type that can hold a whole JSON record. We can load the data before deciding on its columns, and later read a field with `record:name`.

### Loading

- **COPY INTO:** the command that loads files from a stage into a table. It needs a running warehouse, so it uses credits. `PUT` doesn't.
- **Load history:** Snowflake remembers which files have been loaded into each table, for 64 days. `COPY INTO` skips those files, so running the loader twice doesn't load them twice. A file is only protected once: it doesn't remove duplicate messages from the consumer that ended up in different files.

### Security

- **Role and least privilege:** a role is a named set of permissions, and users get their permissions through roles. Least privilege means giving only what is needed. If the loader's login leaks, `TFL_LOADER_ROLE` can't touch anything outside its warehouse, database and schema, or delete data.
- **Service user:** a user for programs instead of people. `TFL_LOADER` has `TYPE = SERVICE`, so it has no password and can't use the Snowsight website.
- **Key-pair authentication:** logging in with two matching keys instead of a password. Snowflake stores the public key, which is safe to share. The private key stays on my laptop and signs a login token. The private key is never sent to Snowflake, and anyone who has the file can log in as the loader, so it must never be shared or committed.

### Why auto-suspend saves money

A warehouse uses credits for as long as it is running, even when it is doing nothing. Each time it starts, it is charged for at least 60 seconds. `AUTO_SUSPEND = 60` stops it after 60 idle seconds, and `AUTO_RESUME = TRUE` starts it again when a query arrives. We pay for the time it is working, plus about a minute each time, instead of for all day.

## Day 6

### dbt terms

- **dbt:** a tool that turns SQL `SELECT` statements into tables and views in the warehouse, in the right order. It doesn't move data into Snowflake. It transforms data that is already there.
- **Model:** one SQL file containing one `SELECT`. The file name becomes the name of the view or table, so `stg_line_status.sql` builds `STG_LINE_STATUS`.
- **Source:** a table that dbt reads but doesn't build. Ours is `RAW.LINE_STATUS_RAW`, which the loader fills.
- **`ref()`:** how one model points to another. dbt uses it to work out the build order and to fill in the full table name.
- **Tests:** checks on the data. Each one looks for bad rows, and it fails if it finds any. `not_null` looks for missing values, `accepted_values` looks for values not on a list, and a SQL file in `tests/` can check anything else.
- **Freshness:** a check on how recently data arrived. It looks at the newest `loaded_at`: we get a warning after 1 hour and an error after 24 hours. It runs with `dbt source freshness`, not with `dbt test`.

### Staging vs marts

- **Staging:** a cleaned-up copy of a source, with proper columns and names and duplicates removed. `stg_line_status` has one row per line per poll.
- **Marts:** tables built to answer business questions, using the staging models. `fct_line_status_daily` answers "how good was each line's service each day?"

### View vs table

- **View:** a saved query. It stores no data and runs each time it is read, so it is always up to date but does the work every time. Staging models are views.
- **Table:** stored results. It is quick to read, but it only changes when dbt runs again. Marts are tables.

### Deduplication

Deduplication means keeping only one copy of something that appears more than once. Our consumer is at-least-once, so the same message can be saved in two files, and load history can't catch that. A partition and offset together identify one Kafka message, so `stg_line_status` keeps one row for each pair.

This only works while offsets stay unique. Deleting the Kafka volume with `docker compose down -v` restarts offsets from 0, so new messages could share a partition and offset with old rows in Snowflake and be wrongly removed as duplicates.

A line can also have more than one status in a single poll. We keep the lowest code, which TfL uses for the more serious problems, so a poll only counts as good service if nothing worse was reported.
