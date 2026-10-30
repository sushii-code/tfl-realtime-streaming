"""Read tube line statuses from Kafka and save them as JSON Lines files in the landing zone."""

# datetime and timezone let us work with UTC timestamps
from datetime import datetime, timezone
# json converts between JSON text and Python data
import json
# logging prints messages with the time and level in front
import logging
# os lets us read environment variables and safely move files
import os
# Path makes working with file and folder paths easy
from pathlib import Path
# sys lets us stop the script early with a message
import sys
# time lets us measure how long a batch has been open
import time
# uuid gives each batch file a unique name
import uuid

# These let us read from Kafka, write to Kafka, and name a partition when committing
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

# Wait this many seconds for a new message before checking the batch timer again
POLL_TIMEOUT_SECONDS = 1
# Wait at most this many seconds for Kafka to confirm dead letters
FLUSH_TIMEOUT_SECONDS = 10
# Keep at most this much raw data in a dead letter, so it fits in a Kafka message
MAX_RAW_CHARS = 100_000
# Find the project's main folder: this file is in consumer/, so go up two levels
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Read the Kafka address; "or" also uses the default if the variable is set but empty
BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVER") or "localhost:9092"
# Read the name of the topic to consume
LINE_STATUS_TOPIC = os.environ.get("LINE_STATUS_TOPIC") or "line_status"
# Read the name of the topic for messages we could not process
DEAD_LETTER_TOPIC = os.environ.get("DEAD_LETTER_TOPIC") or "dead_letter"
# Read the name of the consumer group to join
CONSUMER_GROUP_ID = os.environ.get("CONSUMER_GROUP_ID") or "line_status_landing"
# Read the output folder; a relative path is treated as relative to the project's main folder
LANDING_DIR = PROJECT_ROOT / (os.environ.get("LANDING_DIR") or "data/landing/line_status")
# Read how many messages make a full batch, and convert the text to a number
BATCH_SIZE = int(os.environ.get("BATCH_SIZE") or 100)
# Read how many seconds a batch can stay open, and convert the text to a number
BATCH_SECONDS = int(os.environ.get("BATCH_SECONDS") or 30)

# Show messages of level INFO and above, with the time and level in front
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# Create the logger that this script uses
logger = logging.getLogger("consumer")
# Collect any dead letters that Kafka failed to store
failed_deliveries = []


# Define the function Kafka calls once it knows what happened to a dead letter
def on_delivery(error, message):
    # Kafka gives us an error if the dead letter could not be delivered
    if error is not None:
        # Log the failure and the topic it was meant for
        logger.error("Delivery failed to %s: %s", message.topic(), error)
        # Remember the failure, so that we do not commit past this message
        failed_deliveries.append(error)


# Define a function that removes every "$type" field, at any depth
def remove_type_fields(value):
    # A dictionary may hold "$type" itself, and more data inside its values
    if isinstance(value, dict):
        # Rebuild the dictionary without "$type", cleaning each value that we keep
        return {key: remove_type_fields(item) for key, item in value.items() if key != "$type"}
    # A list may hold dictionaries inside it
    if isinstance(value, list):
        # Rebuild the list, cleaning each item
        return [remove_type_fields(item) for item in value]
    # Anything else (text, numbers and so on) is returned as it is
    return value


# Define a function that converts Kafka's timestamp to UTC ISO format
def format_kafka_timestamp(message):
    # Kafka gives us the timestamp's type and its value in milliseconds
    timestamp_type, milliseconds = message.timestamp()
    # Type 0 means that this message has no timestamp
    if timestamp_type == 0:
        # Store an empty value for it
        return None
    # Convert milliseconds to seconds, then to a UTC date and time in ISO format
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat()


# Define a function that turns one Kafka message into a date and a clean record
def parse_message(message):
    # Convert the message's JSON bytes into Python data
    data = json.loads(message.value())
    # Each message should be one JSON object, so anything else counts as bad data
    if not isinstance(data, dict):
        # Raise an error so that the caller sends this message to the dead letter topic
        raise ValueError("expected a JSON object")
    # Find the date for the folder name; this raises an error if ingested_at is missing or invalid
    date = datetime.fromisoformat(data["ingested_at"]).date().isoformat()
    # Remove every "$type" field from the data
    record = remove_type_fields(data)
    # Read the message key, which is bytes, or None if the message has no key
    key = message.key()
    # Keep the key as text
    record["kafka_key"] = key.decode("utf-8", errors="replace") if key is not None else None
    # Keep the partition the message came from
    record["kafka_partition"] = message.partition()
    # Keep the message's position in that partition
    record["kafka_offset"] = message.offset()
    # Keep the time Kafka recorded for the message
    record["kafka_timestamp"] = format_kafka_timestamp(message)
    # Return the date and the clean record together
    return date, record


# Define a function that sends a message we could not parse to the dead letter topic
def send_dead_letter(producer, message, error):
    # Log the problem and where the message came from
    logger.warning(
        # The text of the log line, with a slot for each value below
        "Sending partition %s offset %s to %s: %s",
        # The partition the message came from
        message.partition(),
        # The message's position in that partition
        message.offset(),
        # The topic we are sending it to
        DEAD_LETTER_TOPIC,
        # What went wrong
        error,
    # End of the log call
    )
    # Read the message's bytes as text, using an empty value if it has none
    raw_data = (message.value() or b"").decode("utf-8", errors="replace")
    # Read the message key, which is bytes, or None if the message has no key
    key = message.key()
    # Build the dead letter message, with the same main fields the producer uses
    dead_letter = {
        # Record which topic this message came from
        "source_topic": LINE_STATUS_TOPIC,
        # Record what went wrong
        "error": str(error),
        # Keep the raw data for inspection, cut down if it is very large
        "raw_data": raw_data[:MAX_RAW_CHARS],
        # Record when we handled the message
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        # Keep the original key as text
        "kafka_key": key.decode("utf-8", errors="replace") if key is not None else None,
        # Keep the partition the message came from
        "kafka_partition": message.partition(),
        # Keep the message's position in that partition
        "kafka_offset": message.offset(),
    # End of the dead letter message
    }
    # Convert the dead letter to JSON text, then to the bytes Kafka stores
    value = json.dumps(dead_letter).encode("utf-8")
    # Put it in the send queue; an error here stops the script, so nothing is committed
    producer.produce(DEAD_LETTER_TOPIC, value=value, on_delivery=on_delivery)
    # Give Kafka's client a moment to run delivery callbacks that are ready
    producer.poll(0)


# Define a function that safely writes one date's records to a new JSON Lines file
def write_batch_file(date, records):
    # Build the folder name for this date
    folder = LANDING_DIR / f"date={date}"
    # Create the folder if it does not exist yet
    folder.mkdir(parents=True, exist_ok=True)
    # Create the current UTC time in a form that is safe in a file name
    created_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # Build a unique file name, so that a batch never overwrites an earlier one
    final_path = folder / f"batch_{created_at}_{uuid.uuid4().hex[:8]}.jsonl"
    # Write to a temporary name first, so a half-written file never looks finished
    temp_path = final_path.with_suffix(".tmp")
    # Open the file for writing, always using "\n" line endings; "with" closes it afterwards
    with temp_path.open("w", encoding="utf-8", newline="\n") as file:
        # Go through the records one at a time
        for record in records:
            # Write the record as JSON on its own line
            file.write(json.dumps(record) + "\n")
        # Push the text out of Python's memory
        file.flush()
        # Ask Windows to save it to the disk, so it survives a crash
        os.fsync(file.fileno())
    # Give the finished file its real name
    os.replace(temp_path, final_path)
    # Tell the user what was saved
    logger.info("Wrote %s records to %s", len(records), final_path)


# Define a function that saves a batch and only then commits its offsets
def finish_batch(consumer, producer, batch, offsets):
    # Wait for Kafka to confirm the dead letters; it returns how many are still waiting
    remaining = producer.flush(FLUSH_TIMEOUT_SECONDS)
    # If any dead letter is not safely stored, we must not commit past its message
    if remaining > 0 or failed_deliveries:
        # Stop the script without committing, so these messages are read again next time
        raise RuntimeError("Could not deliver dead letters, so offsets were not committed")
    # A batch can hold more than one date, so go through the dates one at a time
    for date, records in batch.items():
        # Write this date's records to their own new file
        write_batch_file(date, records)
    # Build the list of positions to commit: the next offset to read in each partition
    positions = [TopicPartition(LINE_STATUS_TOPIC, partition, offset) for partition, offset in offsets.items()]
    # Start a block where we can catch a failed commit
    try:
        # Commit the positions, and wait until Kafka confirms
        consumer.commit(offsets=positions, asynchronous=False)
        # Tell the user what was committed
        logger.info("Committed offsets: %s", offsets)
    # Catch a failed commit, for example if Kafka is down
    except KafkaException as error:
        # The files are safe; the worst case is that these messages get written again next time
        logger.warning("Commit failed, so these messages may be written again: %s", error)
    # Empty the batch, ready for new records
    batch.clear()
    # Empty the positions too, ready for new messages
    offsets.clear()


# Define the main function, which keeps reading until we press Ctrl+C
def main():
    # A batch size or time limit below 1 makes no sense, so refuse to start
    if BATCH_SIZE < 1 or BATCH_SECONDS < 1:
        # Stop the script and show a clear message
        sys.exit("BATCH_SIZE and BATCH_SECONDS must be 1 or more.")
    # Create the consumer, using the settings below
    consumer = Consumer({
        # The address of Kafka
        "bootstrap.servers": BOOTSTRAP_SERVER,
        # The consumer group to join
        "group.id": CONSUMER_GROUP_ID,
        # With no saved position yet, start from the earliest message
        "auto.offset.reset": "earliest",
        # We commit offsets ourselves, only after a batch file is safely written
        "enable.auto.commit": False,
        # Use IPv4 only: Windows tries IPv6 first, but our Docker port is IPv4 only
        "broker.address.family": "v4",
    # End of the settings
    })
    # Create the producer that sends messages to the dead letter topic, using the settings below
    producer = Producer({
        # The address of Kafka
        "bootstrap.servers": BOOTSTRAP_SERVER,
        # Idempotence stops retries from duplicating or reordering messages
        "enable.idempotence": True,
        # Use IPv4 only: Windows tries IPv6 first, but our Docker port is IPv4 only
        "broker.address.family": "v4",
    # End of the settings
    })
    # The batch's clean records, grouped by date
    batch = {}
    # The next offset to read in each partition, once this batch is saved
    offsets = {}
    # How many messages the batch has handled, including any sent to the dead letter topic
    message_count = 0
    # The time the batch's first message arrived
    batch_started = 0
    # Start a block where we can catch Ctrl+C
    try:
        # Join the consumer group and start reading the topic
        consumer.subscribe([LINE_STATUS_TOPIC])
        # Tell the user what is happening
        logger.info("Reading %s as group %s. Press Ctrl+C to stop.", LINE_STATUS_TOPIC, CONSUMER_GROUP_ID)
        # Keep reading until Ctrl+C
        while True:
            # Ask Kafka for the next message; this gives None if none arrives in time
            message = consumer.poll(POLL_TIMEOUT_SECONDS)
            # Only handle a real message
            if message is not None:
                # Kafka can also use this to report a problem instead of giving us data
                if message.error():
                    # A fatal problem can't be fixed by carrying on
                    if message.error().fatal():
                        # Stop the script without committing
                        raise KafkaException(message.error())
                    # Log any other problem; Kafka's client usually recovers by itself
                    logger.warning("Kafka reported a problem: %s", message.error())
                # Otherwise this message holds data
                else:
                    # The first message of a new batch starts the batch timer
                    if message_count == 0:
                        # Note the time
                        batch_started = time.monotonic()
                    # Start a block where we can catch a message that cannot be parsed
                    try:
                        # Turn the message into a date and a clean record
                        date, record = parse_message(message)
                        # Add the record to this date's list, creating the list if needed
                        batch.setdefault(date, []).append(record)
                    # Catch bad data: invalid text or JSON, a missing field, or JSON nested too deeply
                    except (ValueError, TypeError, KeyError, RecursionError) as error:
                        # Send the message to the dead letter topic, then carry on
                        send_dead_letter(producer, message, repr(error))
                    # Record the next offset only now, so an unhandled message is never committed
                    offsets[message.partition()] = message.offset() + 1
                    # Count this message as part of the batch
                    message_count += 1
            # Check whether the batch has been open for long enough
            time_is_up = message_count > 0 and time.monotonic() - batch_started >= BATCH_SECONDS
            # Finish the batch when it is full or its time is up, whichever comes first
            if message_count >= BATCH_SIZE or time_is_up:
                # Save the batch, then commit its offsets
                finish_batch(consumer, producer, batch, offsets)
                # Start counting the next batch from zero
                message_count = 0
    # Ctrl+C raises KeyboardInterrupt
    except KeyboardInterrupt:
        # Tell the user that we are stopping on purpose
        logger.info("Stopping...")
        # Only finish the batch if it has anything in it
        if message_count > 0:
            # Save the remaining messages, then commit their offsets
            finish_batch(consumer, producer, batch, offsets)
    # This runs however the loop ended
    finally:
        # Give any dead letters still in the queue a last chance to be sent
        producer.flush(FLUSH_TIMEOUT_SECONDS)
        # Leave the consumer group cleanly; this does not commit anything
        consumer.close()


# Run main() only when this file is run directly, not when it is imported
if __name__ == "__main__":
    # Start the script
    main()
