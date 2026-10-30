"""Poll the TfL Line Status API and send each tube line's status to Kafka."""

# datetime and timezone let us create a UTC timestamp
from datetime import datetime, timezone
# json converts between JSON text and Python data
import json
# logging prints messages with the time and level in front
import logging
# os lets us read environment variables
import os
# sys lets us stop the script early with a message
import sys
# time lets us pause and measure how long something took
import time

# Producer sends messages to Kafka; KafkaException is the error it can raise
from confluent_kafka import KafkaException, Producer
# requests is the library we use to call web APIs
import requests

# Web address that returns the current status of every tube line
LINE_STATUS_URL = "https://api.tfl.gov.uk/Line/Mode/tube/Status"
# Give up on a request if TfL takes longer than this many seconds
TIMEOUT_SECONDS = 30
# Try a failing API call at most this many times before waiting for the next poll
MAX_ATTEMPTS = 5
# Wait this many seconds before the first retry; the wait doubles after each failure
BACKOFF_START_SECONDS = 2
# Wait at most this many seconds for Kafka to confirm outstanding messages
FLUSH_TIMEOUT_SECONDS = 10
# Keep at most this much raw data in a dead letter, so it fits in a Kafka message
MAX_RAW_CHARS = 100_000

# Read the Kafka address; "or" also uses the default if the variable is set but empty
BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVER") or "localhost:9092"
# Read the name of the topic for good messages
LINE_STATUS_TOPIC = os.environ.get("LINE_STATUS_TOPIC") or "line_status"
# Read the name of the topic for messages we could not process
DEAD_LETTER_TOPIC = os.environ.get("DEAD_LETTER_TOPIC") or "dead_letter"
# Read how many seconds to leave between polls, and convert the text to a number
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS") or 30)

# Show messages of level INFO and above, with the time and level in front
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# Create the logger that this script uses
logger = logging.getLogger("producer")


# Define the function Kafka calls once it knows what happened to a message
def on_delivery(error, message):
    # Kafka gives us an error if the message could not be delivered
    if error is not None:
        # Log the failure and the topic it was meant for
        logger.error("Delivery failed to %s: %s", message.topic(), error)
    # Otherwise the message is safely stored in Kafka
    else:
        # Log where the message was stored
        logger.info(
            # The text of the log line, with a slot for each value below
            "Delivered to %s partition %s offset %s",
            # The topic that stored the message
            message.topic(),
            # The partition that stored the message
            message.partition(),
            # The message's position in that partition
            message.offset(),
        # End of the log call
        )


# Define a function that calls the API, retrying with backoff; it returns text or None
def fetch_line_status(key):
    # Send the key as a parameter if we have one; otherwise send no parameters
    params = {"app_key": key} if key else None
    # Remember how long to wait before the first retry
    wait_seconds = BACKOFF_START_SECONDS
    # Try the call up to MAX_ATTEMPTS times, counting from 1
    for attempt in range(1, MAX_ATTEMPTS + 1):
        # Start a block where we can catch request errors ourselves
        try:
            # Call the API with those parameters and the time limit
            response = requests.get(LINE_STATUS_URL, params=params, timeout=TIMEOUT_SECONDS)
            # "ok" means the HTTP status shows success
            if response.ok:
                # Return the body as text; a bad character must not stop us reaching the parse step
                return response.content.decode("utf-8", errors="replace")
            # Save the status code, which is safe to log and helps with debugging
            reason = f"HTTP status {response.status_code}"
            # Only "too many requests" (429) and server errors (500+) can fix themselves
            if response.status_code != 429 and response.status_code < 500:
                # Retrying will not help with errors such as a rejected key (401), so log it
                logger.error("TfL API call failed, not retrying: %s", reason)
                # Give up on this poll
                return None
        # Catch connection problems and timeouts
        except requests.RequestException as error:
            # Request errors can include the full URL WITH the key, so keep only the error type
            reason = type(error).__name__
        # Stop retrying if that was the last attempt
        if attempt == MAX_ATTEMPTS:
            # Leave the loop without waiting again
            break
        # Log what went wrong and how long we will wait
        logger.warning("TfL API call failed (%s), retrying in %s seconds", reason, wait_seconds)
        # Wait before trying again, so we do not hammer an API that is struggling
        time.sleep(wait_seconds)
        # Double the wait for next time
        wait_seconds *= 2
    # Every attempt failed, so log it; the main loop will try again at the next poll
    logger.error("TfL API call failed after %s attempts: %s", MAX_ATTEMPTS, reason)
    # Tell the caller that there is no data this time
    return None


# Define a function that sends one Python dictionary to a Kafka topic
def send_message(producer, topic, key, message):
    # Convert the message to JSON text, then to the bytes Kafka stores
    value = json.dumps(message).encode("utf-8")
    # Convert the key to bytes too; dead letters have no key, so they keep None
    key_bytes = key.encode("utf-8") if key else None
    # Start a block where we can catch errors from handing the message to Kafka's client
    try:
        # Put the message in the send queue; on_delivery reports the result later
        producer.produce(topic, key=key_bytes, value=value, on_delivery=on_delivery)
    # Catch a full send queue, or a message Kafka's client refuses
    except (BufferError, KafkaException) as error:
        # Log the problem and carry on with the next message
        logger.error("Could not queue message for %s: %s", topic, error)
    # Give Kafka's client a moment to run delivery callbacks that are ready
    producer.poll(0)


# Define a function that sends data we could not process to the dead letter topic
def send_dead_letter(producer, raw_data, error, ingested_at):
    # Log the problem so that it is visible in the terminal
    logger.warning("Sending data to %s: %s", DEAD_LETTER_TOPIC, error)
    # Build the dead letter message
    message = {
        # Record which topic this data was meant for
        "source_topic": LINE_STATUS_TOPIC,
        # Record what went wrong
        "error": error,
        # Keep the raw data for inspection, cut down if it is very large
        "raw_data": raw_data[:MAX_RAW_CHARS],
        # Record when we received the data
        "ingested_at": ingested_at,
    # End of the message
    }
    # Send it; a dead letter has no line id, so it has no key
    send_message(producer, DEAD_LETTER_TOPIC, None, message)


# Define a function that checks that one line has the fields we rely on
def is_valid_line(line):
    # A valid line is a dictionary with a text id (used as the key) and a list of statuses
    return isinstance(line, dict) and isinstance(line.get("id"), str) and bool(line["id"]) and "lineStatuses" in line


# Define a function that turns one API response into Kafka messages
def process_response(producer, body):
    # Create the current UTC time in ISO format, shared by everything in this response
    ingested_at = datetime.now(timezone.utc).isoformat()
    # Start a block where we can catch data that cannot be parsed
    try:
        # Convert the JSON text into Python data
        lines = json.loads(body)
        # The API should send a list of lines, so anything else counts as bad data
        if not isinstance(lines, list):
            # Raise an error so that it is handled by the "except" below
            raise ValueError("expected a JSON list of lines")
    # Catch broken JSON (JSONDecodeError is a kind of ValueError) and the error above
    except ValueError as error:
        # Send the whole response to the dead letter topic
        send_dead_letter(producer, body, str(error), ingested_at)
        # There is nothing more to do with this response
        return
    # Go through the tube lines one at a time
    for line in lines:
        # A single bad line must not stop the good ones
        if not is_valid_line(line):
            # Send just this line to the dead letter topic, as JSON text
            send_dead_letter(producer, json.dumps(line), "line is missing id or lineStatuses", ingested_at)
            # Move on to the next line
            continue
        # Copy the line's fields into a new message and add the timestamp
        message = {**line, "ingested_at": ingested_at}
        # Send it with the line id as the key, so each line's messages stay together in order
        send_message(producer, LINE_STATUS_TOPIC, line["id"], message)


# Define a function that waits for Kafka to confirm the messages still in the queue
def flush_messages(producer):
    # Wait for delivery (this also runs the callbacks); it returns how many are still waiting
    remaining = producer.flush(FLUSH_TIMEOUT_SECONDS)
    # Anything left means Kafka did not confirm in time
    if remaining > 0:
        # Log it; Kafka might be down, and a later flush can still deliver these
        logger.warning("%s messages not yet confirmed by Kafka", remaining)


# Define the main function, which keeps polling until we press Ctrl+C
def main():
    # An interval below 1 second would hammer the API, so refuse to start
    if POLL_INTERVAL_SECONDS < 1:
        # Stop the script and show a clear message
        sys.exit("POLL_INTERVAL_SECONDS must be 1 or more.")
    # Read the key from the environment; this gives None if it is not set
    key = os.environ.get("TFL_APP_KEY")
    # "not key" is true if the key is missing or empty
    if not key:
        # The key is optional, so say we are going without it and carry on
        print("No TFL_APP_KEY set - using anonymous access (lower rate limit)")
    # Create the connection to Kafka, using the settings below
    producer = Producer({
        # The address of Kafka
        "bootstrap.servers": BOOTSTRAP_SERVER,
        # Idempotence stops retries from duplicating or reordering messages
        "enable.idempotence": True,
        # Use IPv4 only: Windows tries IPv6 first, but our Docker port is IPv4 only
        "broker.address.family": "v4",
    # End of the settings
    })
    # Tell the user what is happening
    logger.info("Sending to %s every %s seconds. Press Ctrl+C to stop.", BOOTSTRAP_SERVER, POLL_INTERVAL_SECONDS)
    # Start a block where we can catch Ctrl+C
    try:
        # Keep polling until Ctrl+C
        while True:
            # Note the time, so that polls start one interval apart
            started = time.monotonic()
            # Call the API, retrying if needed
            body = fetch_line_status(key)
            # None means every attempt failed, so only process real data
            if body is not None:
                # Send the response to Kafka
                process_response(producer, body)
            # Wait for Kafka to confirm this poll's messages
            flush_messages(producer)
            # Work out how long this poll took
            elapsed = time.monotonic() - started
            # Sleep for the rest of the interval; never a negative time
            time.sleep(max(0, POLL_INTERVAL_SECONDS - elapsed))
    # Ctrl+C raises KeyboardInterrupt, even during a sleep
    except KeyboardInterrupt:
        # Tell the user that we are stopping on purpose
        logger.info("Stopping...")
    # This runs however the loop ended
    finally:
        # Send anything still in the queue before exiting
        flush_messages(producer)


# Run main() only when this file is run directly, not when it is imported
if __name__ == "__main__":
    # Start the script
    main()
