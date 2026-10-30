# Create the project's Kafka topics inside the tfl-kafka container, then list all topics.

# The name of the running Kafka container
$container = "tfl-kafka"
# The full path of Kafka's topic tool inside the container
$topicsTool = "/opt/kafka/bin/kafka-topics.sh"
# Inside the container, Kafka can be reached at this address
$server = "localhost:9092"
# Each topic name and its number of partitions, kept in this order
$topics = [ordered]@{ line_status = 1; bike_points = 3; dead_letter = 1 }

# Go through the topics one at a time
foreach ($name in $topics.Keys) {
    # Look up how many partitions this topic should have
    $partitions = $topics[$name]
    # Show which topic we are working on
    Write-Host "Creating topic $name with $partitions partition(s)..."
    # Create the topic if it is missing; we have one broker, so replication factor must be 1
    docker exec $container $topicsTool --bootstrap-server $server --create --if-not-exists --topic $name --partitions $partitions --replication-factor 1
    # A non-zero exit code means the command failed, for example if Kafka is not running
    if ($LASTEXITCODE -ne 0) {
        # Show a clear message
        Write-Host "Could not create topic $name. Is the tfl-kafka container running and healthy?"
        # Stop the script and report failure
        exit 1
    # End of the "if" block
    }
# End of the "foreach" loop
}

# Print a heading above the topic list
Write-Host "Topics now in Kafka:"
# List every topic in Kafka
docker exec $container $topicsTool --bootstrap-server $server --list
