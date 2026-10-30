-- This test fails if it returns any rows: each Kafka message must appear only once

-- Choose the partition and offset
select
    -- The Kafka partition
    kafka_partition,
    -- The Kafka offset
    kafka_offset
-- Read from the staging model
from {{ ref('stg_line_status') }}
-- Make one group for each message
group by kafka_partition, kafka_offset
-- Keep only messages that appear more than once
having count(*) > 1
