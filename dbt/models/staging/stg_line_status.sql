-- One row per tube line per poll, cleaned up from the raw JSON

-- Start a named step that opens each record's list of statuses
with flattened as (
    -- Choose the columns for this step
    select
        -- The line's id, as text
        raw_data.record:id::string as line_id,
        -- The line's name, as text
        raw_data.record:name::string as line_name,
        -- TfL's status code, as a whole number
        status.value:statusSeverity::integer as status_severity,
        -- The status in words, such as Good Service
        status.value:statusSeverityDescription::string as status_description,
        -- TfL's explanation, which is only there when there is a problem
        status.value:reason::string as reason,
        -- Read the time as text, convert it to UTC, then drop the time zone part
        convert_timezone('UTC', to_timestamp_tz(raw_data.record:ingested_at::string))::timestamp_ntz as ingested_at,
        -- The Kafka partition the message came from
        raw_data.record:kafka_partition::integer as kafka_partition,
        -- The message's position in that partition
        raw_data.record:kafka_offset::integer as kafka_offset,
        -- The stage file the record came from
        raw_data.source_file,
        -- Convert the load time to UTC, then drop the time zone part
        convert_timezone('UTC', raw_data.loaded_at)::timestamp_ntz as loaded_at
    -- Read from the raw table
    from {{ source('raw', 'line_status_raw') }} as raw_data,
        -- Make one row per status; outer keeps a line even if its list is empty
        lateral flatten(input => raw_data.record:lineStatuses, outer => true) as status
-- End of this step
)

-- Choose the final columns
select
    -- The line's id
    line_id,
    -- The line's name
    line_name,
    -- The status code
    status_severity,
    -- The status in words
    status_description,
    -- The explanation, if any
    reason,
    -- When the producer received the data, in UTC
    ingested_at,
    -- The Kafka partition
    kafka_partition,
    -- The Kafka offset
    kafka_offset,
    -- The stage file
    source_file,
    -- When the row was loaded, in UTC
    loaded_at
-- Read from the step above
from flattened
-- Keep one row per message: this removes duplicates, and keeps the worst status (lowest code) of a line with several
qualify row_number() over (
    -- Compare rows that have the same partition and offset, which means the same message
    partition by kafka_partition, kafka_offset
    -- Put the worst status first, then the earliest load, then use the description to settle any tie
    order by status_severity asc nulls last, loaded_at asc, status_description asc
-- Keep only the first row of each message
) = 1
