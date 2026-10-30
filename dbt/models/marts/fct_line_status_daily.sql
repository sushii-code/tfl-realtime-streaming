-- One row per tube line per UTC day, showing how good the service was

-- Choose the columns
select
    -- The line's id
    line_id,
    -- The line's name; max picks one if the name ever changed during a day
    max(line_name) as line_name,
    -- The UTC day, taken from the time the data was received
    ingested_at::date as status_date,
    -- How many polls we have for this line on this day
    count(*) as poll_count,
    -- How many of those polls showed Good Service (code 10)
    count_if(status_severity = 10) as good_service_poll_count,
    -- The share of polls with Good Service, as a percentage rounded to 2 decimal places
    round(100 * count_if(status_severity = 10) / count(*), 2) as good_service_pct,
    -- The first time this line was seen on this day
    min(ingested_at) as first_seen_at,
    -- The last time this line was seen on this day
    max(ingested_at) as last_seen_at
-- Read from the staging model; ref tells dbt to build that model first
from {{ ref('stg_line_status') }}
-- Make one row for each line on each day
group by line_id, status_date
