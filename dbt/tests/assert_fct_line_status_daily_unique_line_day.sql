-- This test fails if it returns any rows: each line must have only one row per day

-- Choose the line and the day
select
    -- The line's id
    line_id,
    -- The UTC day
    status_date
-- Read from the daily model
from {{ ref('fct_line_status_daily') }}
-- Make one group for each line on each day
group by line_id, status_date
-- Keep only combinations that appear more than once
having count(*) > 1
