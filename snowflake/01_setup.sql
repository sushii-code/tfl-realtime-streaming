-- Run this file once in a Snowsight worksheet. It is safe to run again: nothing here is deleted.
-- BEFORE RUNNING: replace the placeholder in the ALTER USER line at the very end.
-- Snowsight may run one statement at a time: put the cursor on each statement and press Ctrl+Enter, top to bottom.

-- ACCOUNTADMIN is the only role that can create the resource monitor below
USE ROLE ACCOUNTADMIN;

-- Show and calculate times in UTC across the whole account, matching our ingested_at timestamps
ALTER ACCOUNT SET TIMEZONE = 'UTC';

-- ===== Warehouse and spending limit =====

-- Create a spending limit, so a mistake can't use up the trial credits
CREATE RESOURCE MONITOR IF NOT EXISTS TFL_MONITOR
    -- Allow at most 20 credits in each period
    WITH CREDIT_QUOTA = 20
    -- Start a new period, and a new allowance of 20 credits, every month
    FREQUENCY = MONTHLY
    -- Start counting now
    START_TIMESTAMP = IMMEDIATELY
    -- Suspend the warehouse when all 20 credits are used; queries already running can finish
    TRIGGERS ON 100 PERCENT DO SUSPEND;

-- Create the warehouse, which provides the computing power for loading and querying
CREATE WAREHOUSE IF NOT EXISTS TFL_WH
    -- XSMALL is the smallest size, and it uses the fewest credits
    WAREHOUSE_SIZE = XSMALL
    -- Suspend after 60 seconds with no work, so idle time costs nothing
    AUTO_SUSPEND = 60
    -- Wake up automatically when a query arrives
    AUTO_RESUME = TRUE
    -- Do not start running (and spending) until it is first needed
    INITIALLY_SUSPENDED = TRUE
    -- Apply the spending limit to this warehouse
    RESOURCE_MONITOR = TFL_MONITOR;

-- ===== Database and schema =====

-- Create the database, the top-level container for this project's data
CREATE DATABASE IF NOT EXISTS TFL_DB;

-- Create the schema for raw data, a folder-like group inside the database
CREATE SCHEMA IF NOT EXISTS TFL_DB.RAW;

-- ===== Objects for loading =====

-- Create a file format that tells Snowflake our files hold JSON
CREATE FILE FORMAT IF NOT EXISTS TFL_DB.RAW.LINE_STATUS_JSON_FORMAT
    -- Each line of a JSON Lines file is one JSON record
    TYPE = JSON;

-- Create an internal stage, a storage area inside Snowflake for files waiting to be loaded
CREATE STAGE IF NOT EXISTS TFL_DB.RAW.LINE_STATUS_STAGE
    -- Read the files in this stage as JSON
    FILE_FORMAT = (FORMAT_NAME = 'TFL_DB.RAW.LINE_STATUS_JSON_FORMAT');

-- Create the table that stores the raw records
CREATE TABLE IF NOT EXISTS TFL_DB.RAW.LINE_STATUS_RAW (
    -- One whole JSON record
    record VARIANT,
    -- The stage file the record came from
    source_file STRING,
    -- When the record was loaded; Snowflake fills this in by itself
    loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
-- End of the table's columns
);

-- ===== Role with only the rights the loader needs =====

-- Create the role used by the loader
CREATE ROLE IF NOT EXISTS TFL_LOADER_ROLE;

-- Allow the role to run queries on the warehouse
GRANT USAGE ON WAREHOUSE TFL_WH TO ROLE TFL_LOADER_ROLE;

-- Allow the role to enter the database
GRANT USAGE ON DATABASE TFL_DB TO ROLE TFL_LOADER_ROLE;

-- Allow the role to enter the RAW schema
GRANT USAGE ON SCHEMA TFL_DB.RAW TO ROLE TFL_LOADER_ROLE;

-- Allow the role to create new stages and tables in RAW
GRANT CREATE STAGE, CREATE TABLE ON SCHEMA TFL_DB.RAW TO ROLE TFL_LOADER_ROLE;

-- Allow the role to use the JSON file format, which COPY INTO needs
GRANT USAGE ON FILE FORMAT TFL_DB.RAW.LINE_STATUS_JSON_FORMAT TO ROLE TFL_LOADER_ROLE;

-- Allow the role to read and upload stage files; PUT needs WRITE, and COPY INTO needs READ
GRANT READ, WRITE ON STAGE TFL_DB.RAW.LINE_STATUS_STAGE TO ROLE TFL_LOADER_ROLE;

-- Allow the role to add rows to the table, and to read it, as COPY INTO requires
GRANT SELECT, INSERT ON TABLE TFL_DB.RAW.LINE_STATUS_RAW TO ROLE TFL_LOADER_ROLE;

-- ===== Service user for the loader =====

-- Create a user for programs, not people; it has no password
CREATE USER IF NOT EXISTS TFL_LOADER
    -- SERVICE means it can't log in with a password or use the Snowsight website
    TYPE = SERVICE
    -- Use the loader role unless told otherwise
    DEFAULT_ROLE = TFL_LOADER_ROLE
    -- Use our warehouse unless told otherwise
    DEFAULT_WAREHOUSE = TFL_WH
    -- Start in the RAW schema unless told otherwise
    DEFAULT_NAMESPACE = 'TFL_DB.RAW';

-- Give the role to the user
GRANT ROLE TFL_LOADER_ROLE TO USER TFL_LOADER;

-- Give the user the PUBLIC key: replace this whole line with the one generate_keys.py prints
ALTER USER TFL_LOADER SET RSA_PUBLIC_KEY='<paste public key here>';
