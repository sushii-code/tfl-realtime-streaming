-- Run this file once in a Snowsight worksheet, after 01_setup.sql. It is safe to run again: nothing here is deleted.
-- BEFORE RUNNING: replace the placeholder in the ALTER USER line at the very end.
-- Snowsight runs one statement at a time: put the cursor on each statement and press Ctrl+Enter, top to bottom.

-- ACCOUNTADMIN can create roles and users, and give out all the rights below
USE ROLE ACCOUNTADMIN;

-- ===== Role for dbt =====

-- Create the role used by dbt
CREATE ROLE IF NOT EXISTS TFL_TRANSFORMER_ROLE;

-- Allow the role to run queries on the warehouse
GRANT USAGE ON WAREHOUSE TFL_WH TO ROLE TFL_TRANSFORMER_ROLE;

-- Allow the role to enter the database
GRANT USAGE ON DATABASE TFL_DB TO ROLE TFL_TRANSFORMER_ROLE;

-- Allow the role to enter the RAW schema
GRANT USAGE ON SCHEMA TFL_DB.RAW TO ROLE TFL_TRANSFORMER_ROLE;

-- Allow the role to read the tables that are already in RAW
GRANT SELECT ON ALL TABLES IN SCHEMA TFL_DB.RAW TO ROLE TFL_TRANSFORMER_ROLE;

-- Allow the role to read tables added to RAW later
GRANT SELECT ON FUTURE TABLES IN SCHEMA TFL_DB.RAW TO ROLE TFL_TRANSFORMER_ROLE;

-- Allow the role to create schemas; it owns those schemas, so it can create tables and views in them
GRANT CREATE SCHEMA ON DATABASE TFL_DB TO ROLE TFL_TRANSFORMER_ROLE;

-- Give the role to ACCOUNTADMIN too, so I can see the results in Snowsight
GRANT ROLE TFL_TRANSFORMER_ROLE TO ROLE ACCOUNTADMIN;

-- ===== Service user for dbt =====

-- Create a user for programs, not people; it has no password
CREATE USER IF NOT EXISTS TFL_DBT
    -- SERVICE means it can't log in with a password or use the Snowsight website
    TYPE = SERVICE
    -- Use the dbt role unless told otherwise
    DEFAULT_ROLE = TFL_TRANSFORMER_ROLE
    -- Use our warehouse unless told otherwise
    DEFAULT_WAREHOUSE = TFL_WH;

-- Give the role to the user
GRANT ROLE TFL_TRANSFORMER_ROLE TO USER TFL_DBT;

-- Give the user the PUBLIC key: replace the placeholder with the existing public key text
ALTER USER TFL_DBT SET RSA_PUBLIC_KEY='<paste public key here>';
