-- Run this file once in a Snowsight worksheet, after dbt has created the two ANALYTICS schemas.
-- BEFORE RUNNING: replace the placeholder in the ALTER USER line, statement 16.
-- Snowsight runs one statement at a time: put the cursor on each statement and press Ctrl+Enter, top to bottom.

-- Statement 1: ACCOUNTADMIN can create roles and users, and give out all the rights below
USE ROLE ACCOUNTADMIN;

-- Statement 2: create the read-only role used by the dashboard
CREATE ROLE IF NOT EXISTS TFL_REPORTER_ROLE;

-- Statement 3: allow the role to run queries on the warehouse
GRANT USAGE ON WAREHOUSE TFL_WH TO ROLE TFL_REPORTER_ROLE;

-- Statement 4: allow the role to enter the database
GRANT USAGE ON DATABASE TFL_DB TO ROLE TFL_REPORTER_ROLE;

-- Statement 5: allow the role to enter the staging schema
GRANT USAGE ON SCHEMA TFL_DB.ANALYTICS_STAGING TO ROLE TFL_REPORTER_ROLE;

-- Statement 6: allow the role to enter the marts schema
GRANT USAGE ON SCHEMA TFL_DB.ANALYTICS_MARTS TO ROLE TFL_REPORTER_ROLE;

-- Statement 7: allow reading the tables already in staging
GRANT SELECT ON ALL TABLES IN SCHEMA TFL_DB.ANALYTICS_STAGING TO ROLE TFL_REPORTER_ROLE;

-- Statement 8: allow reading the views already in staging
GRANT SELECT ON ALL VIEWS IN SCHEMA TFL_DB.ANALYTICS_STAGING TO ROLE TFL_REPORTER_ROLE;

-- Statement 9: allow reading tables in staging that dbt creates or replaces later
GRANT SELECT ON FUTURE TABLES IN SCHEMA TFL_DB.ANALYTICS_STAGING TO ROLE TFL_REPORTER_ROLE;

-- Statement 10: allow reading views in staging that dbt creates or replaces later
GRANT SELECT ON FUTURE VIEWS IN SCHEMA TFL_DB.ANALYTICS_STAGING TO ROLE TFL_REPORTER_ROLE;

-- Statement 11: allow reading the tables already in marts
GRANT SELECT ON ALL TABLES IN SCHEMA TFL_DB.ANALYTICS_MARTS TO ROLE TFL_REPORTER_ROLE;

-- Statement 12: allow reading the views already in marts
GRANT SELECT ON ALL VIEWS IN SCHEMA TFL_DB.ANALYTICS_MARTS TO ROLE TFL_REPORTER_ROLE;

-- Statement 13: allow reading tables in marts that dbt creates or replaces later
GRANT SELECT ON FUTURE TABLES IN SCHEMA TFL_DB.ANALYTICS_MARTS TO ROLE TFL_REPORTER_ROLE;

-- Statement 14: allow reading views in marts that dbt creates or replaces later
GRANT SELECT ON FUTURE VIEWS IN SCHEMA TFL_DB.ANALYTICS_MARTS TO ROLE TFL_REPORTER_ROLE;

-- Statement 15: create a user for programs, not people; it has no password
CREATE USER IF NOT EXISTS TFL_APP
    -- SERVICE means it can't log in with a password or use the Snowsight website
    TYPE = SERVICE
    -- Use the read-only role unless told otherwise
    DEFAULT_ROLE = TFL_REPORTER_ROLE
    -- Use our warehouse unless told otherwise
    DEFAULT_WAREHOUSE = TFL_WH;

-- Statement 16: give the user the PUBLIC key: replace the placeholder with the existing public key text
ALTER USER TFL_APP SET RSA_PUBLIC_KEY='<paste public key here>';

-- Statement 17: give the role to the user
GRANT ROLE TFL_REPORTER_ROLE TO USER TFL_APP;
