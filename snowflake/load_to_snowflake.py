"""Upload the landing JSON Lines files to a Snowflake stage and load them into LINE_STATUS_RAW."""

# os lets us read environment variables
import os
# Path makes working with file and folder paths easy
from pathlib import Path
# sys lets us stop the script early with a message
import sys

# snowflake.connector lets Python talk to Snowflake
import snowflake.connector

# Find the project's main folder: this file is in snowflake/, so go up two levels
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Read the folder of batch files; a relative path is treated as relative to the project's main folder
LANDING_DIR = PROJECT_ROOT / (os.environ.get("LANDING_DIR") or "data/landing/line_status")
# The stage that receives the files
STAGE_NAME = "LINE_STATUS_STAGE"
# The SQL that loads the files in the stage; load history makes it skip files already loaded
COPY_SQL = f"""
    COPY INTO LINE_STATUS_RAW (record, source_file)
    FROM (SELECT $1, METADATA$FILENAME FROM @{STAGE_NAME})
    FILE_FORMAT = (FORMAT_NAME = 'LINE_STATUS_JSON_FORMAT')
"""
# The environment variables this script needs in order to connect
REQUIRED_SETTINGS = [
    # The Snowflake account identifier
    "SNOWFLAKE_ACCOUNT",
    # The user to log in as
    "SNOWFLAKE_USER",
    # The role to use
    "SNOWFLAKE_ROLE",
    # The warehouse that runs the COPY
    "SNOWFLAKE_WAREHOUSE",
    # The database holding the stage and table
    "SNOWFLAKE_DATABASE",
    # The schema holding the stage and table
    "SNOWFLAKE_SCHEMA",
    # Where the private key file is; this is a path, not the key itself
    "SNOWFLAKE_PRIVATE_KEY_PATH",
# End of the list
]


# Define a function that reads every setting, or stops if any are missing
def read_settings():
    # Find the settings that are missing or empty
    missing = [name for name in REQUIRED_SETTINGS if not os.environ.get(name)]
    # "if missing" is true if the list has anything in it
    if missing:
        # Stop the script and name the missing settings
        sys.exit(f"Please set these environment variables: {', '.join(missing)}")
    # Return the settings, using each variable's name to look it up
    return {name: os.environ[name] for name in REQUIRED_SETTINGS}


# Define a function that counts the rows in each local batch file
def count_local_rows(files):
    # Start with no counts
    rows_by_file = {}
    # Go through the files one at a time
    for path in files:
        # Open the file for reading; "with" closes it automatically afterwards
        with path.open("rb") as file:
            # Each line is one record, so count the lines
            rows_by_file[path.name] = sum(1 for _ in file)
    # Return the count for each file name
    return rows_by_file


# Define a function that fetches a query's results as dictionaries of column name and value
def fetch_results(cursor):
    # Read the column names in lower case
    columns = [column[0].lower() for column in cursor.description]
    # Pair each row's values with the column names
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# Define a function that uploads the local files to the stage, one date folder at a time
def upload_files(cursor, files):
    # Count the files uploaded now
    uploaded = 0
    # Count the files that were already in the stage
    already_in_stage = 0
    # Go through each folder that holds at least one batch file
    for folder in sorted({path.parent for path in files}):
        # PUT all this folder's files into a matching stage folder; existing files are not overwritten
        cursor.execute(f"PUT 'file://{folder.as_posix()}/*.jsonl' '@{STAGE_NAME}/{folder.name}/'")
        # Go through the result for each file
        for result in fetch_results(cursor):
            # UPLOADED means that the file is new to the stage
            if result["status"] == "UPLOADED":
                # Add one to the uploaded count
                uploaded += 1
            # Anything else means that the file was not sent again
            else:
                # Add one to the already-there count
                already_in_stage += 1
    # Return both counts
    return uploaded, already_in_stage


# Define a function that loads the stage's files and works out the row counts
def copy_files(cursor, rows_by_file):
    # Run COPY INTO, which skips files that load history says are already loaded
    cursor.execute(COPY_SQL)
    # Count the rows loaded now
    rows_loaded = 0
    # Collect the names of the local files loaded now
    loaded_names = set()
    # Go through the result; if nothing was loaded, its one row has no "file" column
    for result in fetch_results(cursor):
        # Only rows with a "file" column describe a loaded file
        if "file" in result:
            # Add this file's rows to the total
            rows_loaded += result["rows_loaded"]
            # Stage files end in .gz because PUT compresses them, so remove that to match local names
            loaded_names.add(Path(result["file"]).name.removesuffix(".gz"))
    # COPY does not report skipped files, so count the rows of local files that it did not load
    rows_skipped = sum(rows for name, rows in rows_by_file.items() if name not in loaded_names)
    # Return both counts
    return rows_loaded, rows_skipped


# Define the main function, which runs each step in order
def main():
    # Read the settings
    settings = read_settings()
    # Build the key file's full path; a relative path starts at the project's main folder
    key_path = PROJECT_ROOT / settings["SNOWFLAKE_PRIVATE_KEY_PATH"]
    # Stop early if the key file is not there
    if not key_path.is_file():
        # Stop the script and show the path only, never the key
        sys.exit(f"Private key file not found: {key_path}")
    # Find every finished batch file; unfinished .tmp files do not match
    files = sorted(LANDING_DIR.rglob("*.jsonl"))
    # Count the rows in each one
    rows_by_file = count_local_rows(files)
    # Connect to Snowflake, using the settings below; "with" closes the connection afterwards
    with snowflake.connector.connect(
        # The account identifier
        account=settings["SNOWFLAKE_ACCOUNT"],
        # The user to log in as
        user=settings["SNOWFLAKE_USER"],
        # Log in with a key pair instead of a password
        authenticator="SNOWFLAKE_JWT",
        # The private key file, which proves who we are; the key never leaves this laptop
        private_key_file=str(key_path),
        # The role to use
        role=settings["SNOWFLAKE_ROLE"],
        # The warehouse that runs the COPY
        warehouse=settings["SNOWFLAKE_WAREHOUSE"],
        # The database holding the stage and table
        database=settings["SNOWFLAKE_DATABASE"],
        # The schema holding the stage and table
        schema=settings["SNOWFLAKE_SCHEMA"],
    # End of the connection settings
    ) as connection:
        # Create a cursor, which runs SQL; "with" closes it afterwards
        with connection.cursor() as cursor:
            # Upload the local files to the stage
            uploaded, already_in_stage = upload_files(cursor, files)
            # Load the stage's files into the table
            rows_loaded, rows_skipped = copy_files(cursor, rows_by_file)
    # Print how many files were uploaded
    print(f"Files uploaded: {uploaded} (already in stage: {already_in_stage})")
    # Print how many rows were added to the table
    print(f"Rows loaded: {rows_loaded}")
    # Print how many rows were skipped
    print(f"Rows skipped (already loaded): {rows_skipped}")


# Run main() only when this file is run directly, not when it is imported
if __name__ == "__main__":
    # Start the script
    main()
