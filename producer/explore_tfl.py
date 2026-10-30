"""Explore the TfL API: print a summary of the data and save samples to disk."""

# json lets us write Python data to a file as JSON text
import json
# os lets us read environment variables
import os
# sys lets us stop the script early with a message
import sys
# Path makes working with file and folder paths easy
from pathlib import Path

# requests is the library we use to call web APIs
import requests

# Web address that returns the current status of every tube line
LINE_STATUS_URL = "https://api.tfl.gov.uk/Line/Mode/tube/Status"
# Web address that returns every Santander Cycles bike point
BIKE_POINT_URL = "https://api.tfl.gov.uk/BikePoint"
# Give up on a request if TfL takes longer than this many seconds
TIMEOUT_SECONDS = 30
# Find the project's main folder: this file is in producer/, so go up two levels
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# The folder where the sample files will be saved
SAMPLES_DIR = PROJECT_ROOT / "data" / "samples"


# Define a function that calls one URL and returns the data it sends back
def fetch_json(url, key):
    # Start a block where we can catch request errors ourselves
    try:
        # Send the key as a parameter if we have one; otherwise send no parameters
        params = {"app_key": key} if key else None
        # Call the URL with those parameters and the time limit
        response = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
        # Turn a failed HTTP status (such as 401 or 500) into an error
        response.raise_for_status()
    # Catch anything that went wrong with the request
    except requests.RequestException as error:
        # Request error messages can include the full URL WITH the key, so never
        # print the error itself; print only its type and the safe URL we defined
        print(f"Request to {url} failed: {type(error).__name__}", file=sys.stderr)
        # If TfL did reply, the status code is safe to show and helps with debugging
        if error.response is not None:
            # Print the status code on its own line
            print(f"HTTP status: {error.response.status_code}", file=sys.stderr)
        # Stop the script; "from None" hides the original error, which may hold the key
        raise SystemExit(1) from None
    # Convert the JSON reply into Python lists and dictionaries, and return it
    return response.json()


# Define a function that saves data to a file in the samples folder
def save_sample(data, filename):
    # Create the samples folder if it does not exist yet
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    # Build the full path of the file to write
    path = SAMPLES_DIR / filename
    # Open the file for writing; "with" closes it automatically afterwards
    with path.open("w", encoding="utf-8") as file:
        # Write the data as pretty-printed JSON, indented by 2 spaces
        json.dump(data, file, indent=2, ensure_ascii=False)
    # Tell the user where the file was saved
    print(f"Saved {path}")


# Define the main function, which runs each step in order
def main():
    # Read the key from the environment; this gives None if it is not set
    key = os.environ.get("TFL_APP_KEY")
    # "not key" is true if the key is missing or empty
    if not key:
        # The key is optional, so say we are going without it and carry on
        print("No TFL_APP_KEY set - using anonymous access (lower rate limit)")

    # Get the status of every tube line
    lines = fetch_json(LINE_STATUS_URL, key)
    # Print how many tube lines came back
    print(f"Number of tube lines: {len(lines)}")
    # Go through the tube lines one at a time
    for line in lines:
        # A line can have several statuses, so collect all their descriptions
        descriptions = [status["statusSeverityDescription"] for status in line["lineStatuses"]]
        # Print the line name and its statuses, separated by commas
        print(f"  {line['name']}: {', '.join(descriptions)}")

    # Get every bike point
    bike_points = fetch_json(BIKE_POINT_URL, key)
    # Print how many bike points came back
    print(f"Number of bike points: {len(bike_points)}")
    # Take the first bike point as our example
    first_point = bike_points[0]
    # Print its field names (a list of a dictionary gives its keys)
    print(f"Bike point fields: {list(first_point)}")
    # Each additional property has a "key" field; collect them into a list
    property_keys = [prop["key"] for prop in first_point["additionalProperties"]]
    # Print those keys
    print(f"additionalProperties keys: {property_keys}")

    # Save the tube line response to a file
    save_sample(lines, "line_status_sample.json")
    # Save the bike point response to a file
    save_sample(bike_points, "bike_points_sample.json")


# Run main() only when this file is run directly, not when it is imported
if __name__ == "__main__":
    # Start the script
    main()
