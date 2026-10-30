"""A Streamlit dashboard showing tube line status from Snowflake."""

# datetime and timezone let us work out how old the latest poll is
from datetime import datetime, timezone
# os lets us read environment variables
import os
# Path makes working with file and folder paths easy
from pathlib import Path

# altair draws the bar chart
import altair as alt
# pandas holds query results as tables
import pandas as pd
# snowflake.connector lets Python talk to Snowflake
import snowflake.connector
# streamlit builds the web page
import streamlit as st

# Find the project's main folder: this file is in dashboard/, so go up two levels
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Keep query results for 5 minutes (300 seconds), so each view does not wake the warehouse
CACHE_SECONDS = 300
# The environment variables this app needs in order to connect
REQUIRED_SETTINGS = [
    # The Snowflake account identifier
    "TFL_APP_ACCOUNT",
    # The user to log in as
    "TFL_APP_USER",
    # The read-only role to use
    "TFL_APP_ROLE",
    # The warehouse that runs the queries
    "TFL_APP_WAREHOUSE",
    # Where the private key file is; this is a path, not the key itself
    "TFL_APP_PRIVATE_KEY_PATH",
# End of the list
]

# The SQL that finds each line's most recent status
CURRENT_STATUS_SQL = """
    -- Choose the columns
    select line_name, status_severity, status_description, reason, ingested_at
    -- Read from the staging view, which has one row per line per poll
    from TFL_DB.ANALYTICS_STAGING.STG_LINE_STATUS
    -- Keep only the newest row for each line
    qualify row_number() over (partition by line_id order by ingested_at desc, kafka_offset desc) = 1
    -- List the lines alphabetically
    order by line_name
"""
# The SQL that finds today's good service figures
TODAY_SQL = """
    -- Choose the columns; float makes the percentage a normal Python number
    select line_name, good_service_pct::float as good_service_pct, good_service_poll_count, poll_count
    -- Read from the daily mart
    from TFL_DB.ANALYTICS_MARTS.FCT_LINE_STATUS_DAILY
    -- Keep only today, in UTC
    where status_date = convert_timezone('UTC', current_timestamp())::date
"""
# The SQL that finds today's polls without good service
DISRUPTIONS_SQL = """
    -- Choose the columns
    select ingested_at, line_name, status_severity, status_description, reason
    -- Read from the staging view
    from TFL_DB.ANALYTICS_STAGING.STG_LINE_STATUS
    -- Keep polls that were not Good Service (code 10)
    where status_severity <> 10
        -- Keep only today, in UTC
        and ingested_at::date = convert_timezone('UTC', current_timestamp())::date
    -- Show the newest first
    order by ingested_at desc
    -- Stop at 500 rows, so a long disruption can't fill the page
    limit 500
"""

# TfL's codes that mean all is well
GOOD_CODES = {10, 18}
# TfL's codes that are only information, such as the line being closed for the night
NEUTRAL_CODES = {19, 20}
# TfL's codes for smaller problems; every other code counts as serious
WARNING_CODES = {7, 8, 9, 12, 13, 14, 15}
# The symbol for each kind of status, so colour is never the only clue
GOOD_ICON = "✓"
# The symbol for information
NEUTRAL_ICON = "•"
# The symbol for smaller problems
WARNING_ICON = "▲"
# The symbol for serious problems
CRITICAL_ICON = "✕"
# The background and text colours for each symbol
STATUS_STYLES = {
    # Green with dark text
    GOOD_ICON: "background-color: #0ca30c; color: #1a1a19",
    # Grey with dark text
    NEUTRAL_ICON: "background-color: #c3c2b7; color: #1a1a19",
    # Amber with dark text
    WARNING_ICON: "background-color: #fab219; color: #1a1a19",
    # Red with white text
    CRITICAL_ICON: "background-color: #d03b3b; color: #ffffff",
# End of the colours
}


# Define a function that runs one query and keeps the answer for 5 minutes
@st.cache_data(ttl=CACHE_SECONDS, show_spinner="Reading from Snowflake...")
def run_query(sql):
    # Connect to Snowflake, using the settings below; "with" closes the connection afterwards
    with snowflake.connector.connect(
        # The account identifier
        account=os.environ["TFL_APP_ACCOUNT"],
        # The user to log in as
        user=os.environ["TFL_APP_USER"],
        # Log in with a key pair instead of a password
        authenticator="SNOWFLAKE_JWT",
        # The private key file; a relative path starts at the project's main folder
        private_key_file=str(PROJECT_ROOT / os.environ["TFL_APP_PRIVATE_KEY_PATH"]),
        # The read-only role to use
        role=os.environ["TFL_APP_ROLE"],
        # The warehouse that runs the queries
        warehouse=os.environ["TFL_APP_WAREHOUSE"],
    # End of the connection settings
    ) as connection:
        # Create a cursor, which runs SQL; "with" closes it afterwards
        with connection.cursor() as cursor:
            # Run the query
            cursor.execute(sql)
            # Read the column names in lower case
            columns = [column[0].lower() for column in cursor.description]
            # Return the rows as a table with those column names
            return pd.DataFrame(cursor.fetchall(), columns=columns)


# Define a function that adds a symbol to a status, based on its code
def status_label(severity, description):
    # Check for the codes that mean all is well
    if severity in GOOD_CODES:
        # Use the good symbol
        icon = GOOD_ICON
    # Check for the information codes
    elif severity in NEUTRAL_CODES:
        # Use the information symbol
        icon = NEUTRAL_ICON
    # Check for the smaller problems
    elif severity in WARNING_CODES:
        # Use the warning symbol
        icon = WARNING_ICON
    # Everything else counts as serious
    else:
        # Use the critical symbol
        icon = CRITICAL_ICON
    # Join the symbol and the words
    return f"{icon} {description}"


# Define a function that keeps only the lines chosen in the sidebar
def filter_lines(table, selected_lines):
    # Choosing nothing means show every line
    if not selected_lines:
        # Return the table as it is
        return table
    # Otherwise keep only the chosen lines
    return table[table["line_name"].isin(selected_lines)]


# Define a function that shows a table with a coloured Status column
def show_status_table(table):
    # Work out the Status text for each row
    labels = [status_label(row.status_severity, row.status_description) for row in table.itertuples()]
    # Make a copy with friendlier column names, and with the new Status text
    display = table.assign(Status=labels).rename(columns={"line_name": "Line", "reason": "Reason"})
    # Show an empty Reason as blank
    display["Reason"] = display["Reason"].fillna("")
    # Show a time column, if this table has one
    if "ingested_at" in display:
        # Turn the time into easy-to-read text
        display["Time (UTC)"] = display["ingested_at"].dt.strftime("%H:%M:%S")
        # Choose the columns, with the time first
        display = display[["Time (UTC)", "Line", "Status", "Reason"]]
    # Otherwise there is no time to show
    else:
        # Choose the columns
        display = display[["Line", "Status", "Reason"]]
    # Colour the Status column, using the symbol at the start of each value
    styled = display.style.map(lambda label: STATUS_STYLES[label[0]], subset=["Status"])
    # Draw the table across the full width of the page
    st.dataframe(styled, hide_index=True, width="stretch")


# Define a function that draws the bar chart of good service
def show_good_service_chart(today):
    # Start from today's numbers, and make label text such as 0% or 99.6%
    base = alt.Chart(today).transform_calculate(label="format(datum.good_service_pct, '.1~f') + '%'").encode(
        # The bar's length is the percentage; the scale runs to 110 to leave room for a 100% label, but ticks stop at 100
        x=alt.X("good_service_pct:Q", title="% of polls with Good Service", scale=alt.Scale(domain=[0, 110]), axis=alt.Axis(values=[0, 20, 40, 60, 80, 100])),
        # One row per line, with the worst service at the top
        y=alt.Y("line_name:N", title=None, sort="x"),
        # Show the exact numbers when hovering over a bar
        tooltip=[
            # The line's name
            alt.Tooltip("line_name:N", title="Line"),
            # The percentage
            alt.Tooltip("good_service_pct:Q", title="% good service"),
            # The number of good polls
            alt.Tooltip("good_service_poll_count:Q", title="Good polls"),
            # The total number of polls
            alt.Tooltip("poll_count:Q", title="All polls"),
        # End of the hover details
        ],
    # End of the settings shared by the bars and their labels
    )
    # Draw one thin bar per line, with rounded ends
    bars = base.mark_bar(size=14, cornerRadiusEnd=4, color="#2a78d6")
    # Choose label text that shows up on the page: white on the dark theme, dark on the light theme
    label_colour = "#ffffff" if st.context.theme.type == "dark" else "#1a1a19"
    # Write the label just past the end of each bar, so even a 0% line shows its number
    labels = base.mark_text(align="left", dx=4, color=label_colour).encode(text="label:N")
    # Put the labels on top of the bars, giving every line enough height for its name
    chart = (bars + labels).properties(height=alt.Step(26))
    # Draw the chart across the full width of the page
    st.altair_chart(chart, width="stretch")
    # Offer the same numbers as a table, for anyone who prefers it
    with st.expander("Show as a table"):
        # Rename the columns to make them easier to read
        names = {"line_name": "Line", "good_service_pct": "% good service", "good_service_poll_count": "Good polls", "poll_count": "All polls"}
        # Draw the table
        st.dataframe(today.rename(columns=names), hide_index=True, width="stretch")


# Define the main function, which builds the page from top to bottom
def main():
    # Set the browser tab's title, and use the full width of the screen
    st.set_page_config(page_title="TfL Tube Status", layout="wide")
    # Find the settings that are missing or empty
    missing = [name for name in REQUIRED_SETTINGS if not os.environ.get(name)]
    # "if missing" is true if the list has anything in it
    if missing:
        # Show which settings are missing
        st.error(f"Please set these environment variables: {', '.join(missing)}")
        # Stop building the page
        st.stop()

    # Add a heading to the sidebar
    st.sidebar.header("Options")
    # Add a refresh button; it is true on the run right after it is clicked
    if st.sidebar.button("Refresh data"):
        # Forget the saved query results
        st.cache_data.clear()
        # Build the page again from the top
        st.rerun()

    # Start a block where we can catch Snowflake errors
    try:
        # Read each line's most recent status
        current = run_query(CURRENT_STATUS_SQL)
        # Read today's good service figures
        today = run_query(TODAY_SQL)
        # Read today's polls without good service
        disruptions = run_query(DISRUPTIONS_SQL)
    # Catch anything that went wrong with Snowflake
    except snowflake.connector.errors.Error as error:
        # Show only the error's type, keeping the page simple and free of connection details
        st.error(f"Could not read from Snowflake: {type(error).__name__}. See the terminal for details.")
        # Also print the full message in the terminal; it does not contain the private key
        print(error)
        # Stop building the page
        st.stop()

    # There is nothing to show without data
    if current.empty:
        # Explain what to do
        st.warning("No data yet. Run the Airflow pipeline, then click Refresh data.")
        # Stop building the page
        st.stop()

    # Add a line filter to the sidebar; choosing nothing means every line
    selected_lines = st.sidebar.multiselect("Lines", options=current["line_name"].tolist(), placeholder="All lines")

    # Find the time of the latest poll
    latest_poll = current["ingested_at"].max()
    # Find the current UTC time without a time zone, to match the data
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    # Work out how old the latest poll is, in whole minutes
    age_minutes = int((now - latest_poll).total_seconds() // 60)
    # Show the page's main heading
    st.title("TfL Tube Status")
    # Show the time of the latest poll and how old it is
    st.markdown(f"**Latest poll:** {latest_poll:%d %b %Y, %H:%M} UTC ({age_minutes} minutes ago)")

    # Show the heading for the current status
    st.header("Right now")
    # Explain that this is only as new as the last pipeline run
    st.caption("Each line's status at the latest poll loaded into Snowflake. The pipeline loads data once an hour.")
    # Show the current status of the chosen lines
    show_status_table(filter_lines(current, selected_lines))

    # Show the heading for today's chart
    st.header("Today")
    # Keep only the chosen lines
    today_lines = filter_lines(today, selected_lines)
    # There is no chart without data for today
    if today_lines.empty:
        # Explain why there is no chart
        st.info("No data for today (UTC) yet.")
    # Otherwise we have data to draw
    else:
        # Draw the chart
        show_good_service_chart(today_lines)

    # Show the heading for the disruptions table
    st.header("Disruptions")
    # Explain what is in this table
    st.caption("Today's polls (UTC) without Good Service, newest first, from the latest 500 across all lines.")
    # Keep only the chosen lines
    disruption_lines = filter_lines(disruptions, selected_lines)
    # There may be no disruptions at all
    if disruption_lines.empty:
        # Share the good news
        st.success("No disruptions recorded today.")
    # Otherwise we have rows to show
    else:
        # Show the table
        show_status_table(disruption_lines)


# Run main() each time Streamlit builds the page
main()
