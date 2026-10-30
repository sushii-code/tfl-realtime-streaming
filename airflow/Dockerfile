# Start from the official Airflow image, pinned to one exact version
FROM apache/airflow:3.3.2-python3.12

# Switch to the admin user, so we can create a folder outside the home folder
USER root

# Copy only our list of Python packages; our code and keys are mounted when the container runs
COPY requirements.txt /opt/tfl-requirements.txt

# Create a separate Python environment, so our packages can't clash with Airflow's own
RUN python3 -m venv /opt/tfl-venv

# Install our pinned packages, including dbt; the image turns on PIP_USER, which a venv does not allow
RUN PIP_USER=false /opt/tfl-venv/bin/pip install --no-cache-dir -r /opt/tfl-requirements.txt

# Switch back to the normal Airflow user, which is safer than running as admin
USER airflow
