FROM python:3.11-slim

# Set the DAGSTER_HOME environment variable
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Create necessary directories for Dagster
RUN mkdir -p $DAGSTER_HOME && \
    mkdir -p $DAGSTER_HOME/storage && \
    mkdir -p $DAGSTER_HOME/compute_logs && \
    mkdir -p $DAGSTER_HOME/local_artifact_storage

# Set the working directory
WORKDIR $DAGSTER_HOME

# Copy the requirements file and install dependencies
COPY requirements.txt $DAGSTER_HOME
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of the application code
COPY . $DAGSTER_HOME

# Default command
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]
