# Jaeger Trace Scraper

This application reads Jaeger-like JSON traces from a file and ingests them into a PostgreSQL database using a Python script.

---

## Overview

- **trace_scrapper**: Docker service that builds from your local `Dockerfile`.  
  - It runs `tail -f /dev/null` by default, so the container stays alive until you manually run the Python script.
- **db**: A PostgreSQL database container.

When you run the script (`python app.py /app/output_100.json`), it will read the specified JSON file and populate the database using the credentials found in your `.env` file.

---

## Prerequisites

- **Docker** and **Docker Compose** must be installed on your machine.
- A `.env` file containing environment variables for database configuration.

---

## Setup

1. **Rename the environment file**:  
   - The file `.env.bak` should be renamed to `.env`.

2. **Update `.env` as needed** with your desired database credentials, for example:
   ```bash
   DB_USER=postgres
   DB_PASSWORD=secret
   DB_NAME=jaeger_traces
   DB_PORT=5432

## Usage

1. Start the containers with `docker-compose up -d --build`.
2. The trace_scrapper container is configured to idle (`tail -f /dev/null`) so that you can manually run the Python script inside it. Enter the container using `docker-compose exec trace_scraper /bin/bash` or `docker exec -it trace_scraper sh`.
3. Run the script: `python app.py /app/output_100.json`
   - Adjust /app/output_100.json to point to the JSON file you want to ingest.
   - The script will connect to the db container, read the JSON, create necessary tables if they do not exist already and insert the data into the configured PostgreSQL database.