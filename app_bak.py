import json
import psycopg2
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()


# Database connection setup
def connect_to_db():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOSTNAME"),
        port=os.getenv("DB_PORT")
    )


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        -- Table: traces
        CREATE TABLE IF NOT EXISTS traces (
            trace_id VARCHAR(50) PRIMARY KEY
        );

        -- Table: spans
        CREATE TABLE IF NOT EXISTS spans (
            span_id VARCHAR(50) PRIMARY KEY,
            trace_id VARCHAR(50) REFERENCES traces(trace_id),
            operation_name TEXT,
            flags INTEGER,
            start_time BIGINT,
            duration BIGINT,
            process_id VARCHAR(50),
            warnings TEXT,
            -- parent_span_id VARCHAR(50) REFERENCES spans(span_id)
            parent_span_id VARCHAR(50)
        );

        -- Table: references
        CREATE TABLE IF NOT EXISTS "references" (
            id SERIAL PRIMARY KEY,
            span_id VARCHAR(50) REFERENCES spans(span_id),
            ref_type TEXT,
            parent_span_id VARCHAR(50)
        );

        -- Table: tags
        CREATE TABLE IF NOT EXISTS tags (
            id SERIAL PRIMARY KEY,
            span_id VARCHAR(50) REFERENCES spans(span_id),
            key TEXT,
            type TEXT,
            value TEXT
        );

        -- Table: logs
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            span_id VARCHAR(50) REFERENCES spans(span_id),
            timestamp BIGINT,
            log_message TEXT
        );

        -- Table: log_fields
        CREATE TABLE IF NOT EXISTS log_fields (
            id SERIAL PRIMARY KEY,
            log_id INTEGER REFERENCES logs(id),
            key TEXT,
            type TEXT,
            value TEXT
        );
        
        -- Table: processes
        CREATE TABLE IF NOT EXISTS processes (
            process_id VARCHAR(50) PRIMARY KEY,
            service_name TEXT NOT NULL
        );

        -- Table: process_tags
        CREATE TABLE IF NOT EXISTS process_tags (
            id SERIAL PRIMARY KEY,
            process_id VARCHAR(50) REFERENCES processes(process_id),
            key TEXT NOT NULL,
            type TEXT NOT NULL,
            value TEXT NOT NULL
        );
        
        """)
        conn.commit()

# Insert data into the database
def insert_data(conn, trace_data):
    with conn.cursor() as cur:
        for trace in trace_data['data']:
            # Insert trace
            cur.execute("INSERT INTO traces (trace_id) VALUES (%s) ON CONFLICT (trace_id) DO NOTHING;",
                        (trace['traceID'],))

            processes = trace.get('processes', {})
            for process_id, process in processes.items():
                # Insert into processes table
                cur.execute("""
                INSERT INTO processes (process_id, service_name)
                VALUES (%s, %s)
                ON CONFLICT (process_id) DO NOTHING;
                """,
                (process_id, process['serviceName']))

                # Insert process tags
                for tag in process.get('tags', []):
                    cur.execute("""
                    INSERT INTO process_tags (process_id, key, type, value)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (process_id, tag['key'], tag['type'], str(tag['value'])))

            for span in trace['spans']:
                # Insert span
                cur.execute("""
                INSERT INTO spans (span_id, trace_id, operation_name, flags, start_time, duration, process_id, warnings, parent_span_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (span_id) DO NOTHING;
                """,
                (span['spanID'], span['traceID'], span['operationName'], span['flags'], span['startTime'],
                 span['duration'], span['processID'], span.get('warnings'),
                 span['references'][0]['spanID'] if span.get('references') else None))

                # Insert references
                for ref in span.get('references', []):
                    cur.execute("""
                    INSERT INTO "references" (span_id, ref_type, parent_span_id) 
                    VALUES (%s, %s, %s);
                    """,
                    (span['spanID'], ref['refType'], ref['spanID']))

                # Insert tags
                for tag in span.get('tags', []):
                    cur.execute("""
                    INSERT INTO tags (span_id, key, type, value) 
                    VALUES (%s, %s, %s, %s);
                    """,
                    (span['spanID'], tag['key'], tag['type'], str(tag['value'])))

                # Insert logs
                for log in span.get('logs', []):
                    cur.execute("""
                    INSERT INTO logs (span_id, timestamp, log_message)
                    VALUES (%s, %s, %s)
                    RETURNING id;
                    """,
                    (span['spanID'], log['timestamp'], log.get('message')))
                    log_id = cur.fetchone()[0]

                    # Insert log fields
                    for field in log.get('fields', []):
                        cur.execute("""
                        INSERT INTO log_fields (log_id, key, type, value)
                        VALUES (%s, %s, %s, %s);
                        """,
                        (log_id, field['key'], field['type'], str(field['value'])))
        conn.commit()


# Main function
def main():
    # Load JSON
    with open("output_100.json", "r") as file:
        trace_data = json.load(file)

    # Connect to the database
    conn = connect_to_db()

    try:
        create_tables(conn)  # Create tables
        insert_data(conn, trace_data)  # Insert data
    finally:
        conn.close()  # Close connection


# def main():
#     try:
#         conn = connect_to_db()
#         # logging.info(conn)
#         print(conn)
#     except Exception as err:
#         logging.info("Something went wrong: {}".format(err))
#         # print("Something went wrong: {}".format(err))


if __name__ == "__main__":
    main()
