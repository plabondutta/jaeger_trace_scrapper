import argparse
import json
import os
import logging
from sqlalchemy import create_engine, Column, String, Integer, BigInteger, Text, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
from urllib.parse import quote
from eralchemy import render_er
import uuid

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)

# SQLAlchemy Base
Base = declarative_base()

# SQLAlchemy Models
class Trace(Base):
    __tablename__ = 'traces'
    trace_id = Column(String(50), primary_key=True)


class Span(Base):
    __tablename__ = 'spans'
    span_id = Column(String(50), primary_key=True)
    trace_id = Column(String(50), ForeignKey('traces.trace_id'))
    operation_name = Column(Text, index=True)
    flags = Column(Integer)
    start_time = Column(BigInteger)
    duration = Column(BigInteger)
    process_id = Column(String(50), ForeignKey('processes.process_id'))
    service_name = Column(Text)
    warnings = Column(Text, nullable=True)
    tags = Column(JSONB)  # JSON array for tags


class ParentChildRelation(Base):
    __tablename__ = 'parent_child_relations'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trace_id = Column(String(50), ForeignKey('traces.trace_id'))
    child_span_id = Column(String(50))
    parent_span_id = Column(String(50))


class Log(Base):
    __tablename__ = 'logs'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    span_id = Column(String(50), ForeignKey('spans.span_id'))
    timestamp = Column(BigInteger)
    log_message = Column(Text, nullable=True)
    fields = Column(JSONB)  # JSON array for log fields


class Process(Base):
    __tablename__ = 'processes'
    process_id = Column(String(50), primary_key=True)
    service_name = Column(Text)
    tags = Column(JSONB)  # JSON array for process tags


def get_engine():
    encoded_password = quote(os.getenv('DB_PASSWORD', ''))
    db_url = f"postgresql://{os.getenv('DB_USER')}:{encoded_password}@" \
             f"{os.getenv('DB_HOSTNAME')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(db_url)


def insert_data(session, trace_data):
    for trace in trace_data['data']:
        trace_obj = Trace(trace_id=trace['traceID'])
        session.merge(trace_obj)

        processes = trace.get('processes', {})
        for process_id, process_data in processes.items():
            tags = [{tag['key']: tag['value']} for tag in process_data.get('tags', [])]
            process_obj = Process(
                process_id=process_id,
                service_name=process_data['serviceName'],
                tags=tags
            )
            session.merge(process_obj)

        for span in trace['spans']:
            tags = [{tag['key']: tag['value']} for tag in span.get('tags', [])]
            service_name = processes.get(span['processID'], {}).get('serviceName', None)
            span_obj = Span(
                span_id=span['spanID'],
                trace_id=span['traceID'],
                operation_name=span['operationName'],
                flags=span['flags'],
                start_time=span['startTime'],
                duration=span['duration'],
                process_id=span['processID'],
                service_name=service_name,
                warnings=span.get('warnings'),
                tags=tags
            )
            session.merge(span_obj)

            for ref in span.get('references', []):
                if ref['refType'] == "CHILD_OF":
                    child_span_id = span['spanID']
                    parent_span_id = ref.get('spanID')
                elif ref['refType'] == "FOLLOWS_FROM":
                    parent_span_id = span['spanID']
                    child_span_id = ref.get('spanID')
                else:
                    continue

                ref_obj = ParentChildRelation(
                    trace_id=span['traceID'],
                    child_span_id=child_span_id,
                    parent_span_id=parent_span_id
                )
                session.add(ref_obj)

            for log in span.get('logs', []):
                fields = [{field['key']: field['value']} for field in log.get('fields', [])]
                log_obj = Log(
                    span_id=span['spanID'],
                    timestamp=log['timestamp'],
                    log_message=log.get('message'),
                    fields=fields
                )
                session.add(log_obj)

    session.commit()


def generate_erd(output_file="erd_diagram.png"):
    try:
        encoded_password = quote(os.getenv('DB_PASSWORD', ''))
        db_url = f"postgresql://{os.getenv('DB_USER')}:{encoded_password}@" \
                 f"{os.getenv('DB_HOSTNAME')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        render_er(db_url, output_file)
        print(f"ERD diagram saved to {output_file}")
    except Exception as e:
        print(f"Error generating ERD: {e}")


def main():
    parser = argparse.ArgumentParser(description="Load Jaeger trace JSON into PostgreSQL.")
    parser.add_argument("filename", help="Path to the JSON file containing trace data")
    args = parser.parse_args()

    with open(args.filename, "r") as file:
        trace_data = json.load(file)

    engine = get_engine()
    session_factory = sessionmaker(bind=engine)

    Base.metadata.create_all(engine)

    with session_factory() as session:
        insert_data(session, trace_data)

    generate_erd()


if __name__ == "__main__":
    main()
