import argparse
import json
import os
import logging
from sqlalchemy import create_engine, Column, String, Integer, BigInteger, Text, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
from dotenv import load_dotenv
from urllib.parse import quote
from eralchemy import render_er

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
    # spans = relationship("Span", back_populates="trace")


class Span(Base):
    __tablename__ = 'spans'
    span_id = Column(String(50), primary_key=True)
    trace_id = Column(String(50), ForeignKey('traces.trace_id'))
    operation_name = Column(Text)
    flags = Column(Integer)
    start_time = Column(BigInteger)
    duration = Column(BigInteger)
    process_id = Column(String(50), ForeignKey('processes.process_id'))
    warnings = Column(Text, nullable=True)
    # omitting these as we're just inserting data and won't be using ORM to access data
    # tags = relationship("Tag", back_populates="span")
    # logs = relationship("Log", back_populates="span")
    # trace = relationship("Trace", back_populates="spans")


class SpanReference(Base):
    __tablename__ = 'span_references'
    id = Column(Integer, primary_key=True, autoincrement=True)
    span_id = Column(String(50), ForeignKey('spans.span_id'))
    ref_type = Column(Text)  # "CHILD_OF" or "FOLLOWS_FROM"
    ref_span_id = Column(String(50)) # the referenced span (parent or prior)


class Tag(Base):
    __tablename__ = 'tags'
    id = Column(Integer, primary_key=True, autoincrement=True)
    span_id = Column(String(50), ForeignKey('spans.span_id'))
    key = Column(Text)
    type = Column(Text)
    value = Column(Text)
    # span = relationship("Span", back_populates="tags")


class Log(Base):
    __tablename__ = 'logs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    span_id = Column(String(50), ForeignKey('spans.span_id'))
    timestamp = Column(BigInteger)
    log_message = Column(Text, nullable=True)
    # span = relationship("Span", back_populates="logs")


class LogField(Base):
    __tablename__ = 'log_fields'
    id = Column(Integer, primary_key=True, autoincrement=True)
    log_id = Column(Integer, ForeignKey('logs.id'))
    key = Column(Text)
    type = Column(Text)
    value = Column(Text)


class Process(Base):
    __tablename__ = 'processes'
    process_id = Column(String(50), primary_key=True)
    service_name = Column(Text)
    # tags = relationship("ProcessTag", back_populates="process")


class ProcessTag(Base):
    __tablename__ = 'process_tags'
    id = Column(Integer, primary_key=True, autoincrement=True)
    process_id = Column(String(50), ForeignKey('processes.process_id'))
    key = Column(Text)
    type = Column(Text)
    value = Column(Text)
    # process = relationship("Process", back_populates="tags")


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
            process_obj = Process(process_id=process_id, service_name=process_data['serviceName'])
            session.merge(process_obj)

            for tag in process_data.get('tags', []):
                tag_obj = ProcessTag(process_id=process_id, key=tag['key'], type=tag['type'], value=str(tag['value']))
                session.add(tag_obj)

        for span in trace['spans']:
            span_obj = Span(
                span_id=span['spanID'],
                trace_id=span['traceID'],
                operation_name=span['operationName'],
                flags=span['flags'],
                start_time=span['startTime'],
                duration=span['duration'],
                process_id=span['processID'],
                warnings=span.get('warnings'),
            )
            session.merge(span_obj)

            references_list = span.get('references', [])
            for ref in references_list:
                ref_obj = SpanReference(
                    span_id=span['spanID'],
                    ref_type=ref.get('refType'),
                    ref_span_id=ref.get('spanID')
                )
                session.add(ref_obj)

            for tag in span.get('tags', []):
                tag_obj = Tag(span_id=span['spanID'], key=tag['key'], type=tag['type'], value=str(tag['value']))
                session.add(tag_obj)

            for log in span.get('logs', []):
                log_obj = Log(span_id=span['spanID'], timestamp=log['timestamp'], log_message=log.get('message'))
                session.add(log_obj)
                session.flush()  # To retrieve the auto-generated log_id

                for field in log.get('fields', []):
                    field_obj = LogField(log_id=log_obj.id, key=field['key'], type=field['type'], value=str(field['value']))
                    session.add(field_obj)

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

    insert_data(session, trace_data)

    generate_erd()


if __name__ == "__main__":
    main()
