FROM python:3.12-slim

WORKDIR /app


COPY requirements.txt /app/

RUN apt-get update \
    && apt-get -y install libpq-dev gcc graphviz libgraphviz-dev \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY .env /app/

COPY . /app

EXPOSE 5000

ENV TZ=America/New_York

# Define environment variable for hot-reloading
# ENV RELOAD="true"

CMD ["tail", "-f", "/dev/null"]
