FROM spark:3.5.9

USER root
WORKDIR /opt/stream

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/stream/requirements.txt

RUN pip3 install --no-cache-dir -r /opt/stream/requirements.txt

COPY jars/*.jar /opt/spark/jars/

COPY app/ /opt/stream/app/

ENV PYTHONPATH=/opt/stream/app

RUN mkdir -p /opt/stream/checkpoints