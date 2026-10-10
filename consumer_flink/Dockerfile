FROM flink:1.19.3-scala_2.12-java17

WORKDIR /opt/stream

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/stream/requirements.txt

RUN pip3 install --no-cache-dir -r /opt/stream/requirements.txt

COPY jars/*.jar /opt/flink/lib/

COPY app/ /opt/stream/app/

ENV PYTHONPATH=/opt/stream/app

RUN mkdir -p /opt/stream/checkpoints