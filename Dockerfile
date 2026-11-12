FROM python:3.11-slim

# Runtime deps needed by confluent-kafka's bundled librdkafka for SASL + SSL
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsasl2-2 \
        libsasl2-modules \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DATA_DIR=/data \
    HOST=0.0.0.0 \
    PORT=8000 \
    LOG_LEVEL=info

VOLUME ["/data"]
EXPOSE 8000

CMD ["python", "run.py"]
