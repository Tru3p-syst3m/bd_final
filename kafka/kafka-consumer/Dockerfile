FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-openbsd curl && rm -rf /var/lib/apt/lists/*

RUN pip install confluent-kafka psycopg2-binary

COPY src/consumer_auto.py ./consumer.py
COPY src/schema_loader.py ./schema_loader.py
COPY schema/ ./schema/

CMD ["python", "consumer.py"]
