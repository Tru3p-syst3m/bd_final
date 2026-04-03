FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-openbsd curl && rm -rf /var/lib/apt/lists/*

RUN pip install confluent-kafka requests psycopg2-binary

COPY src/ ./src/
COPY schema/ ./schema/

CMD ["python", "src/consumer_manual.py"]
