FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-openbsd curl && rm -rf /var/lib/apt/lists/*

RUN pip install confluent-kafka requests httpx psycopg2-binary certifi authlib fastavro cachetools attrs sqlite3

COPY src/producer.py ./producer.py
COPY src/schema_loader.py ./schema_loader.py
COPY schema/ ./schema/

CMD ["python", "producer.py"]
