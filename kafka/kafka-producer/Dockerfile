FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-openbsd curl && rm -rf /var/lib/apt/lists/*

RUN pip install confluent-kafka requests

COPY src/ ./src/
COPY schema/ ./schema/
COPY config/ ./config/
COPY init-kafka.sh ./init-kafka.sh

RUN chmod +x ./init-kafka.sh

CMD ["python", "src/producer.py"]
