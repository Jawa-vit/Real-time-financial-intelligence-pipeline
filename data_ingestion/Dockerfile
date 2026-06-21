FROM python:3.11-slim

WORKDIR /app

COPY data_ingestion/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY data_ingestion/ ./data_ingestion/

WORKDIR /app/data_ingestion
CMD ["python", "finance_api_producer.py"]
