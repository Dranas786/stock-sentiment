# services/collector/Dockerfile

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY services/collector /app/services/collector

CMD ["python", "-m", "services.collector.main"]
