# services/aggregator/Dockerfile

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/aggregator/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY services/aggregator /app/services/aggregator

CMD ["python", "-m", "services.aggregator.main"]
