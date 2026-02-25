FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV SYMBOLS_PATH=/app/shared/symbols.txt

WORKDIR /app

COPY services/enricher/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY services/enricher /app/services/enricher

CMD ["python", "-m", "services.enricher.main"]