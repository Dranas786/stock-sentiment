# services/api/Dockerfile

FROM python:3.11-slim

# Prevent Python from buffering logs (so you see logs instantly in docker logs)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps first (better docker layer caching)
# COPY pyproject.toml /app/pyproject.toml
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# Copy the project code
COPY shared /app/shared
COPY services/api /app/services/api

EXPOSE 8000

# Run FastAPI via uvicorn
CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
