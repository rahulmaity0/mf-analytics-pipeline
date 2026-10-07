FROM python:3.11-slim

WORKDIR /app
COPY docker/api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src/mfpipeline /app/mfpipeline
ENV PYTHONPATH=/app

EXPOSE 8000
CMD ["uvicorn", "mfpipeline.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
