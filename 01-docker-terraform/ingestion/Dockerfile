# Start with slim Python 3.13 image
FROM python:3.12.12-slim

# Set working directory
WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy application code
COPY ingestion.py .

# Set entry point
ENTRYPOINT ["python", "ingestion.py"]