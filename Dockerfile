FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies (required for confluent-kafka in some cases)
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Set the environment variables
ENV PYTHONPATH=/app

# Run the application
CMD ["python", "app/main.py"]
