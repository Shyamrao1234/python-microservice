# Python Microservice

A production-ready Python microservice using Kafka for asynchronous messaging.

## Tech Stack
- **Python 3.10+**
- **Kafka** (`confluent-kafka`)
- **Config** (`pydantic-settings`, `python-dotenv`)

## Project Structure
The project is structured into `app/`, `tests/` and contains necessary configurations for Docker and environment variables.

## Prerequisites
- Python 3.10+
- Kafka Broker (running locally or remote)
- Docker (optional, for containerization)

## Setup Instructions

### 1. Create Virtual Environment
```bash
python -m venv venv
# On Windows:
venv\Scripts\activate
# On Unix or MacOS:
source venv/bin/activate
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Environment Configuration
The `.env` file is already created with default values:
```
KAFKA_BROKER_URL=localhost:9092
ENVIRONMENT=development
```

### 4. Running the Microservice
Start the main application (this starts the Kafka consumer):
```bash
# Ensure you are at the root of the project
python -m app.main
```

## Running with Docker
A `Dockerfile` is provided which automatically builds the image.

```bash
docker build -t python-microservice .
docker run --env-file .env python-microservice
```
