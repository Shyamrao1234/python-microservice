from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings using pydantic for validation and env loading."""
    kafka_broker_url: str = "192.168.1.123:9092"
    environment: str = "development"

    class Config:
        env_file = ".env"

settings = Settings()
