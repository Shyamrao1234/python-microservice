from app.kafka.producer import producer_instance
from app.utils.logger import get_logger

logger = get_logger(__name__)

class ExampleService:

    @staticmethod
    def process_data(data: str) -> str:
        """Process the data and send an event to Kafka."""
        logger.info(f"Processing data: {data}")
        result = f"Processed: {data}"
        
        # Send event to Kafka
        event = {"action": "process_data", "data": data, "result": result}
        producer_instance.produce_message(topic="example_topic", key="process", value=event)
        
        return result