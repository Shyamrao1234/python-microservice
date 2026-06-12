import threading
import sys
import time
import random
from app.kafka.consumer import KafkaConsumer
from app.kafka.producer import producer_instance
from app.utils.logger import get_logger
from app.config.settings import settings

logger = get_logger(__name__)

def start_kafka_consumer():
    """Start the Kafka consumer in a separate thread."""
    def message_handler(msg_value):
        logger.info(f"Handled message in consumer: {msg_value}")
        
    consumer = KafkaConsumer(group_id="example_group", topics=["example_topic"])
    logger.info("Starting Kafka consumer thread...")
    consumer.consume(callback=message_handler)

def start_kafka_producer():
    """Continuously produce messages in a separate thread."""
    logger.info("Starting Kafka producer thread...")
    counter = 1
    while True:
        try:
            # Generate some mock data
            data = {
                "id": counter,
                "message": f"Hello from continuous producer {counter}",
                "value": random.randint(1, 100)
            }
            
            # Produce the message to the same topic the consumer listens to
            producer_instance.produce_message(
                topic="example_topic",
                key=str(counter),
                value=data
            )
            
            counter += 1
            # Wait for 2 seconds before sending the next message
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error in continuous producer: {e}")
            time.sleep(5)

def main():
    """Main entry point for the microservice."""
    logger.info(f"Starting microservice in {settings.environment} environment...")
    
    # Start Kafka consumer in a background thread
    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()
    
    # Start Kafka continuous producer in a background thread
    producer_thread = threading.Thread(target=start_kafka_producer, daemon=True)
    producer_thread.start()
    
    try:
        # Keep the main thread alive to allow the background threads to process
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        # Ensure any pending producer messages are flushed out before exiting
        producer_instance.flush()
        sys.exit(0)

if __name__ == "__main__":
    main()
