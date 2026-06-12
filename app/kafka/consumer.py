import json
from confluent_kafka import Consumer, KafkaError
from app.config.settings import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

class KafkaConsumer:
    def __init__(self, group_id: str, topics: list):
        conf = {
            'bootstrap.servers': settings.kafka_broker_url,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe(topics)

    def consume(self, callback):
        """Consume messages and pass them to the callback function."""
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.info(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                    elif msg.error():
                        logger.error(f"Error: {msg.error()}")
                    continue
                
                # Message is valid
                value = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Received message: {value}")
                callback(value) # don't know what to do with the data, so just giving to another function
        except KeyboardInterrupt:
            logger.info("Aborted by user")
        finally:
            self.consumer.close()
