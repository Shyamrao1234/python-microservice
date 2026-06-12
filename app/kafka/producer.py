import json
from confluent_kafka import Producer
from app.config.settings import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

class KafkaProducer:

    def __init__(self):
        conf = {'bootstrap.servers': settings.kafka_broker_url}
        self.producer = Producer(conf)

    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def produce_message(self, topic: str, key: str, value: dict):
        """Produce a message to the specified Kafka topic with backpressure handling."""
        encoded_key = key.encode('utf-8')
        encoded_value = json.dumps(value).encode('utf-8')
        
        while True:
            try:
                self.producer.produce(
                    topic,
                    key=encoded_key,
                    value=encoded_value,
                    callback=self.delivery_report
                )
                # Successfully queued the message
                break
            except BufferError:
                logger.warning("Local producer queue is full (BufferError). Waiting for space...")
                # The queue is full. Wait for messages to be delivered to the broker.
                # poll() serves delivery callbacks and lets the background thread send data.
                self.producer.poll(0.5)
                # Loop continues and we retry producing the same message
            except Exception as e:
                logger.error(f"Error producing message: {e}")
                break
                
        # Serve any outstanding delivery callback queue.
        self.producer.poll(0)
    def flush(self):
        self.producer.flush()

producer_instance = KafkaProducer()
