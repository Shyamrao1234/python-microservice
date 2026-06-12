import json
from unittest.mock import MagicMock, patch
from confluent_kafka import KafkaError
import pytest

from app.kafka.consumer import KafkaConsumer
from app.kafka.producer import KafkaProducer


class TestKafkaConsumer:

    @patch('app.kafka.consumer.Consumer')
    def test_Basic(self, mock_consumer_class):
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer



    @patch('app.kafka.consumer.Consumer')
    def test_consumer_init(self, mock_consumer_class):
        # Arrange
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        group_id = "test_group"
        topics = ["test_topic"]

        # Act
        consumer = KafkaConsumer(group_id, topics)

        # Assert
        assert mock_consumer_class.called, "Consumer class should have been instantiated"
        assert mock_consumer_class.call_count == 1
        mock_consumer.subscribe.assert_called_once_with(topics)
        assert mock_consumer.subscribe.call_args[0][0] == topics

    @patch('app.kafka.consumer.Consumer')
    def test_consume_valid_message(self, mock_consumer_class):
        # Arrange
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = json.dumps({"key": "value"}).encode('utf-8')
        mock_consumer.poll.side_effect = [mock_msg, KeyboardInterrupt()]

        consumer = KafkaConsumer("test_group", ["test_topic"])
        mock_callback = MagicMock()

        # Act
        consumer.consume(mock_callback)

        # Assert
        assert mock_callback.called, "Callback should be called for valid message"
        assert mock_callback.call_count == 1
        assert mock_callback.call_args[0][0] == {"key": "value"}
        assert mock_consumer.close.called, "Consumer should be closed"

    @patch('app.kafka.consumer.Consumer')
    def test_consume_none_message(self, mock_consumer_class):
        # Arrange
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_consumer.poll.side_effect = [None, KeyboardInterrupt()]
        consumer = KafkaConsumer("test_group", ["test_topic"])
        mock_callback = MagicMock()

        # Act
        consumer.consume(mock_callback)

        # Assert
        assert not mock_callback.called, "Callback should not be called when message is None"

    @patch('app.kafka.consumer.logger')
    @patch('app.kafka.consumer.Consumer')
    def test_consume_eof_error(self, mock_consumer_class, mock_logger):
        # Arrange
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_msg = MagicMock()
        mock_error = MagicMock()
        mock_error.code.return_value = KafkaError._PARTITION_EOF
        mock_msg.error.return_value = mock_error
        mock_msg.topic.return_value = "test_topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        mock_consumer.poll.side_effect = [mock_msg, KeyboardInterrupt()]

        consumer = KafkaConsumer("test_group", ["test_topic"])
        mock_callback = MagicMock()

        # Act
        consumer.consume(mock_callback)

        # Assert
        assert not mock_callback.called, "Callback should not be called for EOF error"
        mock_logger.info.assert_any_call("test_topic [0] reached end at offset 100")

    @patch('app.kafka.consumer.logger')
    @patch('app.kafka.consumer.Consumer')
    def test_consume_general_error(self, mock_consumer_class, mock_logger):
        # Arrange
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_msg = MagicMock()
        mock_error = MagicMock()
        mock_error.code.return_value = KafkaError._ALL_BROKERS_DOWN
        mock_msg.error.return_value = mock_error
        mock_consumer.poll.side_effect = [mock_msg, KeyboardInterrupt()]

        consumer = KafkaConsumer("test_group", ["test_topic"])
        mock_callback = MagicMock()

        # Act
        consumer.consume(mock_callback)

        # Assert
        assert not mock_callback.called, "Callback should not be called for general error"
        assert mock_logger.error.called, "Logger should log error"
        assert str(mock_error) in mock_logger.error.call_args[0][0]


class TestKafkaProducer:

    @patch('app.kafka.producer.Producer')
    def test_producer_init(self, mock_producer_class):
        # Act
        producer = KafkaProducer()

        # Assert
        assert mock_producer_class.called, "Producer class should have been instantiated"

    @patch('app.kafka.producer.Producer')
    def test_produce_message_success(self, mock_producer_class):
        # Arrange
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        producer = KafkaProducer()

        # Act
        producer.produce_message("test_topic", "test_key", {"data": "value"})

        # Assert
        assert mock_producer.produce.called, "Producer produce method should be called"
        args, kwargs = mock_producer.produce.call_args
        assert args[0] == "test_topic"
        assert kwargs['key'] == b'test_key'
        assert kwargs['value'] == json.dumps({"data": "value"}).encode('utf-8')
        assert kwargs['callback'] == producer.delivery_report
        assert mock_producer.poll.called
        assert mock_producer.poll.call_args[0][0] == 0

    @patch('app.kafka.producer.Producer')
    def test_produce_message_buffer_error(self, mock_producer_class):
        # Arrange
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        mock_producer.produce.side_effect = [BufferError(), None]
        producer = KafkaProducer()

        # Act
        producer.produce_message("test_topic", "test_key", {"data": "value"})

        # Assert
        assert mock_producer.produce.call_count == 2, "Produce should retry on BufferError"
        assert mock_producer.poll.call_args_list[0][0][0] == 0.5, "Should wait 0.5 on BufferError"
        assert mock_producer.poll.call_args_list[1][0][0] == 0, "Should poll 0 at end"

    @patch('app.kafka.producer.Producer')
    def test_produce_message_general_exception(self, mock_producer_class):
        # Arrange
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        mock_producer.produce.side_effect = Exception("Test Exception")
        producer = KafkaProducer()

        # Act
        producer.produce_message("test_topic", "test_key", {"data": "value"})

        # Assert
        assert mock_producer.produce.call_count == 1, "Produce should not retry on general exception"
        assert mock_producer.poll.called
        assert mock_producer.poll.call_args[0][0] == 0

    @patch('app.kafka.producer.logger')
    @patch('app.kafka.producer.Producer')
    def test_delivery_report_success(self, mock_producer_class, mock_logger):
        # Arrange
        producer = KafkaProducer()
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test_topic"
        mock_msg.partition.return_value = 0

        # Act
        producer.delivery_report(None, mock_msg)

        # Assert
        assert mock_logger.info.called, "Should log success info"
        assert mock_logger.info.call_args[0][0] == 'Message delivered to test_topic [0]'

    @patch('app.kafka.producer.logger')
    @patch('app.kafka.producer.Producer')
    def test_delivery_report_error(self, mock_producer_class, mock_logger):
        # Arrange
        producer = KafkaProducer()

        # Act
        producer.delivery_report("Some Error", None)

        # Assert
        assert mock_logger.error.called, "Should log error"
        assert mock_logger.error.call_args[0][0] == 'Message delivery failed: Some Error'

    @patch('app.kafka.producer.Producer')
    def test_flush(self, mock_producer_class):
        # Arrange
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        producer = KafkaProducer()

        # Act
        producer.flush()

        # Assert
        assert mock_producer.flush.called, "Producer flush method should be called"
