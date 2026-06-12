import pytest
from unittest.mock import patch, MagicMock

from app.Test import FakeKafka, App

def test_fake_kafka_send():
    # Arrange
    kafka = FakeKafka()
    mock_callback = MagicMock()
    
    # Act
    kafka.send(mock_callback)
    
    # Assert
    assert mock_callback.called, "Callback should be invoked"
    assert mock_callback.call_count == 1
    mock_callback.assert_called_once_with(None, "Message Delivered")

def test_app_delivery_report_success(capsys):
    # Arrange
    app_instance = App()
    
    # Act
    app_instance.delivery_report(None, "Test Message")
    
    # Assert
    captured = capsys.readouterr()
    assert "Success :" in captured.out
    assert "Test Message" in captured.out

def test_app_delivery_report_error(capsys):
    # Arrange
    app_instance = App()
    
    # Act
    app_instance.delivery_report("Some Error", None)
    
    # Assert
    captured = capsys.readouterr()
    assert "Error :" in captured.out
    assert "Some Error" in captured.out

@patch('app.Test.FakeKafka')
def test_app_run(mock_fake_kafka_class):
    # Arrange
    mock_kafka = MagicMock()
    mock_fake_kafka_class.return_value = mock_kafka
    
    app_instance = App()
    
    # Act
    app_instance.run()
    
    # Assert
    assert mock_fake_kafka_class.called, "FakeKafka should be instantiated"
    assert mock_kafka.send.called, "send method should be called on FakeKafka"
    mock_kafka.send.assert_called_once_with(app_instance.delivery_report)
