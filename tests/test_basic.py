import unittest
from app.config.settings import settings

class TestBasicSettings(unittest.TestCase):
    def test_settings_loaded(self):
        """Test that settings are loaded correctly."""
        self.assertIsNotNone(settings.kafka_broker_url)
        self.assertEqual(settings.environment, "development")

if __name__ == "__main__":
    unittest.main()
