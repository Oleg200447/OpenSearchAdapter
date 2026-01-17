"""Kafka producer utility for sending messages."""
import json
from typing import Optional
from aiokafka import AIOKafkaProducer
from src.config import settings
from src.logger import logger


class KafkaProducer:
    """
    Async Kafka producer for sending messages.
    """

    def __init__(self):
        """Initialize Kafka producer."""
        self.producer: Optional[AIOKafkaProducer] = None

    async def start(self):
        """Start the Kafka producer."""
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda m: json.dumps(m).encode('utf-8')
            )
            await self.producer.start()
            logger.info("Kafka producer started")

    async def stop(self):
        """Stop the Kafka producer."""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")

    async def send_message(self, topic: str, message: dict):
        """
        Send a message to a Kafka topic.

        Args:
            topic: Kafka topic name
            message: Message to send (will be JSON serialized)
        """
        if not self.producer:
            await self.start()

        try:
            await self.producer.send(topic, message)
            logger.info(f"Message sent to topic {topic}: {message}")
        except Exception as e:
            logger.error(f"Failed to send message to topic {topic}: {e}")
            raise


# Global producer instance
kafka_producer = KafkaProducer()
