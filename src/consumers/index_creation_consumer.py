"""Kafka consumer for processing index creation messages."""
import json
import asyncio
from typing import Optional
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from src.config import settings
from src.logger import logger
from src.models import CreateIndexMessage
from src.opensearch_client import opensearch_client


class IndexCreationConsumer:
    """
    Processes index creation requests from Kafka.
    Creates new user indices with the specified topic name.
    """
    
    def __init__(self):
        """Initialize index creation consumer."""
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
        
    async def start(self):
        """Start Kafka consumer and begin processing index creation messages."""
        try:
            # Initialize Kafka consumer
            self.consumer = AIOKafkaConsumer(
                settings.kafka_index_creation_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=settings.kafka_index_creation_group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            await self.consumer.start()
            logger.info(f"Index creation consumer started. Topic: {settings.kafka_index_creation_topic}, "
                       f"Group: {settings.kafka_index_creation_group_id}")
            
            self.running = True
            
            # Process messages
            async for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    await self.process_message(message.value)
                except Exception as e:
                    logger.error(f"Error processing index creation message: {e}", extra={
                        "message_offset": message.offset,
                        "partition": message.partition
                    })
                    
        except Exception as e:
            logger.error(f"Error in index creation consumer: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop Kafka consumer."""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("Index creation consumer stopped")
    
    async def process_message(self, message_data: dict):
        """
        Process a single index creation message.
        
        Args:
            message_data: Raw message data from Kafka
        """
        # Validate message
        try:
            create_msg = CreateIndexMessage(**message_data)
        except ValidationError as e:
            logger.error(f"Index creation message validation failed: {e.errors()}")
            return
        
        logger.info(f"Processing index creation request for topic {create_msg.topic_name}", extra={
            "user_id": create_msg.user_id, 
            "topic_name": create_msg.topic_name,
            "request_time": create_msg.upload_time
        })
        
        try:
            # The topic_name comes already formatted (e.g., "user_123_custom_topic")
            # Create the index asynchronously using thread pool
            success = await asyncio.to_thread(
                opensearch_client.create_index,
                index_name=create_msg.topic_name
            )
            
            if success:
                logger.info(f"Successfully created index {create_msg.topic_name}", extra={
                    "index_name": create_msg.topic_name,
                    "request_time": create_msg.upload_time
                })
            else:
                logger.error(f"Failed to create index {create_msg.topic_name}")
                
        except Exception as e:
            logger.error(f"Error creating index {create_msg.topic_name}: {e}", extra={
                "topic_name": create_msg.topic_name,
                "error": str(e)
            })


# Global consumer instance
index_creation_consumer = IndexCreationConsumer()
