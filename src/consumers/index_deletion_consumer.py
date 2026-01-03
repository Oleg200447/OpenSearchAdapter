"""Kafka consumer for processing index deletion messages."""
import json
import asyncio
from typing import Optional
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from src.config import settings
from src.logger import logger
from src.models import DeleteIndexMessage
from src.opensearch_client import opensearch_client


class IndexDeletionConsumer:
    """
    Processes index deletion requests from Kafka.
    Deletes user indices with the specified topic name.
    """
    
    def __init__(self):
        """Initialize index deletion consumer."""
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
        
    async def start(self):
        """Start Kafka consumer and begin processing index deletion messages."""
        try:
            # Initialize Kafka consumer
            self.consumer = AIOKafkaConsumer(
                settings.kafka_index_deletion_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=settings.kafka_index_deletion_group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            await self.consumer.start()
            logger.info(f"Index deletion consumer started. Topic: {settings.kafka_index_deletion_topic}, "
                       f"Group: {settings.kafka_index_deletion_group_id}")
            
            self.running = True
            
            # Process messages
            async for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    await self.process_message(message.value)
                except Exception as e:
                    logger.error(f"Error processing index deletion message: {e}", extra={
                        "message_offset": message.offset,
                        "partition": message.partition
                    })
                    
        except Exception as e:
            logger.error(f"Error in index deletion consumer: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop Kafka consumer."""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("Index deletion consumer stopped")
    
    async def process_message(self, message_data: dict):
        """
        Process a single index deletion message.
        
        Args:
            message_data: Raw message data from Kafka
        """
        # Validate message
        try:
            delete_msg = DeleteIndexMessage(**message_data)
        except ValidationError as e:
            logger.error(f"Index deletion message validation failed: {e.errors()}")
            return
        
        logger.info(f"Processing index deletion request for topic {delete_msg.topic_name}", extra={
            "user_id": delete_msg.user_id, 
            "topic_name": delete_msg.topic_name,
            "request_time": delete_msg.upload_time
        })
        
        try:
            # The topic_name comes already formatted (e.g., "user_123_custom_topic")
            # Delete the index asynchronously using thread pool
            success = await asyncio.to_thread(
                opensearch_client.delete_index,
                index_name=delete_msg.topic_name
            )
            
            if success:
                logger.info(f"Successfully deleted index {delete_msg.topic_name}", extra={
                    "index_name": delete_msg.topic_name,
                    "request_time": delete_msg.upload_time
                })
            else:
                logger.error(f"Failed to delete index {delete_msg.topic_name}")
                
        except Exception as e:
            logger.error(f"Error deleting index {delete_msg.topic_name}: {e}", extra={
                "topic_name": delete_msg.topic_name,
                "error": str(e)
            })


# Global consumer instance
index_deletion_consumer = IndexDeletionConsumer()
