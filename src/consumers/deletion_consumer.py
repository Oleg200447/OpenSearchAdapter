"""Kafka consumer for processing document deletion messages."""
import json
import asyncio
from typing import Optional
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from src.config import settings
from src.logger import logger
from src.models import DeleteDocumentMessage
from src.opensearch_client import opensearch_client


class DeletionConsumer:
    """
    Processes document deletion requests from Kafka.
    Deletes all documents with the specified doc_id from a given index.
    """
    
    def __init__(self):
        """Initialize deletion consumer."""
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
        
    async def start(self):
        """Start Kafka consumer and begin processing deletion messages."""
        try:
            # Initialize Kafka consumer
            self.consumer = AIOKafkaConsumer(
                settings.kafka_delete_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=settings.kafka_delete_group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            await self.consumer.start()
            logger.info(f"Deletion consumer started. Topic: {settings.kafka_delete_topic}, "
                       f"Group: {settings.kafka_delete_group_id}")
            
            self.running = True
            
            # Process messages
            async for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    await self.process_message(message.value)
                except Exception as e:
                    logger.error(f"Error processing deletion message: {e}", extra={
                        "message_offset": message.offset,
                        "partition": message.partition
                    })
                    
        except Exception as e:
            logger.error(f"Error in deletion consumer: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop Kafka consumer."""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("Deletion consumer stopped")
    
    async def process_message(self, message_data: dict):
        """
        Process a single deletion message.
        
        Args:
            message_data: Raw message data from Kafka
        """
        # Validate message
        try:
            delete_msg = DeleteDocumentMessage(**message_data)
        except ValidationError as e:
            logger.error(f"Deletion message validation failed: {e.errors()}")
            return
        
        logger.info(f"Processing deletion request for doc_id {delete_msg.doc_id}", extra={
            "doc_id": delete_msg.doc_id,
            "topic_name": delete_msg.topic_name,
            "upload_time": delete_msg.upload_time
        })
        
        try:
            # Delete documents asynchronously using thread pool
            success = await asyncio.to_thread(
                opensearch_client.delete_documents_by_doc_id,
                index_name=delete_msg.topic_name,
                doc_id=delete_msg.doc_id
            )
            
            if success:
                logger.info(f"Successfully processed deletion request for doc_id {delete_msg.doc_id}", extra={
                    "doc_id": delete_msg.doc_id,
                    "index_name": delete_msg.topic_name
                })
            else:
                logger.error(f"Failed to delete doc_id {delete_msg.doc_id} from {delete_msg.topic_name}")
                
        except Exception as e:
            logger.error(f"Error deleting doc_id {delete_msg.doc_id}: {e}", extra={
                "doc_id": delete_msg.doc_id,
                "index_name": delete_msg.topic_name,
                "error": str(e)
            })


# Global consumer instance
deletion_consumer = DeletionConsumer()
