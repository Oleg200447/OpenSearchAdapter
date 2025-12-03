"""Kafka consumer for processing document messages."""
import json
import asyncio
import hashlib
import re
from typing import Optional
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from src.config import settings
from src.logger import logger
from src.models import KafkaMessage, Document, DocumentWithEmbedding
from src.embedding_client import embedding_client
from src.opensearch_client import opensearch_client


def get_first_n_words(text: str, n: int = 5000) -> str:
    """
    Extract first N words from text.
    
    Args:
        text: Input text
        n: Number of words to extract
        
    Returns:
        String containing first N words
    """
    words = re.findall(r'\S+', text)
    return ' '.join(words[:n])


def calculate_text_hash(text: str) -> str:
    """
    Calculate SHA256 hash of text.
    
    Args:
        text: Input text
        
    Returns:
        Hexadecimal hash string
    """
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


class DocumentConsumer:
    """
    Processes documents from Kafka and indexes them into OpenSearch.
    Indexes must already exist - this consumer does not create them.
    """
    
    def __init__(self):
        """Initialize document consumer."""
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
        
    async def start(self):
        """Start Kafka consumer and begin processing messages."""
        try:
            # Initialize Kafka consumer
            self.consumer = AIOKafkaConsumer(
                settings.kafka_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=settings.kafka_group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            await self.consumer.start()
            logger.info(f"Document consumer started. Topic: {settings.kafka_topic}, "
                       f"Group: {settings.kafka_group_id}")
            
            self.running = True
            
            # Process messages
            async for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    await self.process_message(message.value)
                except Exception as e:
                    logger.error(f"Error processing message: {e}", extra={
                        "message_offset": message.offset,
                        "partition": message.partition
                    })
                    
        except Exception as e:
            logger.error(f"Error in document consumer: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop Kafka consumer."""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("Document consumer stopped")
    
    async def process_message(self, message_data: dict):
        """
        Process a single Kafka message.
        
        Args:
            message_data: Raw message data from Kafka
        """
        # Validate message
        try:
            kafka_msg = KafkaMessage(**message_data)
        except ValidationError as e:
            logger.error(f"Message validation failed: {e.errors()}")
            return
        
        logger.info(f"Processing document {kafka_msg.doc_id}", extra={
            "doc_id": kafka_msg.doc_id,
            "topic_name": kafka_msg.topic_name,
            "source_type": kafka_msg.source_type
        })
        
        try:
            # Get expected index name
            index_name = kafka_msg.topic_name
         
            # Check if index exists
            index_exists = await asyncio.to_thread(
                opensearch_client.client.indices.exists,
                index=index_name
            )
            
            if not index_exists:
                logger.error(
                    f"Index {index_name} does not exist. Cannot process document {kafka_msg.doc_id}. "
                    f"Index must be created first via index creation topic.",
                    extra={
                        "doc_id": kafka_msg.doc_id,
                        "index_name": index_name,
                        "topic_name": kafka_msg.topic_name
                    }
                )
                return
            
            # Step 1: Extract first 5000 words for embedding
            text_for_embedding = get_first_n_words(kafka_msg.text, 5000)
            
            logger.info(f"Extracted {len(text_for_embedding.split())} words for embedding from document {kafka_msg.doc_id}")
            
            # Step 2: Calculate hash of full text
            text_hash = calculate_text_hash(kafka_msg.text)
            
            logger.info(f"Calculated text hash for document {kafka_msg.doc_id}: {text_hash[:16]}...")
            
            # Step 3: Get embedding for first 5000 words
            embeddings = await embedding_client.get_embeddings([text_for_embedding])
            
            if not embeddings or len(embeddings) != 1:
                logger.error(f"Failed to get embedding for document {kafka_msg.doc_id}")
                return
            
            # Step 4: Parse upload time
            upload_time = None
            try:
                upload_time = datetime.fromisoformat(kafka_msg.upload_time.replace('Z', '+00:00'))
            except Exception as e:
                logger.warning(f"Failed to parse upload time: {e}")
            
            # Step 5: Create document with full text + embedding + hash
            document = DocumentWithEmbedding(
                text=kafka_msg.text,  # Full text
                text_hash=text_hash,
                embedding=embeddings[0],
                doc_id=kafka_msg.doc_id,
                user_id=kafka_msg.user_id,
                source_type=kafka_msg.source_type,
                user_upload_time=upload_time
            )
            
            # Step 6: Index document into OpenSearch
            success = await asyncio.to_thread(
                opensearch_client.index_documents,
                documents=[document],
                index_name=index_name
            )
            
            if success:
                logger.info(f"Successfully processed document {kafka_msg.doc_id}", extra={
                    "doc_id": kafka_msg.doc_id,
                    "text_length": len(kafka_msg.text),
                    "embedding_words": len(text_for_embedding.split()),
                    "text_hash": text_hash,
                    "index_name": index_name
                })
            else:
                logger.error(f"Failed to index document {kafka_msg.doc_id}")
                
        except Exception as e:
            logger.error(f"Error processing document {kafka_msg.doc_id}: {e}", extra={
                "doc_id": kafka_msg.doc_id,
                "error": str(e)
            })


# Global consumer instance
document_consumer = DocumentConsumer()
