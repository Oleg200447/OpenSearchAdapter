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
from src.models import KafkaMessage, Document, DocumentWithEmbedding, DocumentStatusKafkaMessage
from src.embedding_client import embedding_client, ContextLengthError
from src.opensearch_client import opensearch_client
from src.kafka_producer import kafka_producer


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
    
    async def get_embedding_with_adaptive_length(self, text: str, doc_id: str) -> Optional[list]:
        """
        Get embedding for text with adaptive word count reduction.
        Starts with 4500 words and reduces by 500 on context length errors until 500 words.
        
        Args:
            text: Full document text
            doc_id: Document ID for logging
            
        Returns:
            Embedding vector or None if all attempts fail
        """
        word_counts = [4500, 4000, 3500, 3000, 2500, 2000, 1500, 1000, 500]
        
        for word_count in word_counts:
            try:
                text_for_embedding = get_first_n_words(text, word_count)
                actual_words = len(text_for_embedding.split())
                
                logger.info(f"Attempting to get embedding with {word_count} words (actual: {actual_words}) for document {doc_id}")
                
                embeddings = await embedding_client.get_embeddings([text_for_embedding])
                
                if embeddings and len(embeddings) == 1:
                    logger.info(f"Successfully got embedding with {actual_words} words for document {doc_id}")
                    return embeddings[0]
                else:
                    logger.warning(f"Empty embedding result for document {doc_id} with {actual_words} words")
                    
            except ContextLengthError as e:
                logger.warning(f"Context length exceeded with {word_count} words for document {doc_id}, trying with fewer words")
                if word_count == 500:
                    logger.error(f"Failed to get embedding even with minimum 500 words for document {doc_id}: {e}")
                    return None
                continue
            except Exception as e:
                logger.error(f"Error getting embedding for document {doc_id} with {word_count} words: {e}")
                return None
        
        return None
        
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
            "doc_name": kafka_msg.doc_name,
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
                        "doc_name": kafka_msg.doc_name,
                        "index_name": index_name,
                        "topic_name": kafka_msg.topic_name
                    }
                )
                return
            
            # Step 1: Calculate hash of full text
            text_hash = calculate_text_hash(kafka_msg.text)
            
            logger.info(f"Calculated text hash for document {kafka_msg.doc_id}: {text_hash[:16]}...")
            
            # Step 2: Get embedding with adaptive word count (4500 -> 500 with -500 step)
            embedding = await self.get_embedding_with_adaptive_length(kafka_msg.text, kafka_msg.doc_id)
            
            if not embedding:
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
                embedding=embedding,
                doc_id=kafka_msg.doc_id,
                doc_name = kafka_msg.doc_name,
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
                    "doc_name": kafka_msg.doc_name,
                    "text_length": len(kafka_msg.text),
                    "text_hash": text_hash,
                    "index_name": index_name
                })

                # Send document status update to Kafka
                status_message = DocumentStatusKafkaMessage(doc_id=kafka_msg.doc_id)
                try:
                    await kafka_producer.send_message("document_status_updates", status_message.model_dump())
                except Exception as e:
                    logger.error(f"Failed to send document status update for {kafka_msg.doc_id}: {e}")

            else:
                logger.error(f"Failed to index document {kafka_msg.doc_id}")
                
        except Exception as e:
            logger.error(f"Error processing document {kafka_msg.doc_id}: {e}", extra={
                "doc_id": kafka_msg.doc_id,
                "error": str(e)
            })



    #     async for message in self.consumer:
    # try:
    #     await self.process_message(message.value)
    #     await self.consumer.commit()
    # except RetryableError:
    #     logger.warning("Retryable error, not committing offset")
    #     await send_to_retry_topic(message)
    # except FatalError:
    #     logger.error("Fatal error, committing offset")
    #     await self.consumer.commit()


# Global consumer instance
document_consumer = DocumentConsumer()
