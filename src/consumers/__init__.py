"""Kafka consumers package."""
from src.consumers.document_consumer import document_consumer
from src.consumers.deletion_consumer import deletion_consumer
from src.consumers.index_creation_consumer import index_creation_consumer

__all__ = [
    'document_consumer',
    'deletion_consumer',
    'index_creation_consumer'
]
