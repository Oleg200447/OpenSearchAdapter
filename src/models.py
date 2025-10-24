"""Pydantic models for data validation."""
from typing import Optional, Dict, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class DocumentMetadata(BaseModel):
    """Metadata about the document."""
    filename: Optional[str] = None
    page_count: Optional[int] = None
    created_at: Optional[str] = None
    
    class Config:
        extra = "allow"  # Allow additional fields


class KafkaMessage(BaseModel):
    """Schema for incoming Kafka messages."""
    doc_id: str = Field(..., description="Unique document identifier")
    user_id: Optional[str] = Field(None, description="User ID, null for system topics")
    topic_type: Literal["system", "user"] = Field(..., description="Type of topic")
    topic_name: str = Field(..., description="Name of the topic for system topics")
    source_type: Literal["pdf", "docx", "html", "plain_text"] = Field(..., description="Document source type")
    text: str = Field(..., min_length=1, description="Full document text")
    metadata: DocumentMetadata = Field(default_factory=DocumentMetadata, description="Additional metadata")
    
    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v: Optional[str], info) -> Optional[str]:
        """Validate user_id based on topic_type."""
        topic_type = info.data.get("topic_type")
        if topic_type == "user" and not v:
            raise ValueError("user_id is required for user topic_type")
        if topic_type == "system" and v:
            raise ValueError("user_id must be null for system topic_type")
        return v
    
    @field_validator("topic_name")
    @classmethod
    def validate_topic_name(cls, v: str) -> str:
        """Validate topic name."""
        if not v or not v.strip():
            raise ValueError("topic_name cannot be empty")
        return v.strip()


class TextChunk(BaseModel):
    """Represents a text chunk with metadata."""
    chunk_id: int = Field(..., description="Chunk sequence number")
    text: str = Field(..., description="Chunk text content")
    doc_id: str = Field(..., description="Parent document ID")
    user_id: Optional[str] = Field(None, description="User ID if applicable")
    source_type: str = Field(..., description="Document source type")
    document_url: Optional[str] = Field(None, description="URL or path to original document")
    user_upload_time: Optional[datetime] = Field(None, description="Upload timestamp")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class ChunkWithEmbedding(TextChunk):
    """Text chunk with embedding vector."""
    embedding: list[float] = Field(..., description="Embedding vector")
