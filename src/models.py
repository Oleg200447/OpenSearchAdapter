from typing import Optional, Dict, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class KafkaMessage(BaseModel):
    doc_id: str = Field(..., description="Unique document identifier")
    doc_name: str = Field(..., description="Document name")
    user_id: Optional[str] = Field(None, description="User ID, null for system topics")
    topic_name: str = Field(..., description="Topic name starting with 'user' or 'system'")
    source_type: Literal["pdf", "pptx", "docx"] = Field(..., description="Document source type")
    #document_url: str = Field(..., description="URL to document in MinIO")
    upload_time: str = Field(..., description="Upload timestamp from upstream service")
    text: str = Field(..., min_length=1, description="Full document text")
    
    # @field_validator("topic_name")
    # @classmethod
    # def validate_topic_name(cls, v: str) -> str:
    #     if not v or not v.strip():
    #         raise ValueError("topic_name cannot be empty")
    #     v = v.strip()
    #     if not v.startswith("user") and not v.startswith("system"):
    #         raise ValueError("topic_name must start with 'user' or 'system'")
    #     return v
    
    # @field_validator("user_id")
    # @classmethod
    # def validate_user_id(cls, v: Optional[str], info) -> Optional[str]:
    #     topic_name = info.data.get("topic_name", "")
    #     if topic_name.startswith("user") and not v:
    #         raise ValueError("user_id is required for user topics")
    #     if topic_name.startswith("system") and v:
    #         raise ValueError("user_id must be null for system topics")
    #     return v
    
    # def get_topic_type(self) -> Literal["user", "system"]:
    #     """Infer topic type from topic_name prefix."""
    #     return "user" if self.topic_name.startswith("user") else "system"


class Document(BaseModel): #need doc_name TODO
    text: str = Field(..., description="Full document text content")
    text_hash: str = Field(..., description="SHA256 hash of the text")
    doc_id: str = Field(..., description="Document ID")
    doc_name: str = Field(..., description="Document name")
    user_id: Optional[str] = Field(None, description="User ID if applicable")
    source_type: str = Field(..., description="Document source type")
    #document_url: Optional[str] = Field(None, description="URL or path to original document")
    user_upload_time: Optional[datetime] = Field(None, description="Upload timestamp")
    #metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class DocumentWithEmbedding(Document):
    embedding: list[float] = Field(..., description="Embedding vector")


class DeleteDocumentMessage(BaseModel):
    """Message for deleting documents by doc_id from an index."""
    doc_id: str = Field(..., description="Document ID to delete")
    topic_name: str = Field(..., description="Name of the index to delete from")
    upload_time: str = Field(..., description="Request timestamp")
    
    @field_validator("doc_id", "topic_name")
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Field cannot be empty")
        return v.strip()


class CreateIndexMessage(BaseModel):
    """Message for creating a new user index."""
    user_id: str = Field(..., description="topic creator")
    topic_name: str = Field(..., description="Name of the topic/index to create")
    upload_time: str = Field(..., description="Request timestamp")
    
    @field_validator("topic_name")
    @classmethod
    def validate_topic_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("topic_name cannot be empty")
        return v.strip()


class DeleteIndexMessage(BaseModel):
    """Message for deleting an index."""
    user_id: str = Field(..., description="User ID who owns the index")
    topic_name: str = Field(..., description="Name of the topic/index to delete")
    upload_time: str = Field(..., description="Request timestamp")

    @field_validator("topic_name")
    @classmethod
    def validate_topic_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("topic_name cannot be empty")
        return v.strip()


class DocumentStatusKafkaMessage(BaseModel):
    """Kafka message for document status update."""
    doc_id: str
