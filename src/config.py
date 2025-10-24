"""Configuration management using Pydantic Settings."""
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "documents"
    kafka_group_id: str = "opensearch_adapter_group"
    
    # OpenSearch Configuration
    opensearch_host: str = "opensearch"
    opensearch_port: int = 9200
    opensearch_user: str = "admin"
    opensearch_password: str = "our_password"
    opensearch_use_ssl: bool = True
    opensearch_verify_certs: bool = False
    
    # Embedding Service Configuration
    embedding_service_url: str = "http://embedding_server:8000"
    embedding_model: str = "google/embeddinggemma-300m"
    embedding_dimension: int = 256
    embedding_batch_size: int = 10
    
    # Chunking Configuration
    chunk_size_words: int = 1400
    chunk_overlap_words: int = 150
    
    # System Topics
    system_topics: str = "topic_1,topic_2,topic_3,topic_4,topic_5"
    
    # Logging
    log_level: str = "INFO"
    
    @property
    def system_topics_list(self) -> List[str]:
        """Get system topics as a list."""
        return [topic.strip() for topic in self.system_topics.split(",")]
    
    @property
    def opensearch_url(self) -> str:
        """Get full OpenSearch URL."""
        protocol = "https" if self.opensearch_use_ssl else "http"
        return f"{protocol}://{self.opensearch_host}:{self.opensearch_port}"


# Global settings instance
settings = Settings()
