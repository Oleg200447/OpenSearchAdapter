from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "documents"
    kafka_group_id: str = "opensearch_adapter_group"
    opensearch_host: str = "opensearch"
    opensearch_port: int = 9200
    opensearch_user: str = "admin"
    opensearch_password: str = "Our password"
    opensearch_use_ssl: bool = True
    opensearch_verify_certs: bool = False
    embedding_service_url: str = "http://embedding_server:8000"
    embedding_model: str = "google/embeddinggemma-300m"
    embedding_dimension: int = 256
    embedding_batch_size: int = 10
    chunk_size_words: int = 1400
    chunk_overlap_words: int = 150
    system_topics: str = "topic_1,topic_2,topic_3,topic_4,topic_5"
    log_level: str = "INFO"
    
    @property
    def system_topics_list(self) -> List[str]:
        return [topic.strip() for topic in self.system_topics.split(",")]

    @property
    def opensearch_url(self) -> str:
        protocol = "https" if self.opensearch_use_ssl else "http"
        return f"{protocol}://{self.opensearch_host}:{self.opensearch_port}"


# Global settings instance
settings = Settings()
