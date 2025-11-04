"""OpenSearch client for index management and document operations."""
from typing import List, Dict, Any, Optional
from datetime import datetime
from opensearchpy import OpenSearch, helpers
from src.config import settings
from src.logger import logger
from src.models import ChunkWithEmbedding


class OpenSearchClient:
    """
    Async client for OpenSearch operations.
    Handles index creation, document indexing, and queries.
    """
    
    def __init__(self):
        """Initialize OpenSearch client."""
        self.client = None
        self.embedding_dimension = settings.embedding_dimension
        
    def connect(self) -> bool:
        """
        Connect to OpenSearch cluster.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.client = OpenSearch(
                hosts=[{
                    'host': settings.opensearch_host,
                    'port': settings.opensearch_port
                }],
                http_auth=(settings.opensearch_user, settings.opensearch_password),
                use_ssl=settings.opensearch_use_ssl,
                verify_certs=settings.opensearch_verify_certs,
                ssl_show_warn=False
            )
            
            # Test connection
            info = self.client.info()
            logger.info(f"Connected to OpenSearch cluster: {info['version']['number']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to OpenSearch: {e}")
            return False
    
    def close(self):
        """Close OpenSearch connection."""
        if self.client:
            self.client.close()
            logger.info("OpenSearch connection closed")
    
    def _get_index_name(self, topic_type: str, topic_name: str, user_id: Optional[str] = None) -> str:
        """
        Get index name based on topic type.
        
        Args:
            topic_type: "system" or "user"
            topic_name: Name of the topic
            user_id: User ID for user topics
            
        Returns:
            Index name
        """
        if topic_type == "system":
            return f"system_{topic_name}"
        else:
            return f"user_{user_id}_topic"
    
    def create_index(
        self,
        index_name: str,
        embedding_dimension: int = None
    ) -> bool:
        """
        Create an OpenSearch index with BM25 and HNSW configuration.
        
        Args:
            index_name: Name of the index to create
            embedding_dimension: Dimension of embedding vectors
            
        Returns:
            True if index created or already exists, False otherwise
        """
        embedding_dimension = embedding_dimension or self.embedding_dimension
        
        # Check if index already exists
        try:
            exists = self.client.indices.exists(index=index_name)
            if exists:
                logger.info(f"Index {index_name} already exists")
                return True
        except Exception as e:
            logger.error(f"Error checking index existence: {e}")
        
        # Base properties
        properties = {
            "text": {
                "type": "text",
                "analyzer": "standard"
            },
            "embedding": {
                "type": "knn_vector",
                "dimension": embedding_dimension,
                "method": {
                    "name": "hnsw",
                    "space_type": "cosinesimil",
                    "parameters": {
                        "m": 48,
                        "ef_construction": 256
                    }
                }
            },
            "doc_id": {
                "type": "keyword"
            },
            "chunk_id": {
                "type": "integer"
            },
            "source_type": {
                "type": "keyword"
            },
            "document_url": {
                "type": "keyword"
            },
            "upload_time":{
                "type":"date"
            },
            "metadata": {
                "type": "object",
                "enabled": True
            }
        }
        
        # Add user-specific fields for user indices
        if not index_name.startswith("system_"):
            properties["user_id"] = {"type": "keyword"}
        
        # Index configuration
        index_body = {
            "settings": {
                "index": {
                    "knn": True,
                    "knn.algo_param.ef_search": 100
                }
            },
            "mappings": {
                "properties": properties
            }
        }
        
        try:
            self.client.indices.create(index=index_name, body=index_body)
            logger.info(f"Created index: {index_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create index {index_name}: {e}")
            return False
    
    def ensure_index_exists(
        self,
        topic_type: str,
        topic_name: str,
        user_id: Optional[str] = None
    ) -> str:
        """
        Ensure index exists, create if it doesn't.
        
        Args:
            topic_type: "system" or "user"
            topic_name: Name of the topic
            user_id: User ID for user topics
            
        Returns:
            Index name
        """
        index_name = self._get_index_name(topic_type, topic_name, user_id)
        
        # Create index if it doesn't exist
        self.create_index(index_name)
        
        return index_name
    
    def index_chunks(
        self,
        chunks: List[ChunkWithEmbedding],
        index_name: str
    ) -> bool:
        """
        Index chunks into OpenSearch using bulk API.
        
        Args:
            chunks: List of chunks with embeddings
            index_name: Target index name
            
        Returns:
            True if indexing successful, False otherwise
        """
        if not chunks:
            logger.warning("No chunks to index")
            return True
        
        # Prepare bulk actions
        is_system = index_name.startswith("system_")
        actions = []
        for chunk in chunks:
            # Base source
            source = {
                "text": chunk.text,
                "embedding": chunk.embedding,
                "doc_id": chunk.doc_id,
                "chunk_id": chunk.chunk_id,
                "source_type": chunk.source_type,
                "document_url": chunk.document_url,
                "metadata": chunk.metadata
            }
            
            if is_system:
                source["upload_time"] = chunk.user_upload_time.isoformat() if chunk.user_upload_time else None
            else:
                source["user_id"] = chunk.user_id
                source["user_upload_time"] = chunk.user_upload_time.isoformat() if chunk.user_upload_time else None
            
            action = {
                "_index": index_name,
                "_id": f"{chunk.doc_id}_{chunk.chunk_id}",
                "_source": source
            }
            actions.append(action)
        
        try:
            # Bulk index with synchronous helper
            success, failed = helpers.bulk(
                self.client,
                actions,
                raise_on_error=False,
                raise_on_exception=False
            )
            
            if failed:
                logger.error(f"Failed to index {len(failed)} chunks in {index_name}")
                for item in failed:
                    logger.error(f"Failed item: {item}")
            
            logger.info(f"Successfully indexed {success} chunks into {index_name}")
            return success > 0
            
        except Exception as e:
            logger.error(f"Error during bulk indexing: {e}")
            return False
    
    def delete_documents_by_doc_id(self, index_name: str, doc_id: str) -> bool:
        """
        Delete all documents with the specified doc_id from an index.
        
        Args:
            index_name: Name of the index to delete from
            doc_id: Document ID to delete
            
        Returns:
            True if deletion successful, False otherwise
        """
        try:
            # Check if index exists
            if not self.client.indices.exists(index=index_name):
                logger.warning(f"Index {index_name} does not exist, cannot delete doc_id {doc_id}")
                return False
            
            # Delete all documents with the specified doc_id
            query = {
                "query": {
                    "term": {
                        "doc_id": doc_id
                    }
                }
            }
            
            response = self.client.delete_by_query(
                index=index_name,
                body=query,
                refresh=True
            )
            
            deleted_count = response.get('deleted', 0)
            
            if deleted_count > 0:
                logger.info(f"Deleted {deleted_count} documents with doc_id {doc_id} from index {index_name}")
                return True
            else:
                logger.warning(f"No documents found with doc_id {doc_id} in index {index_name}")
                return True  # Not an error, just no documents to delete
                
        except Exception as e:
            logger.error(f"Error deleting documents with doc_id {doc_id} from {index_name}: {e}")
            return False
    
    def create_system_indices(self) -> bool:
        """
        Create all system topic indices.
        
        Returns:
            True if all indices created successfully, False otherwise
        """
        system_topics = settings.system_topics_list
        all_success = True
        
        for topic_name in system_topics:
            index_name = f"system_{topic_name}"
            success = self.create_index(index_name)
            if not success:
                all_success = False
                logger.error(f"Failed to create system index: {index_name}")
            else:
                logger.info(f"System index ready: {index_name}")
        
        return all_success


# Global OpenSearch client instance
opensearch_client = OpenSearchClient()
