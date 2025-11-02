import asyncio
from typing import List
import httpx
from src.config import settings
from src.logger import logger


class EmbeddingClient:
    def __init__(self):
        self.base_url = settings.embedding_service_url
        self.model = settings.embedding_model
        self.batch_size = settings.embedding_batch_size
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
    async def get_embeddings(
        self,
        texts: List[str],
        batch_size: int = None
    ) -> List[List[float]]:
        """
        Get embeddings for a list of texts.
        Processes in batches for efficiency.
        
        Args:
            texts: List of text strings to embed
            batch_size: Batch size for processing (default from config)
            
        Returns:
            List of embedding vectors
            
        Raises:
            Exception: If embedding generation fails after retries
        """
        if not texts:
            logger.warning("Empty text list provided for embedding")
            return []
        
        batch_size = batch_size or self.batch_size
        all_embeddings = []
        
        # Process in batches
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_embeddings = await self._get_batch_embeddings(batch)
            all_embeddings.extend(batch_embeddings)
            
            logger.info(f"Generated embeddings for batch {i // batch_size + 1}, "
                       f"texts: {len(batch)}")
        
        return all_embeddings
    
    async def _get_batch_embeddings(
        self,
        texts: List[str]
    ) -> List[List[float]]:
        """
        Get embeddings for a batch of texts with retry logic.
        
        Args:
            texts: List of text strings
            
        Returns:
            List of embedding vectors
            
        Raises:
            Exception: If all retries fail
        """
        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.post(
                        f"{self.base_url}/v1/embeddings",
                        json={
                            "model": self.model,
                            "input": texts
                        }
                    )
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    # Extract embeddings from response
                    # OpenAI format: {"data": [{"embedding": [...], "index": 0}, ...]}
                    embeddings = [
                        item["embedding"]
                        for item in sorted(data["data"], key=lambda x: x["index"])
                    ]
                    
                    return embeddings
                    
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error getting embeddings (attempt {attempt + 1}): "
                           f"{e.response.status_code} - {e.response.text}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    raise
                    
            except httpx.RequestError as e:
                logger.error(f"Request error getting embeddings (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    raise
                    
            except (KeyError, ValueError) as e:
                logger.error(f"Error parsing embedding response: {e}")
                raise
        
        raise Exception("Failed to get embeddings after all retries")
    
    async def test_connection(self) -> bool:
        """
        Test connection to embedding service.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            embeddings = await self.get_embeddings(["test"])
            if embeddings and len(embeddings) == 1:
                logger.info(f"Embedding service connection successful. "
                          f"Vector dimension: {len(embeddings[0])}")
                return True
            return False
        except Exception as e:
            logger.error(f"Embedding service connection failed: {e}")
            return False


# Global embedding client instance
embedding_client = EmbeddingClient()
