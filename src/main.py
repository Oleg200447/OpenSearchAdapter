"""Main FastAPI application and entry point."""
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.config import settings
from src.logger import logger
from src.embedding_client import embedding_client
from src.opensearch_client import opensearch_client
from src.kafka_consumer import document_processor


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    """
    # Startup
    logger.info("Starting OpenSearch Adapter service...")
    
    # Connect to OpenSearch
    logger.info("Connecting to OpenSearch...")
    opensearch_connected = await opensearch_client.connect()
    if not opensearch_connected:
        logger.error("Failed to connect to OpenSearch. Exiting...")
        raise Exception("OpenSearch connection failed")
    
    # Test embedding service connection
    logger.info("Testing embedding service connection...")
    embedding_connected = await embedding_client.test_connection()
    if not embedding_connected:
        logger.error("Failed to connect to embedding service. Exiting...")
        raise Exception("Embedding service connection failed")
    
    # Create system indices
    logger.info("Creating system topic indices...")
    system_indices_created = await opensearch_client.create_system_indices()
    if not system_indices_created:
        logger.warning("Some system indices failed to create, but continuing...")
    
    # Start Kafka consumer in background
    logger.info("Starting Kafka consumer...")
    consumer_task = asyncio.create_task(document_processor.start())
    
    logger.info("OpenSearch Adapter service started successfully!")
    
    yield
    
    # Shutdown
    logger.info("Shutting down OpenSearch Adapter service...")
    
    # Stop Kafka consumer
    await document_processor.stop()
    
    # Wait for consumer task to complete
    try:
        await asyncio.wait_for(consumer_task, timeout=10.0)
    except asyncio.TimeoutError:
        logger.warning("Kafka consumer did not stop gracefully")
        consumer_task.cancel()
    
    # Close OpenSearch connection
    await opensearch_client.close()
    
    logger.info("OpenSearch Adapter service stopped")


# Create FastAPI app
app = FastAPI(
    title="OpenSearch Adapter",
    description="Service for processing documents from Kafka and indexing into OpenSearch",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "opensearch-adapter",
        "version": "1.0.0"
    }


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "OpenSearch Adapter",
        "status": "running",
        "description": "Processing documents from Kafka and indexing into OpenSearch"
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        log_level=settings.log_level.lower()
    )
