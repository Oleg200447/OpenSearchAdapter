# OpenSearch Adapter Service

Asynchronous Python service for processing documents from Kafka and indexing them into OpenSearch with vector embeddings.

## Features

- ✅ **Asynchronous processing**: FastAPI + aiokafka for maximum performance
- ✅ **Text chunking**: Intelligent text splitting with overlap to preserve context
- ✅ **Vector embeddings**: Integration with vLLM embedding server (google/embeddinggemma-300m)
- ✅ **OpenSearch indexing**: BM25 + HNSW for hybrid search
- ✅ **Data validation**: Pydantic models for incoming message validation
- ✅ **Structured logging**: JSON logs for easy parsing
- ✅ **Docker support**: Ready Dockerfile and docker-compose

## Architecture

```
Kafka → Validation → Chunking → Embeddings → OpenSearch
  ↓         ↓           ↓            ↓            ↓
JSON     Pydantic    1400 words   vLLM API   BM25+HNSW
```

## Project Structure

```
OpenSearchAdapter/
├── src/
│   ├── main.py              # FastAPI application
│   ├── config.py            # Configuration
│   ├── models.py            # Pydantic models
│   ├── kafka_consumer.py    # Kafka consumer
│   ├── chunker.py           # Text chunking
│   ├── embedding_client.py  # Embeddings client
│   ├── opensearch_client.py # OpenSearch client
│   └── logger.py            # Logging
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Requirements

- Docker & Docker Compose
- Kafka (in test_network)
- OpenSearch (in test_network)
- vLLM Embedding Server (in test_network)

## Installation and Launch

### 1. Clone the repository

```bash
cd OpenSearchAdapter
```

### 2. Configure environment

Copy `.env.example` to `.env` and adjust variables if needed:

```bash
cp .env.example .env
```

### 3. Ensure test_network exists

```bash
docker network create test_network
```

### 4. Launch the service

```bash
docker-compose up -d --build
```

**Note:** All system indices are created automatically on startup. No additional initialization scripts are needed.

### 5. Check status

```bash
# Check logs
docker logs -f opensearch_adapter

# Health check
curl http://localhost:8005/health
```

## Quick Start

### Prerequisites

Before starting, ensure the following are running in the `test_network`:
- **Kafka** (kafka:9092)
- **OpenSearch** (opensearch:9200)
- **vLLM Embedding Server** (embedding_server:8000)

### Check operation

After starting the container, check the logs:

```bash
docker logs opensearch_adapter
```

You should see:
- ✅ Connected to OpenSearch cluster
- ✅ Embedding service connection successful
- ✅ All system indices initialized successfully (system indices are created automatically)
- ✅ Kafka consumer started

### Send a test message

Use the test script:

```bash
pip install kafka-python
python test_kafka_producer.py
```

### Check indexing

```bash
# View processing logs
docker logs opensearch_adapter | grep "Successfully processed"

# Check indices
curl -k -u admin:our_password https://localhost:9200/_cat/indices?v

# Search in user index
curl -k -u admin:our_password https://localhost:9200/user_user_123_topic/_search?pretty
```

### Stop the service

```bash
# Stop the container
docker-compose down

# Stop and delete data
docker-compose down -v
```

## Configuration

Primary parameters in `.env`:

### Kafka
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka address (default: kafka:9092)
- `KAFKA_TOPIC`: Topic for reading messages (default: documents)
- `KAFKA_GROUP_ID`: Consumer group ID

### OpenSearch
- `OPENSEARCH_HOST`: OpenSearch host (default: opensearch)
- `OPENSEARCH_PORT`: Port (default: 9200)
- `OPENSEARCH_USER`: User (default: admin)
- `OPENSEARCH_PASSWORD`: Password (default: our_password)

### Embedding Service
- `EMBEDDING_SERVICE_URL`: vLLM server URL (default: http://embedding_server:8000)
- `EMBEDDING_MODEL`: Embedding model (default: google/embeddinggemma-300m)
- `EMBEDDING_DIMENSION`: Vector dimension (default: 256)

### Chunking
- `CHUNK_SIZE_WORDS`: Chunk size in words (default: 1400)
- `CHUNK_OVERLAP_WORDS`: Overlap between chunks (default: 150)

### System Topics
- `SYSTEM_TOPICS`: System topics list (comma-separated) (default: topic_1,topic_2,topic_3,topic_4,topic_5)

## Incoming Message Format

The service expects JSON messages from Kafka in the following format:

```json
{
  "doc_id": "1234-abcd",
  "user_id": "user_5678",
  "topic_type": "user",
  "topic_name": "topic_1",
  "source_type": "pdf",
  "text": "Full document text here...",
  "metadata": {
    "filename": "document.pdf",
    "page_count": 12,
    "created_at": "2025-10-22T12:00:00Z"
  }
}
```

### Fields:

- `doc_id` (string, required): Unique document identifier
- `user_id` (string, nullable): User ID (null for system topics)
- `topic_type` (string, required): "system" or "user"
- `topic_name` (string, required): Topic name
- `source_type` (string, required): "pdf", "docx", "html", or "plain_text"
- `text` (string, required): Full document text
- `metadata` (object, optional): Additional metadata

## OpenSearch Indices

### System indices

Created **automatically on service startup** (no additional scripts needed):
- `system_topic_1`
- `system_topic_2`
- `system_topic_3`
- `system_topic_4`
- `system_topic_5`

### User indices

Created **dynamically on first user document**:
- `user_{user_id}_topic`

### Index structure

Each index contains:
- `text` (text): Chunk text for BM25 search
- `embedding` (knn_vector): Vector embeddings for HNSW
- `doc_id` (keyword): Parent document ID
- `chunk_id` (integer): Chunk number
- `user_id` (keyword): User ID
- `source_type` (keyword): Source type
- `document_url` (keyword): Document URL/path
- `user_upload_time` (date): Upload time
- `metadata` (object): Additional metadata

## Processing Workflow

1. **Receive message** from Kafka
2. **Validate** with Pydantic
3. **Chunk** text (1400 words with 150 word overlap)
4. **Generate embeddings** via vLLM API (batch of 10 chunks)
5. **Check/create** index in OpenSearch
6. **Bulk index** chunks with metadata

## Logging

The service uses structured JSON logging:

```json
{
  "asctime": "2025-10-24 05:00:00",
  "name": "opensearch_adapter",
  "levelname": "INFO",
  "message": "Successfully processed document 1234-abcd",
  "doc_id": "1234-abcd",
  "chunks_count": 5,
  "index_name": "user_user_5678_topic"
}
```

## Monitoring

### Health Check

```bash
curl http://localhost:8005/health
```

Response:
```json
{
  "status": "healthy",
  "service": "opensearch-adapter",
  "version": "1.0.0"
}
```

### View logs

```bash
# All logs
docker logs opensearch_adapter

# Follow logs
docker logs -f opensearch_adapter

# Last 100 lines
docker logs --tail 100 opensearch_adapter
```

## Troubleshooting

### Service cannot connect to Kafka

Check that Kafka is running and accessible in the test_network:

```bash
docker network inspect test_network
```

### Service cannot connect to OpenSearch

Ensure OpenSearch is running and credentials are correct:

```bash
curl -k -u admin:our_password https://localhost:9200
```

### Embedding service errors

Check that vLLM server is running:

```bash
curl http://localhost:8004/v1/models
```

### View indices in OpenSearch

```bash
curl -k -u admin:our_password https://localhost:9200/_cat/indices?v
```

## Development

### Local launch without Docker

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure .env file
cp .env.example .env

# Launch service
python -m uvicorn src.main:app --reload
```

## Performance

- **Chunking**: ~1400 words per chunk, optimized for 2048 token context
- **Batch embeddings**: 10 chunks at a time for speed/memory balance
- **Bulk indexing**: All chunks of a document indexed in one request
- **Async I/O**: All operations are fully asynchronous

## License

MIT

## Support

For questions and issues, create an issue in the repository.
