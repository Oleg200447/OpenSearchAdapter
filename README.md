# OpenSearch Adapter Service

Асинхронный Python сервис для обработки документов из Kafka и индексации в OpenSearch с использованием векторных embeddings.

## Возможности

- ✅ **Асинхронная обработка**: FastAPI + aiokafka для максимальной производительности
- ✅ **Chunking текста**: Интеллектуальное разбиение на чанки с overlap для сохранения контекста
- ✅ **Векторные embeddings**: Интеграция с vLLM embedding сервером (google/embeddinggemma-300m)
- ✅ **OpenSearch индексация**: BM25 + HNSW для гибридного поиска
- ✅ **Валидация данных**: Pydantic модели для проверки входящих сообщений
- ✅ **Структурированное логирование**: JSON логи для легкого парсинга
- ✅ **Docker поддержка**: Готовый Dockerfile и docker-compose

## Архитектура

```
Kafka → Validation → Chunking → Embeddings → OpenSearch
  ↓         ↓           ↓            ↓            ↓
JSON     Pydantic    1400 слов    vLLM API   BM25+HNSW
```

## Структура проекта

```
OpenSearchAdapter/
├── src/
│   ├── main.py              # FastAPI приложение
│   ├── config.py            # Конфигурация
│   ├── models.py            # Pydantic модели
│   ├── kafka_consumer.py    # Kafka consumer
│   ├── chunker.py           # Текстовый chunking
│   ├── embedding_client.py  # Клиент для embeddings
│   ├── opensearch_client.py # OpenSearch клиент
│   └── logger.py            # Логирование
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Требования

- Docker & Docker Compose
- Kafka (в сети test_network)
- OpenSearch (в сети test_network)
- vLLM Embedding Server (в сети test_network)

## Установка и запуск

### 1. Клонируйте репозиторий

```bash
cd OpenSearchAdapter
```

### 2. Настройте окружение

Скопируйте `.env.example` в `.env` и настройте переменные при необходимости:

```bash
cp .env.example .env
```

### 3. Убедитесь, что сеть test_network существует

```bash
docker network create test_network
```

### 4. Запустите сервис

```bash
docker-compose up -d --build
```

**Примечание:** Все системные индексы создаются автоматически при старте сервиса. Не требуется запускать дополнительные скрипты инициализации.

### 5. Проверьте статус

```bash
# Проверьте логи
docker logs -f opensearch_adapter

# Health check
curl http://localhost:8005/health
```

## Быстрый старт

### Предварительные требования

Перед запуском убедитесь, что в сети `test_network` запущены:
- **Kafka** (kafka:9092)
- **OpenSearch** (opensearch:9200)
- **vLLM Embedding Server** (embedding_server:8000)

### Проверка работы

После запуска контейнера проверьте логи:

```bash
docker logs opensearch_adapter
```

Вы должны увидеть:
- ✅ Connected to OpenSearch cluster
- ✅ Embedding service connection successful
- ✅ All system indices initialized successfully (системные индексы создаются автоматически)
- ✅ Kafka consumer started

### Отправка тестового сообщения

Используйте тестовый скрипт:

```bash
pip install kafka-python
python test_kafka_producer.py
```

### Проверка индексации

```bash
# Просмотр логов обработки
docker logs opensearch_adapter | grep "Successfully processed"

# Проверка индексов
curl -k -u admin:our_password https://localhost:9200/_cat/indices?v

# Поиск в индексе пользователя
curl -k -u admin:our_password https://localhost:9200/user_user_123_topic/_search?pretty
```

### Остановка сервиса

```bash
# Остановить контейнер
docker-compose down

# Остановить и удалить данные
docker-compose down -v
```

## Конфигурация

Основные параметры в `.env`:

### Kafka
- `KAFKA_BOOTSTRAP_SERVERS`: Адрес Kafka (default: kafka:9092)
- `KAFKA_TOPIC`: Топик для чтения сообщений (default: documents)
- `KAFKA_GROUP_ID`: Consumer group ID

### OpenSearch
- `OPENSEARCH_HOST`: Хост OpenSearch (default: opensearch)
- `OPENSEARCH_PORT`: Порт (default: 9200)
- `OPENSEARCH_USER`: Пользователь (default: admin)
- `OPENSEARCH_PASSWORD`: Пароль (default: our_password)

### Embedding Service
- `EMBEDDING_SERVICE_URL`: URL vLLM сервера (default: http://embedding_server:8000)
- `EMBEDDING_MODEL`: Модель для embeddings (default: google/embeddinggemma-300m)
- `EMBEDDING_DIMENSION`: Размерность векторов (default: 256)

### Chunking
- `CHUNK_SIZE_WORDS`: Размер чанка в словах (default: 1400)
- `CHUNK_OVERLAP_WORDS`: Overlap между чанками (default: 150)

### System Topics
- `SYSTEM_TOPICS`: Список системных топиков через запятую (default: topic_1,topic_2,topic_3,topic_4,topic_5)

## Формат входящих сообщений

Сервис ожидает JSON сообщения из Kafka в следующем формате:

```json
{
  "doc_id": "1234-abcd",
  "user_id": "user_5678",
  "topic_type": "user",
  "topic_name": "topic_1",
  "source_type": "pdf",
  "text": "Полный текст документа здесь...",
  "metadata": {
    "filename": "document.pdf",
    "page_count": 12,
    "created_at": "2025-10-22T12:00:00Z"
  }
}
```

### Поля:

- `doc_id` (string, required): Уникальный идентификатор документа
- `user_id` (string, nullable): ID пользователя (null для системных топиков)
- `topic_type` (string, required): "system" или "user"
- `topic_name` (string, required): Имя топика
- `source_type` (string, required): "pdf", "docx", "html", или "plain_text"
- `text` (string, required): Полный текст документа
- `metadata` (object, optional): Дополнительные метаданные

## Индексы OpenSearch

### Системные индексы

Создаются **автоматически при старте сервиса** (не требуется запуск дополнительных скриптов):
- `system_topic_1`
- `system_topic_2`
- `system_topic_3`
- `system_topic_4`
- `system_topic_5`

### Пользовательские индексы

Создаются **динамически при первом документе пользователя**:
- `user_{user_id}_topic`

### Структура индекса

Каждый индекс содержит:
- `text` (text): Текст чанка для BM25 поиска
- `embedding` (knn_vector): Вектор embeddings для HNSW
- `doc_id` (keyword): ID родительского документа
- `chunk_id` (integer): Номер чанка
- `user_id` (keyword): ID пользователя
- `source_type` (keyword): Тип источника
- `document_url` (keyword): URL/путь к документу
- `user_upload_time` (date): Время загрузки
- `metadata` (object): Дополнительные метаданные

## Workflow обработки

1. **Получение сообщения** из Kafka
2. **Валидация** с помощью Pydantic
3. **Chunking** текста (1400 слов с overlap 150 слов)
4. **Генерация embeddings** через vLLM API (batch по 10 чанков)
5. **Проверка/создание** индекса в OpenSearch
6. **Bulk индексация** чанков с метаданными

## Логирование

Сервис использует структурированное JSON логирование:

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

## Мониторинг

### Health Check

```bash
curl http://localhost:8005/health
```

Ответ:
```json
{
  "status": "healthy",
  "service": "opensearch-adapter",
  "version": "1.0.0"
}
```

### Просмотр логов

```bash
# Все логи
docker logs opensearch_adapter

# Следить за логами
docker logs -f opensearch_adapter

# Последние 100 строк
docker logs --tail 100 opensearch_adapter
```

## Troubleshooting

### Сервис не может подключиться к Kafka

Проверьте, что Kafka запущен и доступен в сети test_network:

```bash
docker network inspect test_network
```

### Сервис не может подключиться к OpenSearch

Убедитесь, что OpenSearch запущен и учетные данные верны:

```bash
curl -k -u admin:our_password https://localhost:9200
```

### Ошибки embedding сервера

Проверьте, что vLLM сервер запущен:

```bash
curl http://localhost:8004/v1/models
```

### Просмотр индексов в OpenSearch

```bash
curl -k -u admin:our_password https://localhost:9200/_cat/indices?v
```

## Разработка

### Локальный запуск без Docker

```bash
# Создайте виртуальное окружение
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Установите зависимости
pip install -r requirements.txt

# Настройте .env файл
cp .env.example .env

# Запустите сервис
python -m uvicorn src.main:app --reload
```

## Производительность

- **Chunking**: ~1400 слов на чанк, оптимизирован для контекста 2048 токенов
- **Batch embeddings**: По 10 чанков за раз для баланса скорости и памяти
- **Bulk indexing**: Все чанки документа индексируются одним запросом
- **Async I/O**: Все операции полностью асинхронные

## Лицензия

MIT

## Поддержка

Для вопросов и проблем создайте issue в репозитории.
