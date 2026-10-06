"""Shared names for the infrastructure topology."""

# 名称定义统一放在topology模块中

# rabbitmq.py负责把这些名称落实为RabbitMQ的实际资源
JOBS_EXCHANGE = "app.jobs"
DEAD_EXCHANGE = "app.dead"
CHAT_QUEUE = "chat.requests"
INGEST_QUEUE = "ingest.jobs"
CHAT_DEAD_QUEUE = "chat.requests.dead"
CHAT_STREAM_ROUTE = "chat.events"
INGEST_DEAD_QUEUE = "ingest.jobs.dead"

COLLECTION = "hybrid_collection"
DENSE_VECTOR = "dense_vector"
SPARSE_VECTOR = "sparse_vector"
DENSE_SIZE = 384  # BAAI/bge-small-en-v1.5 used by the existing ingester
DENSE_DISTANCE = "Cosine"
FILTER_FIELDS = ("user_id", "document_id", "version_id")
