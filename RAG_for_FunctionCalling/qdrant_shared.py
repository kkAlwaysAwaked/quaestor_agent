import atexit
from pathlib import Path

from qdrant_client import QdrantClient

DB_PATH = Path(__file__).resolve().parents[1] / "qdrant_db"
COLLECTION_NAME = "hybrid_collection"

client = QdrantClient(path=str(DB_PATH))


def _close_client() -> None:
    try:
        client.close()
    except Exception:
        pass


atexit.register(_close_client)
