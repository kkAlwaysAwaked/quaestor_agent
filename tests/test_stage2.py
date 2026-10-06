"""Checks for config isolation and non-destructive Qdrant bootstrap."""

import os
import unittest
from unittest.mock import patch

from core.config import load_infrastructure_settings, load_model_settings
from infra import qdrant, topology


ENV = {
    "MYSQL_DATABASE": "test_db",
    "MYSQL_USER": "test_user",
    "MYSQL_PASSWORD": "test_password",
    "REDIS_PASSWORD": "test_password",
    "RABBITMQ_USER": "test_user",
    "RABBITMQ_PASSWORD": "test_password",
    "QDRANT_API_KEY": "test_key",
    "DEEPSEEK_API_KEY": "",
}


# 作用：构造用于测试的 Qdrant 集合结构与载荷索引信息。
def collection(size=topology.DENSE_SIZE, indexes=()):
    return {
        "config": {
            "params": {
                "vectors": {
                    topology.DENSE_VECTOR: {
                        "size": size,
                        "distance": topology.DENSE_DISTANCE,
                    }
                },
                "sparse_vectors": {topology.SPARSE_VECTOR: {}},
            }
        },
        "payload_schema": {field: {"data_type": "keyword"} for field in indexes},
    }


class Stage2Tests(unittest.TestCase):
    # 作用：为每个测试设置独立的基础设施环境变量。
    def setUp(self):
        patcher = patch.dict(os.environ, ENV)
        patcher.start()
        self.addCleanup(patcher.stop)

    # 作用：验证基础设施配置不依赖模型 API Key。
    def test_infrastructure_does_not_require_model_key(self):
        settings = load_infrastructure_settings()
        self.assertEqual(settings.mysql_database, "test_db")
        with self.assertRaisesRegex(ValueError, "DEEPSEEK_API_KEY"):
            load_model_settings()

    # 作用：验证已有向量结构不匹配时初始化过程不会修改集合。
    def test_wrong_existing_vector_schema_is_not_changed(self):
        settings = load_infrastructure_settings()
        calls = []

        # 作用：模拟读取结构不匹配的 Qdrant 集合并记录请求。
        def fake_request(_settings, method, path, payload=None, **_kwargs):
            calls.append((method, path))
            return {"result": collection(size=123)}

        with patch.object(qdrant, "check_ready"), patch.object(
            qdrant, "_request", side_effect=fake_request
        ):
            with self.assertRaisesRegex(RuntimeError, "vector schema differs"):
                qdrant.bootstrap_qdrant(settings)
        self.assertEqual([method for method, _ in calls], ["GET"])

    # 作用：验证集合与索引在重复初始化时只创建一次。
    def test_new_collection_and_indexes_are_idempotent(self):
        settings = load_infrastructure_settings()
        state = {"collection": None, "created": 0, "indexed": []}

        # 作用：模拟 Qdrant 创建集合和索引时的状态变化。
        def fake_request(_settings, method, path, payload=None, **_kwargs):
            if method == "GET":
                return (
                    {"result": state["collection"]}
                    if state["collection"] is not None
                    else None
                )
            if path.endswith(f"/collections/{topology.COLLECTION}"):
                state["created"] += 1
                state["collection"] = collection()
            elif "/index?" in path:
                state["indexed"].append(payload["field_name"])
                state["collection"]["payload_schema"][payload["field_name"]] = {
                    "data_type": "keyword"
                }
            return {"status": "ok"}

        with patch.object(qdrant, "check_ready"), patch.object(
            qdrant, "_request", side_effect=fake_request
        ):
            qdrant.bootstrap_qdrant(settings)
            qdrant.bootstrap_qdrant(settings)
        self.assertEqual(state["created"], 1)
        self.assertCountEqual(state["indexed"], topology.FILTER_FIELDS)


if __name__ == "__main__":
    unittest.main()
