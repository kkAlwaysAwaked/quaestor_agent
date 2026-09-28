"""Create and validate the Qdrant collection without importing embedding models."""

from __future__ import annotations

import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from core.config import InfrastructureSettings
from infra import topology as t


# 作用：向 Qdrant 发送带 API Key 的请求，并按需允许资源不存在。
def _request(
    settings: InfrastructureSettings,
    method: str,
    path: str,
    payload: dict | None = None,
    *,
    allow_missing: bool = False,
) -> dict | None:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        settings.qdrant_url.rstrip("/") + path,
        data=data,
        method=method,
        headers={
            "api-key": settings.qdrant_api_key,
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=10) as response:
            return json.load(response)
    except HTTPError as exc:
        if allow_missing and exc.code == 404:
            return None
        raise


# 作用：检查 Qdrant 的就绪探针是否返回成功状态。
def check_ready(settings: InfrastructureSettings) -> None:
    request = Request(
        settings.qdrant_url.rstrip("/") + "/readyz",
        headers={"api-key": settings.qdrant_api_key},
    )
    with urlopen(request, timeout=5) as response:
        if response.status != 200:
            raise RuntimeError(f"Qdrant readiness returned HTTP {response.status}")


# 作用：读取目标集合的信息，集合不存在时返回空值。
def _collection(settings: InfrastructureSettings) -> dict | None:
    response = _request(
        settings,
        "GET",
        f"/collections/{t.COLLECTION}",
        allow_missing=True,
    )
    return response["result"] if response is not None else None


# 作用：校验现有集合的稠密向量、稀疏向量及载荷索引定义。
def _check_schema(collection: dict, *, require_indexes: bool) -> None:
    params = collection["config"]["params"]
    vectors = params["vectors"]
    dense = vectors.get(t.DENSE_VECTOR) if isinstance(vectors, dict) else None
    sparse = params.get("sparse_vectors") or {}
    if (
        not isinstance(dense, dict)
        or dense.get("size") != t.DENSE_SIZE
        or str(dense.get("distance", "")).lower() != t.DENSE_DISTANCE.lower()
        or t.SPARSE_VECTOR not in sparse
    ):
        raise RuntimeError(
            f"{t.COLLECTION} vector schema differs from infra/topology.py; "
            "inspect it before changing existing data"
        )
    if require_indexes:
        indexes = collection.get("payload_schema") or {}
        missing = [field for field in t.FILTER_FIELDS if field not in indexes]
        if missing:
            raise RuntimeError(f"{t.COLLECTION} is missing payload indexes: {missing}")
        wrong_type = [
            field
            for field in t.FILTER_FIELDS
            if indexes[field].get("data_type") != "keyword"
        ]
        if wrong_type:
            raise RuntimeError(f"{t.COLLECTION} has non-keyword payload indexes: {wrong_type}")


# 作用：按预期结构幂等创建 Qdrant 集合与载荷索引。
def bootstrap_qdrant(settings: InfrastructureSettings) -> None:
    check_ready(settings)
    collection = _collection(settings)
    if collection is None:
        _request(
            settings,
            "PUT",
            f"/collections/{t.COLLECTION}",
            {
                "vectors": {
                    t.DENSE_VECTOR: {
                        "size": t.DENSE_SIZE,
                        "distance": t.DENSE_DISTANCE,
                    }
                },
                "sparse_vectors": {t.SPARSE_VECTOR: {}},
            },
        )
        collection = _collection(settings)
    if collection is None:
        raise RuntimeError(f"Qdrant did not create {t.COLLECTION}")
    _check_schema(collection, require_indexes=False)
    existing_indexes = collection.get("payload_schema") or {}
    for field in t.FILTER_FIELDS:
        if field not in existing_indexes:
            _request(
                settings,
                "PUT",
                f"/collections/{t.COLLECTION}/index?wait=true",
                {"field_name": field, "field_schema": "keyword"},
            )
    check_qdrant(settings)


# 作用：确认 Qdrant 已就绪且目标集合及索引符合预期。
def check_qdrant(settings: InfrastructureSettings) -> None:
    check_ready(settings)
    collection = _collection(settings)
    if collection is None:
        raise RuntimeError(f"Qdrant collection {t.COLLECTION} does not exist")
    _check_schema(collection, require_indexes=True)
