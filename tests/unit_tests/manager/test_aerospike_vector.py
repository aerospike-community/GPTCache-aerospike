import importlib
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest

from test_aerospike_storage import Record, Row, model, no_network_or_installs, sdk
from gptcache.manager.vector_data.base import VectorData


@pytest.fixture
def vector(model):
    storage, session, module = model
    backend = importlib.import_module("gptcache.manager.vector_data.aerospike")
    storage._sdk.Vector = lambda values: SimpleNamespace(numpy_value=np.asarray(values).copy())
    result = backend.AerospikeVectorStore(scalar_storage=storage, dimension=3)
    return result, storage, session, module


def test_factory_lazily_forwards_only_aerospike_options(monkeypatch):
    from gptcache.manager.vector_data.manager import VectorBase
    backend = importlib.import_module("gptcache.manager.vector_data.aerospike")
    constructor = Mock(return_value=object())
    monkeypatch.setattr(backend, "AerospikeVectorStore", constructor)
    params = dict(namespace="test", cache_scope="chat", dimension=3,
                  cluster_definition=object(), top_k=4)
    assert VectorBase.get("aerospike", **params) is constructor.return_value
    constructor.assert_called_once_with(**params)


@pytest.mark.parametrize("embedding", [
    np.array([1, 2]), np.ones((1, 3)), np.array([1, np.nan, 2]),
    np.array([1, np.inf, 2]), np.array([1e300, 2, 3]), ["a", "b", "c"],
])
def test_all_vectors_validated_before_any_write(vector, embedding):
    backend, storage, session, module = vector
    with pytest.raises(ValueError):
        backend.mul_add([VectorData(1, np.ones(3)), VectorData(2, embedding)])
    assert not session.executed


def test_duplicate_ids_rejected_before_any_write(vector):
    backend, storage, session, module = vector
    with pytest.raises(ValueError, match="Duplicate"):
        backend.mul_add([VectorData(1, np.ones(3)), VectorData(1, np.zeros(3))])
    assert not session.executed


@pytest.mark.parametrize("top_k", [-2, True, 2.5, "3", 2**63])
def test_invalid_top_k_rejected_without_database_work(vector, top_k):
    backend, storage, session, module = vector
    with pytest.raises(ValueError):
        backend.search(np.ones(3), top_k=top_k)
    assert not session.executed


@pytest.mark.parametrize("metric", ["L2", "cosine", "inner_product"])
@pytest.mark.parametrize("limit", [5, 1001])
def test_search_uses_exact_projection_fallback_above_native_limit(vector, monkeypatch, metric, limit):
    backend, storage, session, module = vector
    backend.metric = metric
    backend.top_k = limit
    monkeypatch.setattr(backend, "_schema", Mock())
    for name in ("euclidean_squared_distance", "cosine_similarity", "dot_product", "vector_bin"):
        monkeypatch.setattr(storage._sdk.Exp, name, Mock(), raising=False)
    storage._transport.ExpOperation = SimpleNamespace(read=Mock())
    storage._transport.ExpReadFlags = SimpleNamespace(DEFAULT=0)
    storage._sdk.OrderByType = SimpleNamespace(DOUBLE="double")
    storage._sdk.Order = SimpleNamespace(ASC="asc", DESC="desc")
    distances = [float(i) for i in range(1100)]
    rows = Mock()
    rows.__iter__ = Mock(return_value=iter([
        Row(backend._key(i + 1), Record(dict(
            d=d, qid=i + 1, fmt=1, dim=3, scope=storage.cache_scope)))
        for i, d in enumerate(distances)
    ]))
    builder = Mock()
    builder.with_txn.return_value = builder
    builder.where.return_value = builder
    builder.with_op_projection.return_value = builder
    builder.order_by.return_value = builder
    builder.top_k.return_value = builder
    builder.execute.return_value = rows
    monkeypatch.setattr(session, "query", Mock(return_value=builder))
    result = backend.search(np.ones(3))
    expected = sorted(((d if metric == "L2" else 1 - d if metric == "cosine" else -d, i + 1)
                       for i, d in enumerate(distances)))[:limit]
    assert result == expected
    if limit > 1000:
        builder.top_k.assert_not_called()
        builder.order_by.assert_not_called()
    else:
        builder.top_k.assert_called_once_with(limit)
    rows.close.assert_called_once()


def test_large_default_top_k_is_accepted(vector):
    backend, storage, session, module = vector
    result = type(backend)(scalar_storage=storage, dimension=3, top_k=1001)
    assert result.top_k == 1001


def test_empty_mutations_and_zero_top_k_do_not_issue_commands(vector):
    backend, storage, session, module = vector
    backend.mul_add([])
    assert backend.delete([]) is True
    assert backend.search(np.ones(3), top_k=0) == []
    assert not session.executed


def test_shared_scalar_lifecycle_is_not_owned_by_vector_store(vector):
    backend, storage, session, module = vector
    backend.close()
    assert not storage._closed
    with pytest.raises(RuntimeError):
        backend.mul_add([VectorData(1, np.ones(3))])


@pytest.mark.parametrize("kwargs", [
    dict(dimension=0), dict(dimension=True), dict(dimension=3, top_k=-2),
    dict(dimension=3, metric="invalid"),
])
def test_invalid_configuration_fails_before_connect(kwargs):
    backend = importlib.import_module("gptcache.manager.vector_data.aerospike")
    with pytest.raises((ValueError, TypeError)):
        backend.AerospikeVectorStore(**kwargs)
