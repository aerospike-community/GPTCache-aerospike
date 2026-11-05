import heapq

import numpy as np

from gptcache.manager.scalar_data.aerospike_storage import (
    AerospikeStorage, AerospikeStorageError, _id, _integer, _json,
)
from gptcache.manager.vector_data.base import VectorBase, VectorData


class AerospikeVectorStore(VectorBase):
    """Native VECTOR storage with exact, server-side scan-and-rank Top-K search.

    L2 results are squared distances, cosine results are cosine distances, and
    inner-product results are negated similarities. All are ordered closest first.
    A supplied scalar_storage shares its connection without transferring ownership.
    """

    def __init__(self, *, dimension, top_k=1, metric="L2", scalar_storage=None,
                 cluster_definition=None, namespace=None, cache_scope=None, behavior=None):
        self.dimension = _integer(dimension, 1, public=True)
        self.top_k = _integer(top_k, 1, public=True)
        if metric not in ("L2", "cosine", "inner_product"):
            raise ValueError("metric must be L2, cosine or inner_product")
        self.metric = metric
        self._closed = False
        self._owns_storage = scalar_storage is None
        if scalar_storage is not None:
            if not isinstance(scalar_storage, AerospikeStorage):
                raise TypeError("scalar_storage must be an AerospikeStorage")
            if any(value is not None for value in (
                    cluster_definition, namespace, cache_scope, behavior)):
                raise ValueError("Connection options cannot accompany scalar_storage")
            scalar_storage._ready()
            self._storage = scalar_storage
        else:
            self._storage = AerospikeStorage(
                cluster_definition=cluster_definition, namespace=namespace,
                cache_scope=cache_scope, behavior=behavior)
            try:
                self._storage._configuration()
            except BaseException:
                self._storage.close()
                raise
        self._sdk = self._storage._sdk
        self._dataset = self._sdk.DataSet.of(self._storage.namespace, "gptc_v")
        self._manifest = self._storage._dataset("root").id(_json(
            ["gptc", 1, self._storage.cache_scope, "vmeta"]))

    def _open(self):
        if self._closed:
            raise RuntimeError("Aerospike vector storage is closed")
        self._storage._open()

    def _vector(self, data):
        array = np.asarray(data)
        if array.ndim != 1 or array.size != self.dimension or array.dtype.kind not in "biuf":
            raise ValueError("Embedding must be a numeric vector of the configured dimension")
        with np.errstate(over="ignore", invalid="ignore"):
            array = np.ascontiguousarray(array, dtype=np.float32)
        if not np.isfinite(array).all():
            raise ValueError("Embedding must be finite and representable as float32")
        if self.metric == "cosine" and not np.any(array):
            raise ValueError("Cosine distance is undefined for a zero vector")
        return self._sdk.Vector(array)

    def _key(self, identity):
        return self._dataset.id(_json(["gptc", 1, self._storage.cache_scope, "v", identity]))

    def _schema(self):
        storage = self._storage
        expected = dict(fmt=1, scope=storage.cache_scope, dim=self.dimension)
        row, status = storage._point(storage._query(self._manifest), self._manifest)
        if status == "missing":
            _, status = storage._point(
                storage._write("insert", self._manifest).put(expected), self._manifest)
            if status == "ok":
                return
            row, status = storage._point(storage._query(self._manifest), self._manifest)
        if (status != "ok" or row.record is None or row.record.ttl is not None
                or row.record.bins != expected
                or type(row.record.bins.get("fmt")) is not int
                or type(row.record.bins.get("dim")) is not int):
            raise AerospikeStorageError("Vector scope has an incompatible schema or dimension")

    def _bins(self, row, identity):
        storage = self._storage
        storage._physical(row.key, self._key(identity))
        if row.record is None or row.record.ttl is not None:
            raise AerospikeStorageError("Invalid vector retention metadata")
        bins = row.record.bins
        if (set(bins) != {"fmt", "scope", "qid", "dim", "vec"}
                or _integer(bins["fmt"]) != 1 or bins["scope"] != storage.cache_scope
                or _integer(bins["qid"], 1) != identity
                or _integer(bins["dim"], 1) != self.dimension):
            raise AerospikeStorageError("Invalid vector record")
        return bins

    def mul_add(self, datas):
        self._open()
        prepared = []
        for data in datas:
            if not isinstance(data, VectorData):
                raise ValueError("Expected VectorData")
            prepared.append((_id(data.id), self._vector(data.data)))
        if len({identity for identity, _ in prepared}) != len(prepared):
            raise ValueError("Duplicate vector IDs in one batch")
        if not prepared:
            return
        storage = self._storage
        def write():
            self._schema()
            builder = None
            keys = []
            for identity, vector in prepared:
                key = self._key(identity)
                keys.append(key)
                builder = (storage._write("upsert", key) if builder is None else
                           builder.upsert(key).never_expire())
                builder = builder.put(dict(fmt=1, scope=storage.cache_scope,
                                           qid=identity, dim=self.dimension, vec=vector))
            results = storage._batch(builder, keys)
            for result in results:
                if isinstance(result, Exception):
                    raise result
                if result[1] != "ok":
                    raise AerospikeStorageError("Vector batch write rejected")
        storage._run_atomic(write)

    def search(self, data, top_k=-1):
        self._open()
        limit = _integer(top_k, -1, public=True)
        if limit == -1:
            limit = self.top_k
        vector = self._vector(data)
        if not limit:
            return []
        self._schema()
        storage = self._storage
        exp = self._sdk.Exp
        name = {"L2": "euclidean_squared_distance", "cosine": "cosine_similarity",
                "inner_product": "dot_product"}[self.metric]
        distance = getattr(exp, name)(vector, exp.vector_bin("vec"))
        transport = storage._transport
        projections = [transport.ExpOperation.read(
            field, expression, transport.ExpReadFlags.DEFAULT) for field, expression in (
                ("d", distance), ("qid", exp.int_bin("qid")),
                ("fmt", exp.int_bin("fmt")), ("dim", exp.int_bin("dim")),
                ("scope", exp.string_bin("scope")))]
        query = storage._session.query(self._dataset).with_txn(None).where(
            storage._eq("scope", storage.cache_scope)).with_op_projection(
                *projections)
        if limit <= 1000:
            query = query.order_by(
                "d", self._sdk.OrderByType.DOUBLE,
                self._sdk.Order.ASC if self.metric == "L2" else self._sdk.Order.DESC,
            ).top_k(limit)
        rows = query.execute(on_error=self._sdk.ErrorStrategy.IN_STREAM)
        result = {}
        try:
            for row in rows:
                if storage._status(row) != "ok" or row.record is None or row.record.ttl is not None:
                    raise AerospikeStorageError("Vector query failed")
                bins = row.record.bins
                if (set(bins) != {"d", "qid", "fmt", "dim", "scope"}
                        or _integer(bins["fmt"]) != 1
                        or _integer(bins["dim"], 1) != self.dimension
                        or bins["scope"] != storage.cache_scope
                        or type(bins["d"]) not in (int, float) or not np.isfinite(bins["d"])):
                    raise AerospikeStorageError("Invalid vector search projection")
                identity = _integer(bins["qid"], 1)
                storage._physical(row.key, self._key(identity))
                score = float(bins["d"])
                score = score if self.metric == "L2" else (
                    1.0 - score if self.metric == "cosine" else -score)
                if identity in result:
                    raise AerospikeStorageError("Duplicate vector search result")
                result[identity] = score
        finally:
            rows.close()
        return heapq.nsmallest(limit, ((score, identity) for identity, score in result.items()))

    def delete(self, ids):
        self._open()
        identities = list(dict.fromkeys(_id(identity) for identity in ids))
        if not identities:
            return True
        storage = self._storage
        keys = [self._key(identity) for identity in identities]
        def delete():
            self._schema()
            target = keys[0] if len(keys) == 1 else keys
            results = storage._batch(storage._query(target), keys)
            builder, present = None, []
            for identity, key, result in zip(identities, keys, results):
                if isinstance(result, Exception):
                    raise result
                row, status = result
                if status == "missing":
                    continue
                if status != "ok":
                    raise AerospikeStorageError("Vector delete read rejected")
                self._bins(row, identity)
                present.append(key)
                builder = (storage._write("update", key) if builder is None else
                           builder.update(key).never_expire())
                builder = builder.ensure_generation_is(
                    _integer(row.record.generation, 0)).with_durable_delete().delete_record()
            if present:
                for result in storage._batch(builder, present):
                    if isinstance(result, Exception):
                        raise result
                    if result[1] != "ok":
                        raise AerospikeStorageError("Vector delete rejected")
        storage._run_atomic(delete)
        return True

    def get_embeddings(self, data_id):
        self._open()
        identity = _id(data_id)
        self._schema()
        storage = self._storage
        key = self._key(identity)
        row, status = storage._point(storage._query(key), key)
        if status == "missing":
            return None
        if status != "ok":
            raise AerospikeStorageError("Vector read rejected")
        vector = self._bins(row, identity)["vec"]
        if not isinstance(vector, self._sdk.Vector):
            raise AerospikeStorageError("Embedding is not a native VECTOR")
        array = np.asarray(vector.numpy_value)
        self._vector(array)
        return array.copy()

    def update_embeddings(self, data_id, emb):
        self.mul_add([VectorData(_id(data_id), emb)])

    def rebuild(self, ids=None):
        self._open()
        return True

    def flush(self):
        self._open()

    def close(self):
        if not self._closed:
            self._closed = True
            if self._owns_storage:
                self._storage.close()
