"""Scoped scalar persistence using the optional synchronous Aerospike SDK."""

import base64
from collections import namedtuple
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import fields
from datetime import datetime, timedelta
from importlib import metadata
import hashlib
import json
import math
from numbers import Integral
from pathlib import Path
import re
import sys
import time
import random
from importlib import import_module
from functools import wraps
from typing import List
from urllib.parse import unquote, urlparse
import uuid

import numpy as np

from gptcache.manager.scalar_data.base import (
    Answer, CacheData, CacheStorage, Question, QuestionDep,
)


MAX_ID = (1 << 63) - 1
AerospikeSession = namedtuple("AerospikeSession", "id question_id session_id session_question")
SessionName = namedtuple("SessionName", "session_id")
_BINS = {
    "q": ("fmt", "scope", "qid", "state", "payload", "create_on",
          "last_access", "afmt", "adm"),
    "s": ("fmt", "scope", "sid", "qid", "sname", "squestion"),
    "r": ("fmt", "scope", "rid", "payload"),
    "root": ("fmt", "scope", "boot", "bind", "phase"),
    "qid": ("fmt", "scope", "boot", "hi"),
    "sid": ("fmt", "scope", "boot", "hi"),
    "rid": ("fmt", "scope", "boot", "hi"),
}


class AerospikeStorageError(RuntimeError):
    """Failure with non-secret operation correlation and known partial outcomes."""

    def __init__(self, message, *, outcomes=None, phase=None, identity=None, retryable=False):
        super().__init__(message)
        self.outcomes = outcomes
        self.phase = phase
        self.identity = identity
        self.retryable = retryable


def _retryable(errors):
    return bool(errors) and all(
        isinstance(error, AerospikeStorageError) and error.retryable for error in errors)


def _integer(value, low=-MAX_ID - 1, high=MAX_ID, public=False):
    valid = isinstance(value, Integral) if public else type(value) is int
    if not valid or isinstance(value, bool) or not low <= value <= high:
        raise ValueError("Expected an integer in the supported int64 range")
    return int(value)


def _id(value):
    return _integer(value, 1, public=True)


def _text(value):
    if type(value) is not str:
        raise ValueError("Expected a string")
    return value


def _json(value):
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), allow_nan=False)


def _b64(value):
    return base64.b64encode(value).decode("ascii")


def _unb64(value):
    _text(value)
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Invalid base64 payload") from exc
    if _b64(raw) != value:
        raise ValueError("Noncanonical base64 payload")
    return raw


def _node(value, ancestors=None):
    ancestors = set() if ancestors is None else ancestors
    kind = type(value)
    if value is None:
        return ["n"]
    if kind is bool:
        return ["b", value]
    if kind is int:
        return ["i", str(_integer(value))]
    if kind is float and math.isfinite(value):
        return ["f", value.hex()]
    if kind is str:
        return ["s", value]
    if kind is bytes:
        return ["y", _b64(value)]
    if kind not in (list, dict):
        raise ValueError("Unsupported native payload type")
    token = id(value)
    if token in ancestors:
        raise ValueError("Cyclic payload")
    ancestors.add(token)
    try:
        if kind is list:
            return ["l", [_node(item, ancestors) for item in value]]
        return ["d", [[_text(key), _node(item, ancestors)] for key, item in value.items()]]
    finally:
        ancestors.remove(token)


def _unnode(node):
    if type(node) is not list or not node:
        raise ValueError("Invalid tagged payload")
    tag = node[0]
    if tag == "n" and len(node) == 1:
        return None
    if len(node) != 2:
        raise ValueError("Invalid tagged payload arity")
    value = node[1]
    if tag == "b" and type(value) is bool:
        return value
    if tag == "s":
        return _text(value)
    if tag == "y":
        return _unb64(value)
    if tag == "i" and type(value) is str:
        if not re.fullmatch(r"0|-?[1-9][0-9]*", value):
            raise ValueError("Noncanonical integer payload")
        return _integer(int(value))
    if tag == "f" and type(value) is str:
        result = float.fromhex(value)
        if not math.isfinite(result) or result.hex() != value:
            raise ValueError("Noncanonical float payload")
        return result
    if tag in ("l", "d") and type(value) is list:
        if tag == "l":
            return [_unnode(item) for item in value]
        result = {}
        for pair in value:
            if type(pair) is not list or len(pair) != 2:
                raise ValueError("Invalid mapping payload")
            key = _text(pair[0])
            if key in result:
                raise ValueError("Duplicate mapping key")
            result[key] = _unnode(pair[1])
        return result
    raise ValueError("Unknown or invalid payload tag")


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON member")
        result[key] = value
    return result


def _load(payload, keys):
    if type(payload) is not bytes:
        raise ValueError("Expected payload bytes")
    obj = json.loads(payload.decode("utf-8"), object_pairs_hook=_object,
                     parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Nonfinite JSON")))
    if type(obj) is not dict or set(obj) != set(keys) or type(obj["v"]) is not int or obj["v"] != 1:
        raise ValueError("Incompatible payload shape/version")
    return obj


def _time(value):
    if type(value) is not str or not re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}", value):
        raise ValueError("Invalid naive timestamp")
    return datetime.fromisoformat(value)


def _now():
    return datetime.now().isoformat(timespec="microseconds")


def _module_owned(distribution, module, editable=False):
    filename = getattr(module, "__file__", None)
    if filename is None:
        raise AerospikeStorageError("Missing installed SDK/transport module provenance")
    filename = Path(filename).resolve()
    if any(Path(distribution.locate_file(entry)).resolve() == filename for entry in (distribution.files or ())):
        return
    direct = distribution.read_text("direct_url.json")
    if editable and direct:
        origin = json.loads(direct)
        url = urlparse(origin.get("url", ""))
        if url.scheme == "file" and origin.get("dir_info", {}).get("editable") is True:
            root = Path(unquote(url.path)).resolve()
            if root in filename.parents:
                return
    raise AerospikeStorageError("Imported module is not owned by the declared SDK/transport distribution")


def _encode_data(data):
    if not isinstance(data, CacheData):
        raise ValueError("Expected CacheData")
    question = data.question
    deps = []
    if isinstance(question, Question):
        deps = [[_text(dep.name), _text(dep.data), _integer(dep.dep_type, public=True)]
                for dep in (question.deps or [])]
        question = question.content
    question = _text(question)
    answers = [[_integer(a.answer_type, public=True), _node(a.answer)] for a in data.answers]
    embedding = None
    if data.embedding_data is not None:
        array = np.asarray(data.embedding_data)
        if array.ndim != 1 or array.dtype.kind not in "biuf":
            raise ValueError("Embedding must be a one-dimensional numeric array")
        embedding = {"dtype": "<f4", "data": _b64(np.ascontiguousarray(array, dtype="<f4").tobytes())}
    if data.session_id:
        _text(data.session_id)
    return _json(dict(v=1, question=question, answers=answers, deps=deps,
                      embedding=embedding)).encode("utf-8"), question


def _decode_data(payload):
    obj = _load(payload, ("v", "question", "answers", "deps", "embedding"))
    question = _text(obj["question"])
    if type(obj["answers"]) is not list or type(obj["deps"]) is not list:
        raise ValueError("Invalid ordered payload collections")
    answers, deps = [], []
    for row in obj["answers"]:
        if type(row) is not list or len(row) != 2:
            raise ValueError("Invalid answer")
        answers.append(Answer(_unnode(row[1]), _integer(row[0])))
    for row in obj["deps"]:
        if type(row) is not list or len(row) != 3:
            raise ValueError("Invalid dependency")
        deps.append(QuestionDep(_text(row[0]), _text(row[1]), _integer(row[2])))
    embedding = obj["embedding"]
    if embedding is not None:
        if type(embedding) is not dict or set(embedding) != {"dtype", "data"} or embedding["dtype"] != "<f4":
            raise ValueError("Invalid embedding format")
        raw = _unb64(embedding["data"])
        if len(raw) % 4:
            raise ValueError("Invalid embedding length")
        embedding = np.frombuffer(raw, dtype="<f4").copy()
    return Question(question, deps) if deps else question, answers, embedding


def _atomic(method=None, *, iterable=None):
    if method is None:
        return lambda function: _atomic(function, iterable=iterable)

    @wraps(method)
    def call(self, *args, **kwargs):
        self._ready()
        if iterable is not None:
            if args:
                args = (tuple(args[0]),) + args[1:]
            elif iterable in kwargs:
                kwargs[iterable] = tuple(kwargs[iterable])
        return self._run_atomic(lambda: method(self, *args, **kwargs))
    return call


class AerospikeStorage(CacheStorage):
    """Own one connected sync cluster and session for an explicit logical scope.

    ``behavior`` optionally supplies a genuine public SDK Behavior. When omitted,
    a child of DEFAULT supplies a fifteen-second query deadline. Explicit caller
    deadlines must be finite and positive and are never lengthened. The owned
    session is bound once to a preserving child policy.
    """

    def __init__(self, *, cluster_definition, namespace, cache_scope, behavior=None):
        if sys.version_info < (3, 11):
            raise RuntimeError(
                "Aerospike storage requires Python >=3.11; GPTCache core "
                "continues to support Python >=3.8.1."
            )
        for name, value in (("namespace", namespace), ("cache_scope", cache_scope)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError("{} must be an explicit nonblank string".format(name))
        try:
            sdk = import_module("aerospike_sdk.sync")
        except ModuleNotFoundError as exc:
            if exc.name != "aerospike_sdk":
                raise
            raise ModuleNotFoundError(
                "Aerospike storage requires the optional aerospike-sdk package. "
                "Install it separately in a Python >=3.11 environment; "
                "GPTCache does not install it at runtime.",
                name="aerospike_sdk",
            ) from exc
        if not isinstance(cluster_definition, sdk.ClusterDefinition):
            raise TypeError("cluster_definition must be a sync SDK ClusterDefinition")
        self.namespace = namespace
        self.cache_scope = cache_scope
        self._closed = False
        self._active = False
        self._behavior = None
        self._caller = behavior
        self._boot = None
        self._sc = False
        self._transaction_session = ContextVar("gptcache_aerospike_transaction", default=None)
        self._cluster = cluster_definition.connect()
        try:
            self._configure()
            self._session = self._create_session()
        except BaseException as exc:
            try:
                self.close()
            except BaseException as cleanup_error:
                raise exc from cleanup_error
            raise

    def _create_session(self):
        """Bind the validated provider child before any context can be minted."""
        return self._cluster.create_session(self._behavior)

    def _open(self):
        if self._closed:
            raise RuntimeError("Aerospike storage is closed")

    def _ready(self):
        self._open()
        if not self._active and self._boot is None:
            self.create()
        if not self._active:
            raise AerospikeStorageError("Scope requires validated ACTIVE metadata; run operator initialization first")
        self._check_behavior()

    def _configure(self):
        self._open()
        sdk = import_module("aerospike_sdk")
        policy = import_module("aerospike_sdk.policy")
        distribution = metadata.distribution("aerospike-sdk")
        _module_owned(distribution, sdk, editable=True)
        requirements = distribution.requires or []
        transports = {match.group(1).lower() for item in requirements
                      for match in [re.match(r"(aerospike-(?:async|native))(?:\s|[<>=!~@;\[]|$)", item, re.I)]
                      if match}
        if len(transports) != 1:
            raise AerospikeStorageError("SDK has no declared supported transport dependency")
        transport_name = next(iter(transports))
        transport_distribution = metadata.distribution(transport_name)
        supported = {("0.9.0a5", "aerospike-async", "0.6.0a6+gptcache.enumfix1"),
                     ("1.0.0", "aerospike-native", "1.0.0")}
        if (distribution.version, transport_name, transport_distribution.version) not in supported:
            raise AerospikeStorageError("Unsupported SDK/declared transport version pair")
        transport = import_module(transport_name.replace("-", "_"))
        _module_owned(transport_distribution, transport)
        if sdk.Exp is not transport.FilterExpression:
            raise AerospikeStorageError("SDK expression type is not from its declared transport")
        probe_key = sdk.DataSet.of(self.namespace, "gptc_meta").id(_json(["gptc", 1, self.cache_scope, "root"]))
        if not isinstance(probe_key, transport.Key):
            raise AerospikeStorageError("SDK keys are not attached to its declared transport")
        self._sdk, self._policy, self._transport = sdk, policy, transport
        if self._behavior is None:
            caller = (policy.Behavior.DEFAULT.derive_with_changes(
                "gptcache_default_" + uuid.uuid4().hex,
                reads_query=policy.Settings(total_timeout=timedelta(seconds=15)),
            ) if self._caller is None else self._caller)
            if not isinstance(caller, policy.Behavior):
                raise TypeError("behavior must be a public SDK Behavior")
            self._caller = caller
            self._behavior = caller.derive_with_changes(
                "gptcache_" + uuid.uuid4().hex,
                reads=policy.Settings(read_touch_ttl_percent=-1),
                reads_query=policy.Settings(allow_scans_with_where=True, max_retries=0),
                writes=policy.Settings(max_retries=0),
            )
        self._check_behavior()

    def _check_behavior(self):
        p = self._policy
        for mode in (p.Mode.AP, p.Mode.SC):
            for kind in (p.OpKind.READ, p.OpKind.WRITE_RETRYABLE, p.OpKind.WRITE_NON_RETRYABLE):
                for shape in (p.OpShape.POINT, p.OpShape.BATCH, p.OpShape.QUERY):
                    original = self._caller.get_settings(kind, shape, mode)
                    effective = self._behavior.get_settings(kind, shape, mode)
                    overrides = {"max_retries"} if kind != p.OpKind.READ else {"read_touch_ttl_percent"}
                    if kind == p.OpKind.READ and shape == p.OpShape.QUERY:
                        overrides |= {"allow_scans_with_where", "max_retries"}
                        if effective.allow_scans_with_where is not True or effective.max_retries != 0:
                            raise AerospikeStorageError("Unsafe effective query settings")
                    if kind == p.OpKind.READ:
                        if effective.read_touch_ttl_percent != -1:
                            raise AerospikeStorageError("Read touching must be disabled")
                    elif type(effective.max_retries) is not int or effective.max_retries != 0:
                        raise AerospikeStorageError("Modifying retries must be zero")
                    for field in fields(p.Settings):
                        if field.name not in overrides and getattr(original, field.name) != getattr(effective, field.name):
                            raise AerospikeStorageError("Provider changed unrelated caller settings")
                    if kind != p.OpKind.READ or shape != p.OpShape.QUERY:
                        continue
                    deadline = effective.total_timeout
                    if not isinstance(deadline, timedelta):
                        raise AerospikeStorageError("Configure a finite positive total deadline")
                    micros = deadline // timedelta(microseconds=1)
                    if not 1000 <= micros <= ((1 << 32) - 1) * 1000:
                        raise AerospikeStorageError("Deadline is outside unsigned32 millisecond range")
                    if micros % 1000:
                        raise AerospikeStorageError("Deadline must be exactly representable in milliseconds")

    @staticmethod
    def _pairs(body, delimiter):
        if type(body) is not str:
            raise AerospikeStorageError("Missing configuration response")
        result = {}
        for entry in body.split(delimiter):
            if not entry:
                continue
            if "=" not in entry:
                raise AerospikeStorageError("Malformed configuration entry")
            key, value = entry.split("=", 1)
            if not key or key in result:
                raise AerospikeStorageError("Ambiguous configuration entry")
            result[key] = value
        return result

    def _configuration(self):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", self.namespace):
            raise AerospikeStorageError("Namespace is not a safe info-command component")
        info = self._session.info()
        def fetch(command):
            replies = info.info_on_all_nodes(command)
            if type(replies) is not dict or not replies:
                raise AerospikeStorageError("Missing all-node configuration evidence")
            result = {}
            for node, response in replies.items():
                if not node or type(response) is not dict or set(response) != {command}:
                    raise AerospikeStorageError("Incomplete or ambiguous node configuration evidence")
                body = response[command]
                if type(body) is not str or re.match(
                        r"^(?:ERROR|FAIL)(?:$|[:;\t ])", body, re.I):
                    raise AerospikeStorageError("Configuration permission/response failure")
                result[node] = body
            return result
        nodes = fetch("node")
        if any(not value for value in nodes.values()) or len(set(nodes.values())) != len(nodes):
            raise AerospikeStorageError("Invalid active node identities")
        stats = fetch("statistics")
        ns = fetch("get-config:context=namespace;id=" + self.namespace)
        sets = fetch("sets/" + self.namespace)
        if not all(set(reply) == set(nodes) for reply in (stats, ns, sets)) or fetch("node") != nodes:
            raise AerospikeStorageError("Incomplete or changed active-node coverage")
        modes = set()
        owned_sets = {"gptc_q", "gptc_s", "gptc_r", "gptc_meta", "gptc_v"}
        for node in nodes:
            values = self._pairs(stats[node], ";")
            if values.get("cluster_size") != str(len(nodes)):
                raise AerospikeStorageError("Cannot establish complete cluster membership")
            namespace = self._pairs(ns[node], ";")
            modes.add(namespace.get("strong-consistency"))
            namespace_disabled = namespace.get("default-read-touch-ttl-pct") == "0"
            names = set()
            for entry in sets[node].split(";"):
                if not entry:
                    continue
                values = self._pairs(entry, ":")
                name = values.get("set")
                if values.get("ns") != self.namespace or not name or name in names:
                    raise AerospikeStorageError("Malformed or duplicate set configuration")
                names.add(name)
                if name in owned_sets:
                    override = values.get("default-read-touch-ttl-pct")
                    if override not in {"-1", "4294967295"} and not (
                            override == "0" and namespace_disabled):
                        raise AerospikeStorageError("Set read touching is unsafe or unverified")
            if not namespace_disabled and owned_sets - names:
                raise AerospikeStorageError("Absent sets inherit unsafe or unverified namespace read touching")
        if modes not in ({"true"}, {"false"}):
            raise AerospikeStorageError("Namespace consistency mode is unverified or inconsistent")
        self._sc = modes == {"true"}

    @contextmanager
    def _transaction(self):
        if not self._sc or self._transaction_session.get() is not None:
            yield
            return
        failure = None
        try:
            with self._session.transaction() as session:
                token = self._transaction_session.set(session)
                try:
                    yield
                except BaseException as exc:
                    failure = exc
                    status = session.abort()
                    if status not in (self._transport.AbortStatus.OK,
                                      self._transport.AbortStatus.ALREADY_ABORTED):
                        raise AerospikeStorageError("Transaction abort was not confirmed")
                    raise
                else:
                    status = session.commit()
                    if status not in (self._transport.CommitStatus.OK,
                                      self._transport.CommitStatus.ALREADY_COMMITTED):
                        raise AerospikeStorageError("Transaction commit was not confirmed")
                finally:
                    self._transaction_session.reset(token)
        except Exception as exc:
            if exc is failure:
                if isinstance(exc, AerospikeStorageError):
                    raise AerospikeStorageError(
                        "SC transaction aborted; all transactional changes were rolled back",
                        outcomes=exc.outcomes, identity=exc.identity, phase="ABORTED_TRANSACTION",
                        retryable=exc.retryable,
                    ) from exc
                if (isinstance(exc, self._sdk.AerospikeError) and not exc.in_doubt
                        and exc.result_code in (
                            self._sdk.ResultCode.MRT_BLOCKED,
                            self._sdk.ResultCode.MRT_VERSION_MISMATCH)):
                    raise AerospikeStorageError(
                        "SC transaction aborted after a transient conflict",
                        phase="ABORTED_TRANSACTION", retryable=True,
                    ) from exc
                raise
            raise AerospikeStorageError(
                "SC transaction failed; do not blindly replay an uncertain commit",
                phase="UNKNOWN_TRANSACTION",
            ) from exc

    def _run_atomic(self, operation):
        if not self._sc or self._transaction_session.get() is not None:
            with self._transaction():
                return operation()
        p = self._policy
        timeout = self._behavior.get_settings(
            p.OpKind.READ, p.OpShape.QUERY, p.Mode.SC).total_timeout.total_seconds()
        deadline = time.monotonic() + timeout
        for attempt in range(32):
            boot, active = self._boot, self._active
            try:
                with self._transaction():
                    return operation()
            except AerospikeStorageError as exc:
                if not active:
                    self._active = False
                if exc.phase == "ABORTED_TRANSACTION":
                    self._boot = boot
                if exc.phase != "ABORTED_TRANSACTION" or not exc.retryable or attempt == 31:
                    raise
                delay = random.uniform(0.5, 1.0) * min(0.005 * 2 ** min(attempt, 6), 0.25)
                if time.monotonic() + delay >= deadline:
                    raise
                time.sleep(delay)

    def _dataset(self, kind):
        return self._sdk.DataSet.of(self.namespace, "gptc_" + (kind if kind in "qsr" else "meta"))

    def _key(self, kind, identity=None):
        parts = ["gptc", 1, self.cache_scope, kind]
        if identity is not None:
            parts.append(identity)
        return self._dataset(kind).id(_json(parts))

    def _eq(self, name, value):
        exp = self._sdk.Exp
        if type(value) is str:
            return exp.eq(exp.string_bin(name), exp.string_val(value))
        return exp.eq(exp.int_bin(name), exp.int_val(value))

    def _and(self, *terms):
        if not terms:
            raise ValueError("Expected at least one predicate")
        return terms[0] if len(terms) == 1 else self._sdk.Exp.and_(list(terms))

    def _range(self, name, low, high):
        e = self._sdk.Exp
        return self._and(e.ge(e.int_bin(name), e.int_val(low)),
                         e.le(e.int_bin(name), e.int_val(high)))

    def _guard(self, kind, identity=None):
        terms = [self._eq("fmt", 1), self._eq("scope", self.cache_scope)]
        if kind == "q":
            terms += [self._eq("qid", identity), self._eq("afmt", 1),
                      self._sdk.Exp.or_([self._eq("state", 0), self._eq("state", -1)]),
                      self._range("adm", 0, MAX_ID)]
        elif kind in ("qid", "sid", "rid"):
            terms += [self._eq("boot", self._boot), self._range("hi", 0, MAX_ID)]
        elif kind in ("s", "r"):
            terms.append(self._eq(kind + "id", identity))
        return self._and(*terms)

    def _query(self, target):
        session = self._transaction_session.get()
        return (session.query(target) if session is not None else
                self._session.query(target).with_txn(None))

    def _write(self, verb, key, predicate=None):
        session = self._transaction_session.get()
        builder = getattr(session if session is not None else self._session, verb)(key)
        if session is None:
            builder = builder.with_txn(None)
        builder = builder.never_expire()
        return builder.where(predicate) if predicate is not None else builder

    @staticmethod
    def _physical(actual, expected):
        if actual is None or any(getattr(actual, name, None) != getattr(expected, name)
                                 for name in ("namespace", "set_name", "digest")):
            raise AerospikeStorageError("Physical key identity mismatch")
        if getattr(actual, "digest", None) is None:
            raise AerospikeStorageError("Missing physical digest")

    def _status(self, row):
        code = row.result_code
        exc = row.exception
        codes = self._sdk.ResultCode
        if row.in_doubt:
            raise AerospikeStorageError("Command outcome is uncertain", phase="UNKNOWN")
        if row.is_ok and exc is None and code == codes.OK:
            return "ok"
        expected = {
            codes.KEY_NOT_FOUND_ERROR: (self._sdk.RecordNotFoundError, "missing"),
            codes.FILTERED_OUT: (self._sdk.FilteredOutError, "filtered"),
            codes.KEY_EXISTS_ERROR: (self._sdk.RecordExistsError, "exists"),
        }
        if code in expected and (exc is None or isinstance(exc, expected[code][0])
                                 and exc.result_code == code and not exc.in_doubt):
            if row.record is not None:
                raise AerospikeStorageError("Conflicting failure record")
            return expected[code][1]
        if code in (codes.MRT_BLOCKED, codes.MRT_VERSION_MISMATCH) and (
                exc is None or isinstance(exc, self._sdk.AerospikeError)
                and exc.result_code == code and not exc.in_doubt):
            raise AerospikeStorageError(
                "Transaction conflict (result code {})".format(code),
                phase="TRANSIENT_CONFLICT", retryable=True)
        terminal = code in (codes.GENERATION_ERROR, codes.PARAMETER_ERROR, codes.SERVER_MEM_ERROR,
                            codes.RECORD_TOO_BIG, codes.KEY_BUSY, codes.FAIL_FORBIDDEN) and (
            exc is None or isinstance(exc, self._sdk.AerospikeError)
            and exc.result_code == code and not exc.in_doubt)
        raise AerospikeStorageError("Command failed (result code {})".format(code),
                                   phase="TERMINAL_REJECTION" if terminal else "UNKNOWN")

    def _point(self, builder, key):
        rows = list(builder.include_missing_keys().fail_on_filtered_out().execute(
            on_error=self._sdk.ErrorStrategy.IN_STREAM))
        if len(rows) != 1:
            raise AerospikeStorageError("Incomplete/duplicate point outcome")
        row = rows[0]
        self._physical(row.key, key)
        if type(row.index) is not int or row.index not in (-1, 0):
            raise AerospikeStorageError("Point result index mismatch")
        return row, self._status(row)

    def _batch(self, builder, keys):
        if len(keys) == 1:
            try:
                return [self._point(builder, keys[0])]
            except Exception as exc:
                return [exc]
        outcomes = [None] * len(keys)
        errors = []
        try:
            rows = builder.include_missing_keys().fail_on_filtered_out().execute(
                on_error=self._sdk.ErrorStrategy.IN_STREAM)
            for row in rows:
                try:
                    index = _integer(row.index, 0, len(keys) - 1)
                    self._physical(row.key, keys[index])
                    if outcomes[index] is not None:
                        raise AerospikeStorageError("Duplicate batch result")
                    try:
                        outcomes[index] = (row, self._status(row))
                    except Exception as exc:
                        outcomes[index] = exc
                except Exception as exc:
                    errors.append(exc)
        except Exception as exc:
            errors.append(exc)
        if errors:
            raise AerospikeStorageError("Incomplete batch stream/correlation", outcomes=outcomes) from errors[0]
        return [value if value is not None else AerospikeStorageError("Missing batch result")
                for value in outcomes]

    def _validate(self, kind, row, identity=None):
        record = row.record
        if record is None or type(record.bins) is not dict or record.ttl is not None:
            raise AerospikeStorageError("Missing record or invalid retention metadata")
        bins = record.bins
        if set(bins) != set(_BINS[kind]):
            raise AerospikeStorageError("Incompatible physical bin shape")
        if _integer(bins["fmt"]) != 1 or _text(bins["scope"]) != self.cache_scope:
            raise AerospikeStorageError("Incompatible record format/scope")
        if kind in ("q", "s", "r"):
            number = _integer(bins[kind + "id"], 1)
            if identity is not None and identity != number:
                raise AerospikeStorageError("Logical identity mismatch")
            identity = number
        self._physical(row.key, self._key(kind, identity))
        if kind == "q":
            if _integer(bins["state"]) not in (0, -1) or _integer(bins["afmt"]) != 1:
                raise AerospikeStorageError("Incompatible question controls")
            _integer(bins["adm"], 0)
            _time(bins["create_on"])
            _time(bins["last_access"])
            _decode_data(bins["payload"])
        elif kind == "s":
            _integer(bins["qid"], 1)
            _text(bins["sname"])
            _text(bins["squestion"])
        elif kind == "r":
            self._decode_report(bins["payload"])
        else:
            boot = _text(bins["boot"])
            if str(uuid.UUID(boot)) != boot:
                raise AerospikeStorageError("Noncanonical initialization identity")
            if kind == "root":
                if _integer(bins["phase"]) not in (0, 1) or not re.fullmatch(r"[0-9a-f]{64}", _text(bins["bind"])):
                    raise AerospikeStorageError("Invalid initialization root")
            elif bins["boot"] != self._boot:
                raise AerospikeStorageError("Allocator history ownership mismatch")
            else:
                _integer(bins["hi"], 0)
        return bins

    def _read(self, kind, identity=None):
        key = self._key(kind, identity)
        row, status = self._point(self._query(key), key)
        if status == "missing":
            if kind not in ("q", "s", "r"):
                self._active = False
                raise AerospikeStorageError("Missing initialization history; recovery required")
            return None
        if status != "ok":
            raise AerospikeStorageError("Unfiltered diagnostic was rejected")
        try:
            return row, self._validate(kind, row, identity)
        except (ValueError, AerospikeStorageError):
            if kind not in ("q", "s", "r"):
                self._active = False
            raise

    def create(self):
        """Create a fresh SC scope atomically, or validate existing scope history."""
        self._open()
        self._active = False
        self._configure()
        self._configuration()
        if self._sc:
            previous_boot = self._boot
            try:
                self._run_atomic(self._create_sc_scope)
            except AerospikeStorageError as exc:
                if exc.phase == "ABORTED_TRANSACTION":
                    self._boot = previous_boot
                raise
            return
        _, root = self._read("root")
        if root["phase"] != 1:
            raise AerospikeStorageError("Scope is INITIALIZING; only its operator may resume")
        self._boot = root["boot"]
        for kind in ("qid", "sid", "rid"):
            self._read(kind)
        self._active = True

    def _create_sc_scope(self):
        with self._transaction():
            key = self._key("root")
            row, status = self._point(self._query(key), key)
            if status == "missing":
                if self._boot is not None:
                    raise AerospikeStorageError("Missing initialization history; recovery required")
                self._operator_inventory_empty()
                vectors = self._sdk.DataSet.of(self.namespace, "gptc_v")
                rows = self._session.query(vectors).with_txn(None).where(
                    self._eq("scope", self.cache_scope)).bins(["scope"]).execute(
                        on_error=self._sdk.ErrorStrategy.IN_STREAM)
                if list(rows):
                    raise AerospikeStorageError("Initialization conflicts with existing vectors")
                for kind in ("qid", "sid", "rid"):
                    other = self._key(kind)
                    _, absent = self._point(self._query(other), other)
                    if absent != "missing":
                        raise AerospikeStorageError("Allocator exists without owned root")
                self._boot = str(uuid.uuid4())
                binding = hashlib.sha256(_json(
                    ["gptc", 1, self.namespace, self.cache_scope]).encode("utf-8")).hexdigest()
                self._insert("root", None, dict(
                    fmt=1, scope=self.cache_scope, boot=self._boot, bind=binding, phase=1))
                for kind in ("qid", "sid", "rid"):
                    self._insert(kind, None, dict(
                        fmt=1, scope=self.cache_scope, boot=self._boot, hi=0))
            elif status == "ok":
                root = self._validate("root", row)
                if root["phase"] != 1:
                    raise AerospikeStorageError("Scope is INITIALIZING; only its operator may resume")
                self._boot = root["boot"]
                for kind in ("qid", "sid", "rid"):
                    self._read(kind)
            else:
                raise AerospikeStorageError("Initialization root read failed")
        self._active = True

    def _reserve(self, kind, count):
        self._ready()
        count = _integer(count, 1)
        key = self._key(kind)
        predicate = self._and(self._guard(kind), self._range("hi", 0, MAX_ID - count))
        builder = self._write("update", key, predicate).add("hi", count).get("hi")
        row, status = self._point(builder, key)
        if status != "ok":
            _, bins = self._read(kind)
            if bins["hi"] > MAX_ID - count:
                raise AerospikeStorageError("Identifier space exhausted")
            raise AerospikeStorageError("Allocator predicate rejected; no reservation")
        high = _integer(row.operation_result(1), count)
        return list(range(high - count + 1, high + 1))

    def _insert(self, kind, identity, bins):
        key = self._key(kind, identity)
        try:
            row, status = self._point(self._write("insert", key).put(bins), key)
        except Exception as exc:
            raise AerospikeStorageError("Create-only append failed", identity=identity,
                                       phase=getattr(exc, "phase", None) or "UNKNOWN",
                                       retryable=getattr(exc, "retryable", False)) from exc
        if status != "ok":
            raise AerospikeStorageError("Create-only append rejected", identity=identity,
                                       phase="TERMINAL_REJECTION")
        return row

    @_atomic(iterable="all_data")
    def batch_insert(self, all_data: List[CacheData]):
        self._ready()
        prepared = [(_encode_data(data), data.session_id) for data in all_data]
        if not prepared:
            return []
        ids = self._reserve("qid", len(prepared))
        outcomes = [dict(position=position, qid=qid, payload="UNKNOWN", association="NOT_SENT")
                    for position, qid in enumerate(ids)]
        builder = None
        keys = []
        for qid, ((payload, _), _) in zip(ids, prepared):
            key = self._key("q", qid)
            keys.append(key)
            bins = dict(fmt=1, scope=self.cache_scope, qid=qid, state=0, payload=payload,
                        create_on=_now(), last_access=_now(), afmt=1, adm=0)
            builder = (self._write("insert", key) if builder is None else
                       builder.insert(key).never_expire())
            builder = builder.put(bins)
        try:
            results = self._batch(builder, keys)
        except Exception as exc:
            known = getattr(exc, "outcomes", None)
            if isinstance(known, list) and len(known) == len(outcomes):
                for outcome, result in zip(outcomes, known):
                    if result is not None:
                        outcome["payload"] = ("ACKNOWLEDGED" if isinstance(result, tuple) and result[1] == "ok"
                                              else result)
            raise AerospikeStorageError("Batch insert stream incomplete", outcomes=outcomes) from exc
        for outcome, result in zip(outcomes, results):
            outcome["payload"] = ("ACKNOWLEDGED" if not isinstance(result, Exception) and result[1] == "ok"
                                  else result)
        if any(outcome["payload"] != "ACKNOWLEDGED" for outcome in outcomes):
            raise AerospikeStorageError("Batch payload insert incomplete; reserved IDs are not reusable",
                                       outcomes=outcomes, retryable=_retryable([
                                           result for result in results if isinstance(result, Exception)
                                           or result[1] != "ok"]))
        for outcome, ((_, question), session_name) in zip(outcomes, prepared):
            try:
                if session_name:
                    outcome["association"] = "UNKNOWN"
                    self.add_session(outcome["qid"], session_name, question)
                    outcome["association"] = "ACKNOWLEDGED"
            except Exception as exc:
                outcome["association"] = getattr(exc, "phase", "UNKNOWN")
                identity = getattr(exc, "identity", None)
                outcome["sid"] = identity[1] if isinstance(identity, tuple) else None
                raise AerospikeStorageError("Batch insert incomplete; reserved IDs are not reusable",
                                           outcomes=outcomes, identity=outcome["qid"],
                                           retryable=getattr(exc, "retryable", False)) from exc
        return ids

    @_atomic
    def get_data_by_id(self, key):
        self._ready()
        qid = _integer(key, public=True)
        if qid <= 0:
            return None
        physical = self._key("q", qid)
        predicate = self._and(self._guard("q", qid), self._eq("state", 0))
        builder = self._write("update", physical, predicate)
        for name in _BINS["q"][:7]:
            builder = builder.get(name)
        row, status = self._point(builder.put({"last_access": _now()}), physical)
        if status == "missing":
            return None
        if status == "filtered":
            diagnostic = self._read("q", qid)
            if diagnostic is None or diagnostic[1]["state"] == -1:
                return None
            raise AerospikeStorageError("Active read predicate rejected; touch is not replayed")
        captured = {name: row.operation_result(index) for index, name in enumerate(_BINS["q"][:7])}
        if row.record is None or row.record.ttl is not None:
            raise AerospikeStorageError("Invalid touch retention metadata")
        if (_integer(captured["fmt"]) != 1 or _text(captured["scope"]) != self.cache_scope
                or _integer(captured["qid"], 1) != qid or _integer(captured["state"]) != 0):
            raise AerospikeStorageError("Invalid acknowledged touch identity")
        question, answers, embedding = _decode_data(captured["payload"])
        created, accessed = _time(captured["create_on"]), _time(captured["last_access"])
        sessions = [SessionName(item.session_id) for item in self.list_sessions(None, qid)]
        return CacheData(question, answers, embedding, sessions, created, accessed)

    def _scan(self, kind, *predicates):
        rows = self._session.query(self._dataset(kind)).with_txn(None).where(self._and(
            self._eq("scope", self.cache_scope), *predicates)).bins(list(_BINS[kind])).execute(
                on_error=self._sdk.ErrorStrategy.IN_STREAM)
        seen = {}
        errors = []
        try:
            for row in rows:
                try:
                    if self._status(row) != "ok":
                        raise AerospikeStorageError("Query row rejected")
                    bins = self._validate(kind, row)
                    identity = bins[kind + "id"]
                    if identity not in seen:
                        seen[identity] = (row, bins)
                    elif kind != "q" and bins != seen[identity][1]:
                        raise AerospikeStorageError("Immutable query duplicate changed")
                except Exception as exc:
                    errors.append(exc)
        except Exception as exc:
            errors.append(exc)
        finally:
            if hasattr(rows, "close"):
                rows.close()
        if errors:
            raise AerospikeStorageError("Query incomplete or corrupt; no complete result",
                                       outcomes=errors, retryable=_retryable(errors))
        if self._transaction_session.get() is not None:
            keys = [self._key(kind, identity) for identity in seen]
            if not keys:
                return []
            results = self._batch(self._query(keys[0] if len(keys) == 1 else keys), keys)
            current = []
            for identity, result in zip(seen, results):
                if isinstance(result, Exception):
                    raise result
                row, status = result
                if status == "ok":
                    current.append((row, self._validate(kind, row, identity)))
                elif status != "missing":
                    raise AerospikeStorageError("Transactional enumeration read rejected")
            return current
        return list(seen.values())

    def get_ids(self, deleted=True):
        self._ready()
        state = -1 if deleted else 0
        if self._sc:
            return list(self._question_states(state))
        return [bins["qid"] for _, bins in self._scan("q") if bins["state"] == state]

    def count(self, state: int = 0, is_all: bool = False):
        self._ready()
        if not is_all:
            state = _integer(state, public=True)
        if self._sc:
            return len(self._question_states(None if is_all else state))
        return sum(1 for _, bins in self._scan("q") if is_all or bins["state"] == state)

    def _question_states(self, state=None):
        predicates = [self._eq("scope", self.cache_scope)]
        if state is not None:
            predicates.append(self._eq("state", state))
        rows = self._session.query(self._dataset("q")).with_txn(None).where(
            self._and(*predicates)).bins(["fmt", "scope", "qid", "state"]).execute(
                on_error=self._sdk.ErrorStrategy.IN_STREAM)
        result = {}
        try:
            for row in rows:
                if self._status(row) != "ok" or row.record is None or row.record.ttl is not None:
                    raise AerospikeStorageError("Invalid question enumeration")
                bins = row.record.bins
                if (set(bins) != {"fmt", "scope", "qid", "state"}
                        or _integer(bins["fmt"]) != 1 or bins["scope"] != self.cache_scope
                        or _integer(bins["state"]) not in (0, -1)):
                    raise AerospikeStorageError("Invalid question controls")
                qid = _integer(bins["qid"], 1)
                self._physical(row.key, self._key("q", qid))
                result.setdefault(qid, bins["state"])
        finally:
            if hasattr(rows, "close"):
                rows.close()
        return result

    @_atomic(iterable="keys")
    def mark_deleted(self, keys):
        self._ready()
        qids = list(dict.fromkeys(_id(key) for key in keys))
        if not qids:
            return
        builder = None
        physical = []
        for qid in qids:
            key = self._key("q", qid)
            physical.append(key)
            builder = (self._write("update", key) if builder is None else
                       builder.update(key).never_expire())
            builder = builder.where(self._guard("q", qid)).put({"state": -1})
        results = self._batch(builder, physical)
        outcomes = {}
        for qid, result in zip(qids, results):
            try:
                if isinstance(result, Exception):
                    raise result
                _, status = result
                if status not in ("ok", "missing", "filtered"):
                    raise AerospikeStorageError("Unexpected mark rejection")
                if status == "filtered":
                    diagnostic = self._read("q", qid)
                    if diagnostic is not None and diagnostic[1]["state"] != -1:
                        raise AerospikeStorageError("Mark predicate rejected")
                outcomes[qid] = "ACKNOWLEDGED" if status == "ok" else "ABSENT_OR_DELETED"
            except Exception as exc:
                outcomes[qid] = exc
        self._completed("Mark incomplete", outcomes)

    @staticmethod
    def _completed(message, outcomes):
        if any(isinstance(value, Exception) for value in outcomes.values()):
            raise AerospikeStorageError(message, outcomes=outcomes, retryable=_retryable([
                value for value in outcomes.values() if isinstance(value, Exception)]))

    def flush(self):
        """Synchronous no-op, not a barrier that settles uncertain commands."""
        self._open()

    @_atomic
    def add_session(self, question_id, session_id, session_question):
        self._ready()
        qid, name, question = _id(question_id), _text(session_id), _text(session_question)
        sid = self._reserve("sid", 1)[0]
        key = self._key("q", qid)
        if self._sc:
            predicate = self._and(self._guard("q", qid), self._eq("state", 0), self._eq("adm", 0))
            _, status = self._point(self._write("update", key, predicate).put({"adm": 0}), key)
            if status != "ok":
                raise AerospikeStorageError("Session parent is absent, deleted or requires recovery",
                                           phase="ENTRY_REJECTED", identity=(qid, sid))
            try:
                self._insert("s", sid, dict(fmt=1, scope=self.cache_scope, sid=sid,
                                           qid=qid, sname=name, squestion=question))
            except AerospikeStorageError as exc:
                raise AerospikeStorageError("Session append failed", phase=exc.phase,
                                           identity=(qid, sid), retryable=exc.retryable) from exc
            return
        enter = self._and(self._guard("q", qid), self._eq("state", 0),
                          self._range("adm", 0, MAX_ID - 1))
        try:
            row, status = self._point(self._write("update", key, enter).add("adm", 1).get("adm"), key)
            if status != "ok":
                self._read("q", qid)
                raise AerospikeStorageError("Parent absent/deleted, capacity exhausted or entry rejected",
                                           phase="ENTRY_REJECTED", identity=(qid, sid))
            _integer(row.operation_result(1), 1)
        except Exception as exc:
            if isinstance(exc, AerospikeStorageError) and exc.phase == "ENTRY_REJECTED":
                raise
            raise AerospikeStorageError("Session entry failed; do not create or release",
                                       phase="UNKNOWN_ENTRY", identity=(qid, sid)) from exc
        bins = dict(fmt=1, scope=self.cache_scope, sid=sid, qid=qid, sname=name, squestion=question)
        child_error = None
        try:
            self._insert("s", sid, bins)
        except Exception as exc:
            if isinstance(exc, AerospikeStorageError) and exc.phase == "TERMINAL_REJECTION":
                child_error = exc
            else:
                # An unknown child command can still arrive: retain its admission.
                raise AerospikeStorageError("Child append failed; admission retained for recovery",
                                           phase="UNKNOWN_CHILD", identity=(qid, sid)) from exc
        try:
            release = self._and(self._guard("q", qid), self._range("adm", 1, MAX_ID))
            row, status = self._point(self._write("update", key, release).add("adm", -1).get("adm"), key)
            if status != "ok":
                raise AerospikeStorageError("Release rejected or parent missing")
            _integer(row.operation_result(1), 0, MAX_ID - 1)
        except Exception as exc:
            raise AerospikeStorageError("Child persisted; release uncertain, recovery required",
                                       phase="UNKNOWN_RELEASE", identity=(qid, sid)) from exc
        if child_error is not None:
            raise AerospikeStorageError("Child append rejected; admission released",
                                       phase="CHILD_REJECTED", identity=(qid, sid)) from child_error

    def list_sessions(self, session_id=None, key=None):
        self._ready()
        predicates = []
        if session_id:
            predicates.append(self._eq("sname", _text(session_id)))
        elif key:
            predicates.append(self._eq("qid", _id(key)))
        return [AerospikeSession(bins["sid"], bins["qid"], bins["sname"], bins["squestion"])
                for _, bins in self._scan("s", *predicates)]

    def _delete_child(self, sid):
        diagnostic = self._read("s", sid)
        if diagnostic is None:
            return
        row, bins = diagnostic
        predicate = self._and(*(self._eq(name, value) for name, value in bins.items()))
        generation = _integer(row.record.generation, 0)
        key = self._key("s", sid)
        builder = self._write("update", key, predicate).ensure_generation_is(generation)
        _, status = self._point(builder.with_durable_delete().delete_record(), key)
        if status == "filtered":
            if self._read("s", sid) is not None:
                raise AerospikeStorageError("Association delete predicate rejected")

    @_atomic(iterable="keys")
    def delete_session(self, keys):
        self._ready()
        sids = list(dict.fromkeys(_id(key) for key in keys))
        outcomes = {}
        for sid in sids:
            try:
                self._delete_child(sid)
                outcomes[sid] = "ACKNOWLEDGED_OR_ABSENT"
            except Exception as exc:
                outcomes[sid] = exc
        self._completed("Association deletion incomplete", outcomes)

    def _children_empty(self, qid, delete=False):
        children = self._scan("s", self._eq("qid", qid))
        if delete:
            self.delete_session([bins["sid"] for _, bins in children])
        elif children:
            raise AerospikeStorageError("Owned associations remain; parent retained", identity=qid)

    def _purge_parent(self, qid):
        key = self._key("q", qid)
        predicate = self._and(self._guard("q", qid), self._eq("state", -1), self._eq("adm", 0))
        _, status = self._point(self._write("update", key, predicate).with_durable_delete().delete_record(), key)
        if status == "filtered":
            if self._read("q", qid) is not None:
                raise AerospikeStorageError("Final parent delete rejected", identity=qid)

    @_atomic
    def clear_deleted_data(self):
        self._ready()
        candidates = self.get_ids(deleted=True)
        outcomes = {}
        for qid in candidates:
            try:
                diagnostic = self._read("q", qid)
                if diagnostic is not None:
                    bins = diagnostic[1]
                    if bins["state"] != -1:
                        raise AerospikeStorageError("Purge target is active", identity=qid)
                    if bins["adm"] != 0:
                        raise AerospikeStorageError("Pending admissions; operator recovery required", identity=qid)
                    self._children_empty(qid, delete=True)
                    self._children_empty(qid)
                    self._purge_parent(qid)
                else:
                    self._children_empty(qid)
                outcomes[qid] = "PURGED_OR_ABSENT"
            except Exception as exc:
                outcomes[qid] = exc
        self._completed("Question purge incomplete", outcomes)

    @staticmethod
    def _decode_report(payload):
        obj = _load(payload, ("v", "user_question", "cache_question", "cache_question_id",
                              "cache_answer", "similarity", "cache_delta_time", "cache_time"))
        _text(obj["user_question"])
        _text(obj["cache_question"])
        _integer(obj["cache_question_id"], 1)
        _unnode(obj["cache_answer"])
        for name in ("similarity", "cache_delta_time"):
            if type(_unnode(obj[name])) not in (int, float):
                raise ValueError("Invalid numeric report field")
        _time(obj["cache_time"])
        return obj

    @_atomic
    def report_cache(
        self,
        user_question,
        cache_question,
        cache_question_id,
        cache_answer,
        similarity_value,
        cache_delta_time,
    ):
        self._ready()
        for value in (similarity_value, cache_delta_time):
            if type(value) not in (int, float):
                raise ValueError("Report metrics must be native finite numbers")
        payload = _json(dict(
            v=1, user_question=_text(user_question), cache_question=_text(cache_question),
            cache_question_id=_id(cache_question_id), cache_answer=_node(cache_answer),
            similarity=_node(similarity_value), cache_delta_time=_node(cache_delta_time),
            cache_time=_now(),
        )).encode("utf-8")
        rid = self._reserve("rid", 1)[0]
        self._insert("r", rid, dict(fmt=1, scope=self.cache_scope, rid=rid, payload=payload))

    def _operator_context(self, context, action, qids=(), boot=None, binding=None):
        self._open()
        runtime = import_module("gptcache.manager.scalar_data._aerospike_operator_runtime")
        runtime._validate_provider_context(
            context, provider=self, action=action, namespace=self.namespace,
            cache_scope=self.cache_scope, qids=tuple(qids), boot=boot, binding=binding,
        )

    def _operator_inventory_empty(self):
        for kind in ("q", "s", "r"):
            if self._scan(kind):
                raise AerospikeStorageError("Initialization conflicts with owned data")

    def _initialize_scope(self, *, initialization_context, boot, binding, root_attempted):
        """Private settled-runtime primitive; durable intent must precede this call."""
        self._operator_context(initialization_context, "initialize", boot=boot, binding=binding)
        if type(root_attempted) is not bool:
            raise ValueError("Expected durable root-attempt state")
        self._active = False
        self._configure()
        self._configuration()
        if type(boot) is not str or str(uuid.UUID(boot)) != boot or not re.fullmatch(r"[0-9a-f]{64}", _text(binding)):
            raise ValueError("Invalid bootstrap identity")
        key = self._key("root")
        row, status = self._point(self._query(key), key)
        if status == "missing":
            if root_attempted:
                raise AerospikeStorageError("Root absent after prior attempt; history cannot be adopted")
            self._operator_inventory_empty()
            for kind in ("qid", "sid", "rid"):
                other = self._key(kind)
                _, absent = self._point(self._query(other), other)
                if absent != "missing":
                    raise AerospikeStorageError("Allocator exists without owned root")
            self._operator_context(initialization_context, "create_root", boot=boot, binding=binding)
            self._insert("root", None, dict(fmt=1, scope=self.cache_scope, boot=boot, bind=binding, phase=0))
            row, status = self._point(self._query(key), key)
        if status != "ok":
            raise AerospikeStorageError("Initialization root read failed")
        root = self._validate("root", row)
        if root["boot"] != boot or root["bind"] != binding:
            raise AerospikeStorageError("Another intent owns initialization")
        self._boot = boot
        if root["phase"] == 1:
            self.create()
            return
        self._operator_inventory_empty()
        for kind in ("qid", "sid", "rid"):
            self._operator_context(initialization_context, "initialize", boot=boot, binding=binding)
            other = self._key(kind)
            row, status = self._point(self._query(other), other)
            if status == "missing":
                self._insert(kind, None, dict(fmt=1, scope=self.cache_scope, boot=boot, hi=0))
                row, status = self._point(self._query(other), other)
            if status != "ok" or self._validate(kind, row)["hi"] != 0:
                raise AerospikeStorageError("Initialization allocator is not owned and pristine")
        self._operator_inventory_empty()
        for kind in ("qid", "sid", "rid"):
            if self._read(kind)[1]["hi"] != 0:
                raise AerospikeStorageError("Initialization allocator changed")
        self._operator_context(initialization_context, "activate", boot=boot, binding=binding)
        predicate = self._and(self._guard("root"), self._eq("boot", boot),
                              self._eq("bind", binding), self._eq("phase", 0))
        _, status = self._point(self._write("update", key, predicate).put({"phase": 1}), key)
        if status != "ok":
            raise AerospikeStorageError("Activation failed or uncertain")
        self.create()

    def recover_session_purge(self, qids, *, recovery_context):
        qids = tuple(dict.fromkeys(_id(qid) for qid in qids))
        self._operator_context(recovery_context, "recover", qids, self._boot)
        self.create()
        self._operator_context(recovery_context, "recover", qids, self._boot)
        outcomes = {}
        for qid in qids:
            try:
                self._operator_context(recovery_context, "recover", qids, self._boot)
                diagnostic = self._read("q", qid)
                if diagnostic is None:
                    self._children_empty(qid)
                    outcomes[qid] = "ABSENT"
                    continue
                if diagnostic[1]["state"] != -1:
                    raise AerospikeStorageError("Recovery target is not tombstoned")
                self._children_empty(qid, delete=True)
                self._children_empty(qid)
                self._operator_context(recovery_context, "reset", qids, self._boot)
                key = self._key("q", qid)
                predicate = self._and(self._guard("q", qid), self._eq("state", -1))
                row, status = self._point(self._write("update", key, predicate).put({"adm": 0}).get("adm"), key)
                if status != "ok" or _integer(row.operation_result(1), 0, 0) != 0:
                    raise AerospikeStorageError("Recovery reset failed or uncertain")
                self._children_empty(qid)
                self._operator_context(recovery_context, "purge", qids, self._boot)
                self._purge_parent(qid)
                outcomes[qid] = "PURGED"
            except Exception as exc:
                outcomes[qid] = exc
                break
        for qid in qids:
            outcomes.setdefault(qid, "NOT_ATTEMPTED")
        self._operator_context(recovery_context, "complete", qids, self._boot)
        return outcomes

    def close(self):
        if not self._closed:
            self._closed = True
            self._cluster.close()
