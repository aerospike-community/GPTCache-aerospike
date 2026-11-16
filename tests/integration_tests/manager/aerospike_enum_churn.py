"""Bounded history-trigger experiment on a NEW synthetic diagnostic cache only."""

from types import SimpleNamespace
import uuid


def run(provider, definition, observe, inspect_point, safe_failure):
    from gptcache.manager.scalar_data.base import CacheData

    assert inspect_point(provider, "qid")["hi"] == 2
    assert inspect_point(provider, "sid")["hi"] == inspect_point(provider, "rid")["hi"] == 0
    with definition.connect() as cluster:
        truth = SimpleNamespace(_session=cluster.create_session(provider._behavior), _key=provider._key)
        assert all(inspect_point(truth, "q", qid, controls=True)["state"] == -1 for qid in (1, 2))
        known = [1, 2]
        # The old failure environment had536 physical q tombstones. Match that
        # history scale without touching its data: two anchors plus534 new rows.
        for iteration, size in enumerate((64,) * 8 + (22,)):
            token = uuid.uuid4().hex
            ids = provider.batch_insert([CacheData("churn-" + token + str(i), ["synthetic"])
                                         for i in range(size)])
            known.extend(ids)
            assert len(ids) == len(set(ids)) == size
            provider.mark_deleted(ids)
            before = {qid: inspect_point(truth, "q", qid, controls=True) for qid in ids}
            assert all(row is not None and row["state"] == -1 and row["adm"] == 0
                       for row in before.values())
            error = None
            try:
                provider.clear_deleted_data()
                terminal = "RETURNED"
            except Exception as exc:
                terminal, error = safe_failure(exc), exc
            after = {qid: inspect_point(truth, "q", qid, controls=True) for qid in known}
            observe("churn_round", {"iteration": iteration, "size": size, "ids": ids,
                                   "insert_and_mark": "ACKNOWLEDGED",
                                   "before": before, "purge": terminal, "after": after})
            if error is not None:
                raise error
            assert all(row is None for row in after.values()), (
                "Purge returned with independently present tombstones; stop without cleanup")
        ids = provider.batch_insert([CacheData("churn-anchor-" + uuid.uuid4().hex, ["synthetic"])
                                     for _ in range(2)])
        provider.mark_deleted(ids)
        controls = {qid: inspect_point(truth, "q", qid, controls=True) for qid in ids}
        observe("churn_complete", {"physical_q_deletes": 536, "new_anchors": controls})
        return {"physical_q_deletes": 536, "anchor_ids": ids}
