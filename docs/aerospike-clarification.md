# Aerospike Clarification — resolved requirements

## Latest actual conditional constraint decision — 2026-11-06

Actual user selected **“Allow verified constraint”** and **“Allow isolated
runtimes”**, recorded **2026-11-06T01:05:26.763355+00:00** (response recording
time, not an invented message-event time). The former permits adopting a
namespace-default-read-touch-disabled requirement **only after compiled
released-client, wire and TTL proof** preserves no-touch/no-expiry. The latter
permits task-owned CPython 3.12/3.8 standalone downloads, not package-byte
downloads or global runtime changes. This supplements §8's scoped authority;
it does not authorize this documentation revision to perform runtime operations.

The parent now adopts the **proven SDK-only constraint** after the completed
namespace preflight: namespace `default-read-touch-ttl-pct=0`; relevant
`gptc_q`, `gptc_s`, `gptc_r`, `gptc_meta` set overrides 0 (inherit) or -1
(disabled), never positive. Public SDK
`session.info().info_on_all_nodes("get-config:context=namespace;id=...")`
and `"sets/<namespace>"` were executed on both client versions. Activation
must inspect all applicable nodes and fail **before writes** on missing
permission/coverage, malformed, mixed or unsafe configuration. No constructor
namespace mutation or silent info-failure bypass is authorized. Trusted
operators maintain pinned supported configuration/topology; explicit activation,
maintenance/reopen and configuration-change revalidation boundaries are
required. No automatic drift detection or atomic config lease is claimed.

The actual common query path is public SDK typed `.where(expr).bins(list)`
and `.with_txn(None)`, caller-derived public Behavior/Settings only. No public
native-policy bridge, private mapper, PAC or underlying-client CRUD is adopted.
The earlier parent native-policy-extension choice is **superseded**, because
released SDK 1.0 lacks `.with_policy` and `.filter_expression`.
Query wire 0 relies on the verified disabled configuration; point reads use
no-touch -1 and every modifying segment never-expire -1. No retention semantics
are relaxed.

Completed compiled released SDK/native 1.0 on isolated CPython 3.12.14 and
development SDK 0.9.0a5/native 0.6.0a6 on CPython 3.14.7 exercised typed full
4096-partition inventory, exact qids 1/2/3, nine physical bins, digest/bytes and
terminal completion. Foreign finite controls did not extend under query/default
0; explicit +50 extended. Nine owned never-expire markers survived >14.27 s
against test-only namespace TTL12, ordinary point `ttl=None`; literal wire
captures support the result. These are synthetic API/retention observations,
not final-provider tests. The server was **development 8.1.3**; published SDK
minimum is **8.2.0+**. Actual supported released-server group04 evidence remains
mandatory, with no inferred compatibility.

This records actual conditional user authorization and parent evidenced
adoption, **not full revised-contract approval**, independent review/pass,
model-validator success or task 01-02.2 completion. The revised guide/summary
must remain uncommitted pending fresh independent exact-pair review and one
final actual user implementation-contract gate.

**Latest model decision, recorded 2026-11-05T23:33:39Z UTC:** actual user
selected **“Approve conditional model”** for revised group-1 admission controls
and group-2 session contract, subject to resolving explicit capability/recovery
blockers **before implementation**. This supersedes §7's historical pending
approval, not A01–A09. Independent revised review is ready with qualifications;
all HIGH activation blockers remain unresolved. Backend-only
`recover_session_purge(qids, *, recovery_context)` with real proof/refusal is
accepted, **not** a common `CacheStorage` API change. This permits exploratory
group-3 drafting only. No production, implementation or final-model approval.

Actual **“Allow scoped inspection”** separately permits SDK-local editable
dependencies/index and a dedicated local development container with pulling
disabled and new file-backed data. Authorization is not completed evidence;
capability inspection is separately owned. No partial inspection findings are
adopted here. UTC above is recording time, not a supplied message-event time.

**Status: `RESOLVED_APPROVED_ASSUMPTIONS` — Step 0 resolved for exploratory modeling.**
This is a requirements artifact, not a schema or implementation approval.
Actual stakeholder dispositions are recorded in §6. Unmeasured inputs remain
unknown; deployment capability checks and each model review gate remain required.

The scope is the complete scalar `CacheStorage` integration. There is no
Aerospike vector-search or Graph backend in scope. No record layout, physical
keys, bins, indexes, allocator mechanism, or schema summary is proposed here.
The companion [entity-group routing plan](aerospike-entity-groups.md) likewise
does not select storage patterns.

## 1. Confirmed requirements and source facts

Paths and line numbers refer to the host source reviewed for this clarification.
Sibling providers and their tests are read-only evidence, not authority for
inconsistent behavior.

| ID | Confirmed fact / requirement | Source |
|---|---|---|
| F01 | `CacheData` owns a string or structured question, ordered answers, optional embedding, input session name, and optional creation/access datetimes. `QuestionDep` carries name/data/type; answers carry value/type. `Any` does not authorize arbitrary-object persistence. | `gptcache/manager/scalar_data/base.py:10–96` |
| F02 | The scalar contract has twelve abstract methods plus `flush`: all thirteen behaviors below are required. | `gptcache/manager/scalar_data/base.py:99–161` |
| F03 | Returned insert IDs must correlate positionally with inputs. The manager uses these same IDs to add vectors. FAISS passes numeric IDs to `add_with_ids` and converts search IDs to Python integers. | `gptcache/manager/data_manager.py:300–338`; `gptcache/manager/vector_data/faiss.py:33–54` |
| F04 | Host reference state is active `0`, deleted `-1`. Reads return `None` for genuine absence/deletion and return the **previous** `last_access` while persisting a new access time. Creation time remains stable. | `gptcache/manager/scalar_data/sql_storage.py:64–67,277–322`; `tests/unit_tests/manager/test_sql_scalar.py:109–142` |
| F05 | Enumeration/count are question-only, state-filtered, scoped operations. `get_ids(True)` means deleted, not all; `count(is_all=True)` includes both states. Startup enumerates active IDs; eviction separately counts, enumerates tombstones, purges, then deletes vectors. | `gptcache/manager/scalar_data/sql_storage.py:324–360`; `gptcache/manager/data_manager.py:245–247`; `gptcache/manager/eviction_manager.py:20–46` |
| F06 | A session association has its own identity, distinct from its question ID, plus question ID, session name and original session question. Manager session-name deletion first collects **association IDs**, then calls scalar deletion with those IDs. | `gptcache/manager/scalar_data/sql_storage.py:92–113,363–385`; `gptcache/manager/data_manager.py:378–391` |
| F07 | Session reads are by question, by name, and unfiltered. Question-filtered results need `.id`, `.question_id`, `.session_id`, `.session_question`; name-filtered results are converted to association IDs; unfiltered results to distinct session names. | `gptcache/manager/data_manager.py:340–358,381–391` |
| F08 | On save, session name comes from `Session.name`. On hit, the adapter appends an association with the original pre-embedding question. Hit checking consumes parallel name/question lists; default checking rejects a repeated session name. | `gptcache/manager/data_manager.py:273–275,340–358,378–379`; `gptcache/adapter/adapter.py:209–231,472–494`; `gptcache/processor/check_hit.py:2–20` |
| F09 | `Session.drop()` deletes associations through the manager, not cached questions or vectors, despite its broad docstring. SQL purge deletes question-owned answers/dependencies/sessions but not reports. | `gptcache/session.py:69–76`; `gptcache/manager/data_manager.py:389–391`; `gptcache/manager/scalar_data/sql_storage.py:337–351` |
| F10 | Report writes contain user question, cached question/ID/answer, similarity and elapsed time. Adapter hit reporting is conditional on reports being enabled; non-string question/answer surfaces are passed as empty strings. There is no report read/delete API in `CacheStorage`. | `gptcache/adapter/adapter.py:217–231,480–494`; `gptcache/manager/scalar_data/base.py:147–157`; `gptcache/manager/scalar_data/sql_storage.py:387–398` |
| F11 | Manager normalizes embeddings and supplies float32; scalar retrieval must also support absent embeddings without attempting `frombuffer(None)`. Ordered values and Unicode code points/bytes must survive without normalization or executable decoding. | `gptcache/manager/data_manager.py:314–328`; `gptcache/manager/scalar_data/base.py:68–71`; accepted scalar round-trip requirements |

Already accepted constraints, **not questions to reopen**:

- Multiple processes may intentionally share matching configured scopes. All
  point, enumeration, count, session, report and purge paths must honor scope.
  Scope strings are metadata, not authentication.
- No new cross-record/server snapshot guarantee and no scalar/vector distributed
  transaction. Enumeration and counts may observe concurrent mutations.
- Batch/session/report failures, persisted subsets and in-doubt outcomes must be
  visible; an exception cannot be replaced by success-shaped output.
- No implicit TTL, including metadata, updates and read-touch. Unknown retention
  duration is not permission for expiry, and omitting an expiry policy is not
  proof of no expiry under finite namespace defaults.
- Text/image value forms and an explicit native allowlist only: no pickle,
  executable decoder or arbitrary-object fallback.
- Oversize fails clearly, without truncation or transparent chunking.
- One lifecycle-owned SDK connection/session; explicit close; no sibling edits.
- Workload, cardinality, sizes, fan-out, rates, skew, retention measurements and
  production SLOs are **unknown**, already confirmed absent from project context.
  No production bounds or arbitrary application hard caps may be invented.

## 2. Logical entities, relationships, ownership and lifecycle

This is a domain inventory only; entities are not a proposed set/record list.

| Entity / relationship | Ownership and reads | Lifecycle / cardinality disposition |
|---|---|---|
| Logical cache scope → cached questions | Equal configured scopes share intentionally; every path is restricted to that scope. | Many questions; population/growth/skew unknown (A01). |
| Question → answers | Owned ordered value/type sequence; fetched with question, no independent answer API. | Zero or more from DTO construction; exact payload domain A07. Coupled to question purge; size/count unknown (A01). |
| Question → dependencies | Optional owned ordered name/data/type sequence; read through structured question. | Zero or more; absence/empty reconstruction A07; coupled purge; size/count unknown (A01). |
| Question → embedding | Optional scalar float32 payload, not similarity search. | Zero or one array; dimension/bytes unknown (A01); coupled purge. |
| Question ↔ session name, through association | Multiple questions per name and multiple associations per question; association has independent ID and session-question text. Reads in both directions and unfiltered. No separate session account/authentication entity is implied. | Duplicate semantics A02; filters/DTO shape A03; missing/deleted parent and purge races A04; degree/churn unknown (A01). |
| Report → referenced cached question | Hit event includes copies of question/answer values and numeric reference; write-only through this API. Scope owns report history. | SQL reference retains history after question purge; proposed independence A05. Event rate, bytes and horizon unknown (A01). |
| Numeric identity allocation metadata → question / association identities | Shared processes require collision-safe question identities compatible with vector consumers and independent association identities. This is a coordination responsibility, not a selected data structure. | Must not reset/reuse a live identity on create/reopen/purge; lifetime and failure review mandatory. Allocation mechanism and sizing unresolved. A01/A08/A09 apply. |

## 3. Complete access-path and behavior inventory

All rows inherit scope isolation, no expiry and visible transport/auth/decode
failures. Frequency, p95 latency and cardinality are unknown for **every** row
(A01). No ordering is promised for enumeration unless separately specified;
input/answer/dependency/embedding correlation is not optional.

| Behavior | Read / write criteria and required observable result | Remaining gap |
|---|---|---|
| `create` | Repeated initialization must be safe for concurrent shared writers; no data wipe or identity reset. | Environment permissions/capabilities A09; physical actions belong to later modeling. |
| `batch_insert` | Ordered input DTOs → corresponding numeric IDs; own answers/dependencies/embedding plus optional initial association; empty input has no writes and returns `[]`. | A01/A02/A04/A06/A07/A08. |
| `get_data_by_id` | Numeric ID → complete active DTO; genuinely absent/deleted → `None`; return old access time and persist touch, with touch failure visible. | Session field A03; empty/optional payload A07; time A06; race contract reviewed later. |
| `mark_deleted` | Existing questions by IDs → state `-1`; repeated/missing IDs cannot create or resurrect data. | No new gap; model must prove concurrent update-only behavior. |
| `clear_deleted_data` | Scope tombstones → purge and question-owned payload/association cleanup; leave active/unrelated-scope data alone; repeat safely. | A04/A05/A08; fan-out A01. |
| `get_ids` | Scope question IDs in deleted (`True`) or active (`False`) state; complete list, no hidden truncation. | Resource consequences A01; no new snapshot guarantee. |
| `count` | Scope question count by state, or all states with `is_all=True`; exclude associations/reports/coordination metadata. | Resource consequences A01; not a consistent multi-call snapshot. |
| `flush` | Explicit flush semantics; proposed no-op only if writes are synchronously acknowledged, never permission to ignore failures. | A08. |
| `add_session` | Question ID + name + original session question → durable association with independent ID. | A02/A04/A08. |
| `list_sessions` | Name / question / neither / both filters → association DTOs, not question IDs or bare names. | Both-filter and absent-filter rules A03; parent lifecycle A04. |
| `delete_session` | Association IDs only → remove those associations, not parents/vectors/reports; missing IDs and replay safe. | A04/A08. Concurrent new associations are not captured by an earlier name-list operation. |
| `report_cache` | Append supplied six report fields durably; no new read API; failures propagate. | Retention/cascade A05, values A07, replay A08. |
| `close` | Close owned resources once; repeated close safe; no per-operation close. | Existing lifecycle tracer already covers resource ownership; persistence remains unavailable pending model approval. |

## 4. Required decision-pack inputs: source inventory

Reference: data-modeling guide `new-app-modeling-checklist.md` §§5.1–5.8.
Every MISSING item below requires disposition, not a guessed value. Questions
are consolidated into A01–A09 to avoid re-requesting already unavailable data.
The table preserves the original gap inventory. Its MISSING/question labels are
historical input status, **not unresolved dispositions**: §6 maps every row to
the actual accepted answer. No measurement has been added.

| Applicable gate / inputs (exhaustive for routed packs) | Current evidence and required disposition |
|---|---|
| Shared sizing §5.1: child bytes min/p50/p95/max; child counts min/p50/p95/p99/max; projected aggregate p95/p99; operational size/write thresholds | **MISSING — question:** approve A01 leaving all these unmeasured, or supply an actual sizing source? Applies separately to answers, dependencies, embeddings, associations and reports. Aggregate projections cannot be computed from absent distributions. |
| Shared contention §5.2: `events_per_root_active_window`, `active_window_sec`, `contention_window_ms`, `burst_factor_p99_over_avg`, `avg_child_payload_bytes`, `projected_child_count_p99`, `projected_record_size_kib_p99`, `growth_horizon_days`, `write_slo_p95_ms` | **MISSING — question:** approve A01 for exploratory-only use? No local measurement exists. Guide contention/burst defaults are not deployment observations and are not adopted as workload facts. |
| Shared §5.2: `primary_read_shape`, `delete_semantics`, `load_test_done`, `observed_key_busy_rate`, `observed_write_p95_ms` | Read surfaces confirmed in §3: full owned payload read, individual question access, full enumerations and both session directions; dominant frequency unknown. Deletion confirmed in F04/F05 with association/report gaps A04/A05. Load test **no**; busy/latency observations **MISSING — question:** A01 disposition? |
| Pack 1, 1:N §5.4: counts p50/p95/p99; payload p50/p95/max; dominant full-set/subset/single-element read; write/burst profile; lifecycle coupling; eventual/transactional correctness; growth/capacity | Applies to owned question children and question→associations. Structural read coverage confirmed (§3), dominance/sizing/rates/growth **MISSING** (A01). Owned payload purge coupled (F09); sessions A04. Visible partial persistence and no cross-record snapshot accepted; detailed recovery A08. |
| Pack 2, N:M §5.5: query directions; edge metadata; uniqueness/idempotency; edge churn; degree p50/p95/p99 and skew per side; cascade/cleanup | Both directions and unfiltered confirmed F06–F08; metadata is association ID/question ID/name/question text, no host-required edge timestamp. Duplicates **MISSING — question:** A02? Filter semantics A03; degree/churn/skew **MISSING** (A01); parent cleanup **MISSING** (A04/A08). |
| Pack 4, event §5.7, report review: actor/recipient rate p50/p95/p99; read shape; mutation/mark-read model; retention/TTL; read/write targets; cleanup workload | Write-only report surface confirmed F10; no timeline/latest-page/deep-pagination/unread requirement, so mark-read consumption is **not applicable**, not invented. Logical scope/question are known dimensions, not a new actor/recipient entity. Rates/size/targets/horizon **MISSING** (A01); no TTL accepted; independent history **MISSING — question:** A05? Report timeline optimizations require new requirements. |
| Pack 5, cleanup §5.8: side-effect fan-out p50/p95/p99; initiating p95 latency; immediate vs eventual-with-bound; reconciliation lag; per-mutation retry safety; attempts/backoff/escalation; pending-read behavior; trigger model; observability | Fan-out/latency/lag/operational budgets **MISSING** (A01). Host calls purge explicitly and `Session.drop()` explicitly; no worker/scheduler exists here (F05/F09). State hides deleted question reads; association visibility and cleanup outcome **MISSING — question:** A04? Retry/observability **MISSING — question:** A08? No silent eventual-cleanup service inferred. |
| Cross-cutting ID / small-independent-entity cost gate: numeric interoperability, uniqueness, repetition pressure/byte cost, metadata contention/lifetime | Numeric IDs and independent association identity confirmed F03/F06. Repetition/size/rate evidence **MISSING** (A01); no allocator format selected. Lifecycle/no reset in §2; outcome/retry A08 and capabilities A09 must be reviewed. |
| Cross-cutting timestamp, codec, deployment/security/version gates | Host DTO/source facts F01/F04/F11 known, exact required time/allowlist semantics **MISSING** (A06/A07). Actual namespace mode, record-size/default expiry/read-touch settings and permissions **MISSING** (A09); no version-gated feature assumed available. |

Pack 3 hierarchical traversal is **not applicable**: the host exposes no tree,
subtree or arbitrary-node traversal. Routing to a pack is not selecting its
storage pattern. No sizing math has been performed on fabricated inputs.

## 5. Accepted assumptions and consequences

A01/A02/A04/A05/A07/A08/A09 below retain their originally proposed wording,
accepted exactly by the stakeholder. Their “Question / proposed decision” labels
identify the accepted question, not a still-pending vote. A03/A06 are the actual
SQL-compatibility selections, replacing the rejected proposals. See §6.

### A01 — Unknown workload stays unknown (exploratory scope only)

**Question / proposed decision:** Approve proceeding with exploratory scalar
integration while all measurements listed in §4 remain explicitly unknown,
including population, payload/dimension distributions, answer/dependency/session
fan-out, report volume, per-root rates/window/burst/skew, growth/retention horizon,
latency/conflict budgets and cleanup lag? No repeated demand for these same absent
numbers is intended.

**Consequence:** No production fit, capacity/SLO, dominant-path, bounded fan-out,
retention-duration or safe-consolidation claim; no invented caps, synthetic values
masquerading as measurements, or speculative optimization. Complete-list APIs may
have resource costs proportional to actual data. Later group worksheets retain
unknowns and must distinguish exploratory validation from production readiness.
Approval does not waive per-group gates or authorize a sizing-dependent choice
that remains unjustified. **Reopen trigger:** representative samples/traffic,
deployment intent, production acceptance request, or observed size/contention/
latency/resource failure.

### A02 — Session duplicates are distinct associations

**Question / proposed decision:** May repeated `add_session` calls, even with the
same question/name/text, create distinct independently deletable associations,
including the initial association from insert?

**Evidence:** SQL inserts rows without tuple uniqueness (F06/F08); custom hit
checkers see association lists. **Consequence:** No silent deduplication or
exactly-once replay guarantee; repeated or uncertain writes can create duplicates.
**Reopen trigger:** a consumer requires uniqueness or retry-stable event identity.

### A03 — Match SQL edge cases for session filtering and returned shape

**Accepted selection: “Match SQL edge cases.”** `list_sessions(session_id, key)`
uses `if session_id` for exact name filtering, `elif key` for question filtering,
otherwise all scoped associations. A truthy name takes precedence when both
filters are truthy; it is not an intersection. Empty name and zero key do not
activate their filters. Results retain association DTO fields F07 and have no
implicit sort. Initial association insertion is conditional on a truthy input
session name, as in SQL `_insert`.

`get_data_by_id().session_id` is a list of one-column row/tuple-compatible values
`[(name,), ...]`, including duplicates, `[]` when none. Each supports `[0]`,
unpacking, and `.session_id`; do not normalize to strings. This is distinct from
`list_sessions`, which returns full association DTOs.

**Evidence:** `sql_storage.py:248–256,294–298,313,378–385` and F07.
**Consequence:** Empty-filter truthiness and precedence are intentionally
compatible, even if surprising. The prior intersection/None-only/string-list
proposal was not accepted. **Reopen trigger:** explicit change to SQL-compatible
public behavior; do not silently change the manager or siblings.

### A04 — Session ownership and parent lifecycle

**Question / proposed decision:** Approve rejecting association additions to
missing/already-deleted parents, retaining existing associations through soft
deletion until purge, and removing owned associations on physical purge?
Session-name drop deletes associations only, never questions/vectors/reports.

**Evidence:** F04/F09; SQL has no parent guard, so its orphan admission is not
copied as a requirement. **Consequence:** Soft-deleted question reads return
`None`, but associations can remain listable until purge. Purge/session-write
races require a reviewed outcome/recovery contract: no resurrection, no claimed
cross-record atomicity, visible failures and retry-safe cleanup. A successful
purge must satisfy the reviewed ownership contract; this assumption is not
permission to silently leave orphans. A name-drop list/delete can miss a later
concurrent addition; it is not a permanent session-name ban.
**Reopen trigger:** immediate association hiding, concurrent-drop fencing,
independent session survival or bounded cleanup lag is required.

### A05 — Reports survive parent purge, with no automatic expiry

**Question / proposed decision:** Approve reports as independent historical hit
events retained after referenced questions are soft-deleted/purged, with no new
report query/cleanup/automatic-expiry API in this MVP?

**Evidence:** F09/F10; SQL purge excludes reports. **Consequence:** References may
outlive questions; report growth is unknown and not bounded by question eviction.
No legal/compliance retention or audit/exactly-once guarantee is implied.
**Reopen trigger:** erasure/compliance obligation, finite retention requirement,
history read requirement, or observed resource pressure. If parent erasure must
remove copied payloads, this assumption must be revised before modeling.

### A06 — Timestamp meaning and supplied times

**Accepted selection: “Match SQL behavior.”** Ignore supplied `CacheData.create_on`
and `CacheData.last_access` on insert. Generate both with naive client-local
`datetime.now()` at insertion (separate evaluations are permitted, matching SQL
column defaults). Creation time is immutable. On an active read return the prior
`last_access` while persisting a new naive client-local `datetime.now()`. Return
naive datetimes at Python microsecond precision.

**Evidence:** `sql_storage.py:64–65,225–233,277–288,314–315`.
**Consequence:** No UTC-aware return contract, naive-as-UTC inference, supplied
instant preservation, timezone conversion, authoritative server-clock ordering,
or monotonic ordering across processes/clock rollback. Local clock and timezone
configuration affect values just as in SQL. Physical encoding is undecided.
The prior UTC normalization proposal was not accepted. **Reopen trigger:** an
explicit requirement for supplied times, portable instants or clock ordering.

### A07 — Explicit safe values and optional DTO reconstruction

**Question / proposed decision:** Is this sufficient for exploratory callers:
answer values `None`, bool, signed-64-bit integers, finite floats, str, bytes,
lists and string-keyed dicts recursively containing those values; no arbitrary
objects, cycles, tuple/set/datetime-as-answer or coercion fallback? Questions and
dependency name/data remain strings with their type tags. Preserve empty
strings/bytes/answer lists and value order; absent embedding returns `None`,
present embedding returns float32 values/order. Reconstruct a dependency-free
question as a string, otherwise as `Question` with ordered dependencies.

**Evidence:** F01/F11; images may be scalar text/bytes or object-store references,
not permission to serialize an arbitrary image object. **Consequence:** `None`
versus empty dependency-list distinction and original dependency-free `Question`
wrapper are not preserved; no silent Unicode normalization. Unsupported native
values fail clearly before avoidable writes; malformed stored shapes fail rather
than become misses. Integer range is a type/interoperability boundary, not a
workload cap. No byte/dimension/count cap is invented.
**Reopen trigger:** a real caller needs another safe type, non-finite numbers,
different array shape/dtype fidelity, or exact optional-wrapper preservation.

### A08 — Failure recovery has no exactly-once claim or hidden worker

**Question / proposed decision:** Approve caller-driven recovery with no automatic
background reconciliation/SLA and no blind retry of non-idempotent uncertain
inserts/appends? Errors must distinguish known successful/failed/unknown outcomes
where the SDK can determine them, preserve input/ID correlation where available,
and identify in-doubt state without exposing payloads/secrets. Unknown outcomes
stay unknown.

**Evidence:** Partial failure visibility already accepted; host has no request
token or durable retry queue (F02/F05/F10). **Consequence:** Retrying batch,
session or report appends can duplicate previously persisted work; failed scalar
insert can leave data the manager has not added to vectors. Repeating marks,
association-ID deletion and purge must be safe. No automatic scalar/vector
rollback, distributed transaction, dead-letter service or bounded cleanup lag is
promised. Synchronous acknowledged writes may make `flush` a documented no-op,
not a success substitute. Caller-invoked cleanup must surface failures; later
review must specify recovery of interrupted cascades.
**Reopen trigger:** exactly-once, automatic retry/escalation, failure budget,
bounded recovery or cross-provider atomicity becomes a requirement.

### A09 — Unknown deployment capabilities remain gated

**Question / proposed decision:** May actual namespace consistency mode,
configured record-size limit, finite default expiry/read-touch configuration,
server/client compatibility and index permissions remain unverified until
capability inspection in the authorized environment? No secrets are requested.

**Consequence:** No transaction/advanced-index/version capability, production
operability or released compatibility claim is authorized. Model choices that
require a capability stay blocked until checked; explicit no-expiry behavior for
every selected record kind still needs policy and later live proof. Development
provenance is maintained only in local execution handoffs, not these model docs.
**Reopen trigger:** any proposed model depends on such a capability, or live/
released validation is about to start. No installs, network, provisioning or
container starts are authorized by this assumption.

## 6. Actual stakeholder disposition log and resume contract

**Recorded at 2026-11-05T18:11:43Z (UTC recording time).** The response was
delivered in the continuation instruction; no separate message-event timestamp
was supplied. The recording time is not an invented earlier approval time.

| IDs | Actual response | Disposition |
|---|---|---|
| A01, A02, A04, A05, A07, A08, A09 | “Approve listed assumptions” exactly as written, including exploratory consequences/reopen triggers | APPROVED |
| A03 | “Match SQL edge cases”: preserve SQL session-name filter precedence/truthiness and returned session-name row/tuple shapes, not intersection/string normalization | APPROVED_REVISED; normative text in §5 |
| A06 | “Match SQL behavior”: naive client-local `datetime.now()` on insert/touch; ignore supplied DTO timestamps; return prior access; no UTC-aware or naive-as-UTC inference | APPROVED_REVISED; normative text in §5 |

Exhaustive §4 disposition coverage:

| Original inventory row | Disposition of missing inputs |
|---|---|
| Shared sizing §5.1 | APPROVED_UNKNOWN A01: every distribution, aggregate, threshold remains unmeasured |
| Shared contention §5.2 (numeric inputs) | APPROVED_UNKNOWN A01: all rates, windows, burst, byte/count, horizon and SLO inputs unknown |
| Shared §5.2 (read/delete/load evidence) | Structural paths §3; A04/A05 deletion; no load test; busy/latency unknown under A01 |
| Pack 1 | APPROVED_UNKNOWN A01 sizing/dominance/growth; A04 lifecycle; A08 recovery |
| Pack 2 | A02 duplicate identity; A03 filters/shapes; A04/A08 cleanup; A01 unknown degrees/churn/skew |
| Pack 4 | A05 history independence; A01 unknown volume/rates/horizon/budgets; no timeline/unread path |
| Pack 5 | A04 ownership and deleted visibility; A08 caller-driven recovery/no SLA; A01 unknown fan-out/latency/lag/budgets |
| ID / small entity cost | Numeric interoperability confirmed; A01 unknown repetition/size/contention; A08 failures; A09 deployment gate |
| Timestamp / codec / deployment | A06 SQL time behavior; A07 allowlist; A09 approved unknown capabilities, **not permission to rely on them** |

Step 0 requirements disposition is complete, without inventing measurements.
This response approves **clarification only**, not entity groups, any schema,
model evidence, or persistent code. Each group still needs its developer
walkthrough, independent implementer review and actual stakeholder checkpoint.
Final summary is derived only after all groups pass.

## 7. Actual exceptional recovery decision — requirements only

**Actual response: “Allow quiesced recovery”. Recorded
2026-11-05T23:19:11Z** from the continuation instruction (UTC recording time,
not a separately supplied message-event timestamp). This adds a specific
operational prerequisite to A04/A08, without changing A01–A09's accepted text.

Question answered: after crashed/in-doubt session admission, may purge fail
visibly and retain its parent tombstone until caller-driven recovery verifiably
stops **all** applicable writers and settles outstanding database commands?
User accepted that prerequisite and explicitly authorized **reopening group 1's
fixed-size parent admission controls**. Normal operation still supports shared
writers. No online exceptional recovery or recovery-time/SLA promise is made.
Timeouts, process kill alone, repeated empty scans, a blind reset/decrement or
an unchecked `quiesced=True` are not proof. Unknown release cannot be blindly
replayed; unresolved accounting retains the tombstone, not purge-success output.
Successful physical purge must leave no owned associations.

Evidence/architect triage: independent review **0595eced**, inspection clock
2026-11-05T18:42:31Z, **BLOCKED — CONFIRM G2-C1, WITH A MINIMAL-BASELINE
QUALIFICATION**, confirmed parent-precheck/child-write/postcheck's late-child
race. Architect accepts G2-C1/G2-R1–R4 as fix-in-spec requirements addressed in
the unapproved §12 control/recovery draft of the schema guide. G2-R5 remains a
normal implementation/A09 capability gate: SDK MRT surfaces are not deployed
capability or online-recovery proof; MRT is not mandatory for this candidate.
Full findings/triage are in the model-review ledger; no revised independent pass.

Immutable payload, state `0/-1`, timestamps, SQL truthy-name precedence/row
projections and numeric identity remain binding. This is not approval of revised
group 1, group 2, reports, persistent code, production sizing or capabilities.
Backend-specific recovery's actual exclusion/settlement mechanism remains
unverified; no deployment facility or privilege is invented. A change to the
common API/user privileges or inability to satisfy the preconditions requires a
new decision/`REVISION_CONFLICT`, not weaker ownership semantics.

Next: independently review the revised group-1 controls **and** group 2; triage
actual findings, then obtain actual stakeholder approval before group 3.
Original exploratory group-1 approval `3e0bd4b` and clarification completion
`eef1144` are not approval of this draft. Plan 01-02 remains 1/3 tasks complete;
only 01-01 is complete (1/6 plans). No final schema summary/model evidence.

## 8. Actual supported-envelope clarification and scoped-tooling authority

**Recorded 2026-11-06T00:15:57Z UTC**, from the continuation's observed UTC
recording clock. No separate user-message event timestamp was supplied.
Provenance: the actual current user continuation explicitly reports the
selections **“Approve minimal envelope”** and **“Allow scoped tooling”**, and
spells out their consequences below. This is an actual requirements decision,
not an author-assumed approval or an independent implementation review.

The supported envelope is trusted operators, intact persistent cache history,
and new or consistently restored scalar/vector stores. Normal shared-writer
CRUD may be supported under explicit effective configuration assumptions within
that envelope. Explicitly authorized new-logical-cache initialization (including
resumable partial bootstrap) and isolated, maintenance-quiesced recovery are
permitted. Known missing/corrupt history must fail closed; no ordinary
missing-metadata-to-zero path is authorized.

The provider does **not** guarantee safety after out-of-band metadata deletion,
inconsistent restores, malicious administrators, available AP-partition writes,
or power-loss durability. This explicitly supersedes overbroad unconditional
“all-time history” interpretations **only as a user-approved supported-envelope
clarification**, not a silent relaxation of the accepted non-reuse contract.
IDs are never reused over intact supported history; surviving maxima, empty
records, create-only collisions and an empty vector scan are not proof of
historical pristineness. A trusted operator's explicit intent to initialize a
NEW logical cache with a new/consistent vector store is the authority, not an
impossible automatic proof that no historical external references ever existed.
The operator is responsible for preservation and consistent scalar/vector
restore, not a new provider-owned vector backup service.

Narrow **provider-only** initialization/recovery helper APIs and operator CLI,
with cross-operator maintenance locking, are authorized for design and later
reviewed implementation. Existing shared `CacheStorage` signatures stay
unchanged. No hidden worker, mandatory MRT/SC, boolean verification bypass or
permanently refusing placeholder counts as a usable recovery feature.

Verification authority is limited to OWN NEW disposable containers/data and
test writers, including clean stop/restart and termination of those OWN writers.
No image pulls, raw disks, production operations or reuse of existing data are
authorized for those experiments. The current contract-design continuation
does not itself run those experiments or implement provider/tests/helpers.

All three entity groups retain their earlier **conditional exploratory**
approvals. This clarification does not approve a newly revised root/bootstrap/
maintenance contract, close WM-1/WM-2/WM-3 or R5/A09 evidence prerequisites,
provide an independent pass, accept the overall model, or authorize task .3.
Plan 01-02 remains **1/3 tasks complete**, and **1/6 plans** is complete.
Next: concrete revised contract, actual scoped operator preflight, separate
query-policy investigation, fresh independent whole-model review, then final
actual user implementation-contract approval.
