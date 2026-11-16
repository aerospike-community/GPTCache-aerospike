# Aerospike scalar schema guide — incremental draft

## Current SC/MRT and native-vector implementation

This section supersedes conflicting transaction, bootstrap, enumeration, and
vector-scope statements in the historical AP design below. It records the
implementation changes requested on 2026-11-09; it does not change historical
review verdicts or claim released-client qualification.

- The all-node namespace configuration must explicitly identify AP or SC.
  SC operations use the synchronous `aerospike-sdk` transaction context. A whole
  `batch_insert`, including qid/sid reservations, questions, and initial session
  associations, commits or aborts together. Marking, association deletion,
  question purge, report allocation/append, and access-time touch plus session
  retrieval likewise use one MRT per public call. Nested calls join the same
  transaction; transaction sessions are context-local, not shared mutable state.
  Work exceeding server transaction limits fails as a whole, never as separately
  committed chunks.
- SC session append writes the parent and child in the same transaction, with
  `adm` remaining zero. Parent writes serialize append against purge/read;
  transactional point/batch reads validate candidates obtained by nontransactional
  enumeration. Queries are not transactional snapshots and do not predicate-lock
  absent rows. Existing nonzero admissions still require legacy recovery.
- A fresh SC scope creates its root and three allocators in a single MRT after
  checking for conflicting scoped question, session, report, vector, and allocator
  records. Missing counters under an existing root are never backfilled. Root
  `bind` for this automatic path is SHA-256 of UTF-8
  `J(["gptc",1,namespace,cache_scope])`; it is an identity binding, not deployment
  attestation or a vector-index synchronization guarantee. Existing ACTIVE scopes
  retain their boot identity and counters. The AP operator/bootstrap/recovery
  path remains unchanged and does not provide multi-record atomicity.
- Confirmed rollback is reported as `ABORTED_TRANSACTION`; failed/uncertain
  commit or abort is `UNKNOWN_TRANSACTION`, with the cause preserved. Only
  confirmed aborts caused exclusively by `MRT_BLOCKED` or
  `MRT_VERSION_MISMATCH` are retried, with a fresh transaction, jittered backoff,
  at most 32 attempts, and retry admission bounded by the effective SC query
  deadline. Commit/abort statuses are checked explicitly; abandoned or uncertain
  completion is never replayed. Iterable inputs are retained across attempts.
  Per-command acknowledgments in `outcomes`
  are not commit receipts. IDs reserved only inside a successfully aborted MRT
  were never published and can be allocated again.
- Nonpositive signed-int64 neighbor IDs, including Faiss's `-1` sentinel, return
  a scalar miss. Mutations still require positive IDs.
- An omitted `behavior` uses a child of the SDK default with a fifteen-second
  query deadline. Explicit caller policies retain their deadlines and must have
  finite positive query timeouts; the shared SDK default is not mutated.
- Retention preflight accepts either namespace read touching disabled or every
  owned set (`gptc_q`, `gptc_s`, `gptc_r`, `gptc_meta`, `gptc_v`) explicitly
  provisioned with read touching disabled. The server may report the set's `-1`
  sentinel as `4294967295`. With a nonzero namespace default, absent sets or
  inheriting set overrides refuse activation. No namespace/set settings are
  changed by the backend. All-node configuration visibility is still required.
- SC question counts and ID listings project only `fmt`, `scope`, `qid`, and
  `state`, with scope/state predicates on the server. They do not transfer or
  decode answer/embedding payloads and remain non-snapshot enumerations.

The optional `VectorBase("aerospike", ...)` provider implements the existing
vector-store interface. It can share an `AerospikeStorage` via `scalar_storage`
without owning its connection, or own a separately configured connection.

| Record | Set and key | Required bins |
|---|---|---|
| Vector schema | `gptc_meta`, `J(["gptc",1,cache_scope,"vmeta"])` | `fmt=1`, exact `scope`, positive integer `dim` |
| One embedding | `gptc_v`, `J(["gptc",1,cache_scope,"v",qid])` | `fmt=1`, exact `scope`, positive int64 `qid`, positive integer `dim`, native FLOAT32 VECTOR `vec` |

Vector records and schema never expire. A scope's dimension is checked on every
operation before mutation; embeddings must be finite 1-D float32-representable
values, and cosine rejects zero vectors. Batch upsert/delete uses MRT on SC.
Vector keys are externally supplied scalar IDs, not a second allocator.
Deletion is by those IDs, not by truncating sets. There are no collection bins
whose growth is unbounded: one embedding and fixed metadata per vector record.

Search projects server-side `euclidean_squared_distance`, `cosine_similarity`,
or `dot_product`, then applies `order_by`/`top_k` for limits up to 1000. Larger
positive int64 limits use an exact fallback: stream every scoped distance/ID
projection and select the requested nearest results locally, without truncating
to the native Top-K ceiling. This fallback transfers and retains metadata for
all matching records, not their embeddings. Returned
`(distance, id)` tuples are closest first: squared L2, `1-cosine`, or negative dot
product. This is exact brute-force scan-and-rank over the scoped vector set,
not an ANN/HNSW secondary index; embeddings are not downloaded for ranking.
No separate vector service is used. Native VECTOR and Top-K-capable client/server
builds are required; a public artifact is not qualified by these local tests.

The unchanged data manager still calls scalar insert and vector add separately.
Using Aerospike for both does **not** make those two calls one transaction.
The executable example is `scripts/aerospike_quickstart.py`; SC/MRT and native
vector live regressions are in
`tests/integration_tests/manager/test_aerospike_sc_vector.py`.

## 1. Review boundary and input audit

**Current revision: DRAFT_PENDING_FINAL_REVIEW, not an accepted implementation contract.**
Latest clarification records actual conditional “Allow verified constraint” /
“Allow isolated runtimes” and parent adoption after compiled/wire/TTL proof.
§6.1 now binds the SDK-only disabled-configuration route; the native-policy
bridge is superseded. Completed operator preflight including P2 follow-up
supports §14 feasibility. WM-1–4 are addressed at model-contract/feasibility
level, **not an independent closure verdict or final-provider pass**.
This checkpoint overrides older pending-feasibility statements below.
Fresh independent review of this exact guide/derived-summary pair and final
actual user approval remain mandatory before implementation.

**Historical contract-design checkpoint (superseded as above):**
Actual “Approve minimal envelope” / “Allow scoped tooling” are recorded in
clarification §8. New §14 is the proposed concrete bootstrap/history/maintenance
contract responding to WM-1/WM-2/WM-3. It supersedes earlier unconditional
all-time/history/pristineness claims and §12.7's unspecified external-context
construction **only within that explicitly user-approved envelope**. It does
not replace §12's admission algorithm or claim that the new tooling is tested.
The independent whole-model verdict remains BLOCKED; fresh review, actual
operator preflight and final actual user contract acceptance remain required.
WM-4's public-policy construction is resolved at development-evidence level
only (§6.1); released-pair execution and final-provider/live proof remain open.

**Latest actual report checkpoint — recorded 2026-11-05T23:45:08Z UTC**
(recording clock, not a supplied message-event time): user selected
**“Approve report model”**. Group 3 is
**APPROVED_CONDITIONAL_EXPLORATORY_ONLY**, subject to the existing capability/
recovery blockers **BEFORE IMPLEMENTATION**, not overall model approval.
Independent reviewer **aff7f7f7**, inspection 2026-11-05T23:40:41Z,
is **READY FOR STAKEHOLDER WITH QUALIFICATIONS**. Parent accepted G3-C1 LOW
and clarified §13.4: session-purge recovery has no report mutation/deletion;
all-group integrity inventory still includes reports. G3-Q1–Q4 remain binding
qualifications, not closed runtime findings. RA-1/RA-2/R5/A09 are UNRESOLVED.
All three groups now have actual conditional exploratory approval. After the
approved report source revision is committed, a derived **DRAFT_BLOCKED**
summary and whole-contract independent-review handoff are permitted.
Neither derivation nor group approval grants implementation readiness,
independent whole-model acceptance, final stakeholder acceptance or task completion.

**Current checkpoint — recorded 2026-11-05T23:33:39Z (UTC recording time,
not a supplied message-event timestamp):** actual user selected **“Approve
conditional model”** for revised group-1 admission controls and group-2 session
contract. Independent implementer recheck is **READY FOR STAKEHOLDER WITH
QUALIFICATIONS**; actual decision and triage are in the review ledger.
§12 is now **APPROVED_CONDITIONAL_EXPLORATORY_ONLY**, superseding historical
unapproved/pending-review language below. It remains normative and binding.
This permits group-3 drafting, **not implementation readiness**: all HIGH
activation blockers (real exclusion/command settlement, authoritative durable
history/complete enumeration and A09 retention/compatibility evidence) remain
**UNRESOLVED**. Backend-only `recover_session_purge(qids, *, recovery_context)`
is accepted with real proof/refusal obligations, not a shared API change.
Scoped inspection authorization is not capability evidence.

**Historical revision checkpoint — recorded 2026-11-05T23:19:11Z:** actual user
selected **“Allow quiesced recovery”**, authorizing reopening **only group-1
parent controls** alongside group 2. §12 is the authoritative **unapproved
replacement control draft**; it supersedes the unresolved-control wording and
unchanged-parent candidate in §§8/11, not immutable payload/state/timestamps.
Independent review `0595eced` confirmed G2-C1 and conditionally supported the
fixed-size-counter direction; it did not approve this revision. Original
group-1 approval remains historical at `3e0bd4b`. Revised group-1 controls and
group 2 require renewed independent review, then actual stakeholder approval.
Recording time is from this continuation's UTC clock, not a supplied event time.

**Group `cache-payload-identity` is approved for exploratory modeling only.
Not implementation-ready.**
Step 0 is resolved by [actual clarification dispositions](aerospike-clarification.md#6-actual-stakeholder-disposition-log-and-resume-contract).
This guide does not approve itself. Independent implementer findings R1–R5 have
architect triage in [the review ledger](aerospike-model-review.md); actual
independent follow-up `ec5f6dbc` and qualified “Approve group 1” response are
recorded there. Production/capability/full-model gates remain open.
No implementation-ready summary is generated while the activation gates remain open.

Sections 2–10 retain the first-group payload/identity baseline; §11 defines
session shapes and historical failure evidence; §12 is the binding conditionally
approved replacement admission protocol. Section 13 is the conditionally
approved third/final group, report history. The latest checkpoint above
supersedes historical pending-review language; final complete-model acceptance
has not occurred.

Pre-model input audit (checklist §§5.1,5.2,5.4,5.8):

| Input | Evidence / disposition |
|---|---|
| Individual question read; full owned answer/dependency/embedding retrieval | Clarification F01/F03/F04/F11, §3; no independent child mutation/read API |
| Parent lifecycle, state, absence, purge | F04/F05/F09; A04/A05/A08; question payload hidden at state -1 |
| Child count min/p50/p95/p99/max; child bytes min/p50/p95/max; embedding dimensions; projected aggregate | UNKNOWN, expressly accepted A01; no fabricated values |
| Population, scope count, growth horizon, retention duration, ID repetition p95/p99 | UNKNOWN, A01; no implicit TTL, no reuse of allocated IDs |
| Event count/window, contention window, burst factor, update/touch rates, skew, latency/conflict budgets | UNKNOWN, A01; guide defaults are not measured inputs |
| Primary read shape and frequency | Full-set payload structurally required; dominance/frequency UNKNOWN, A01 |
| Cleanup fan-out/latency/lag/retry budgets | UNKNOWN, A01; caller-triggered, visible failures/no worker, A08 |
| Load test; observed busy rate/write p95 | Not run for this draft; observations UNKNOWN |
| Safe values, optional DTO reconstruction | Accepted A07; detailed encoding §4 |
| Supplied/generated timestamps | A06 SQL behavior, not UTC instant preservation |
| Deployment consistency, size, expiry/read-touch, delete guarantees, client/server support | UNKNOWN, A09; activation blocked pending verification, §6 |

No input is FABRICATED. A01 is an exploratory unknown-input disposition, **not**
evidence of safe consolidation, acceptable allocator contention, or production
capacity. This is a provisional baseline draft, not a locked sizing-band verdict.

## 2. Logical versus physical granularity and decisions

Question → ordered answers/dependencies and optional embedding are logical
relationships. Baseline: **one immutable payload per question record**, not one
record per answer/dependency or one growing record per scope. A complete payload
is inserted once and read by numeric ID. No accumulation API mutates its lists.
They can be arbitrarily large at input; capacity overflow is rejected, not hidden.
Touch/mark still rewrite the full physical record even when only one bin changes.

| Decision | Baseline / alternatives and why | Reopen trigger |
|---|---|---|
| G1-D1: full owned payload on question | Pack 1 parent-driven full-set read and same lifecycle. Splitting children adds assembly/ordering/failure paths without a required independent access path. Consolidating multiple questions couples independently deleted/touched payloads and creates growing collections. | Measured payload/touch write amplification, repeated oversize or new subset mutation/read requirement; repeat sizing and compare split |
| G1-D2: typed payload blob | Immutable, always decoded in full; no payload-field SI/CDT mutation path. Typed JSON envelope in bytes avoids unsafe arbitrary-object codecs. Native CDT layout is viable but offers no required server-side payload operation here. | New sub-element access/mutation; codec CPU/size evidence |
| G1-D3: separate fixed-size per-scope question allocator | Shared writers require durable allocation independent of question purge. Embedding allocator into a question couples its lifetime to eviction; a global ID list is unbounded. Dedicated metadata is requirement-driven, not a performance optimization. | Measured hot key/conflicts/latency or scope population/PI pressure; compare reserved ranges/shards without breaking numeric uniqueness |
| G1-D4: positive int64 range allocation, guarded atomic add + returned high water | FAISS requires numeric IDs; process-local counters fail shared writers. UUID strings do not compose. Random numeric IDs with unchecked puts can overwrite; checked-random inserts still need collision retry/reuse policy. Atomic range allocation needs one constant-size coordination record, leaves gaps, never reuses IDs. | Exhaustion, allocator loss, recovery requirements or verified consistency cannot support uniqueness |
| G1-D5: enumerate scoped question set, no secondary index/counter/registry | `get_ids`, count and tombstone discovery need unknown keys; full scans accepted for exploratory scope (A01), no snapshot. A global ID registry grows without bound; materialized count adds coordination with every write/delete. | Measured scan cost violates a newly supplied budget; propose scope/state SI or bounded discovery structure with permissions/cost evidence |
| G1-D6: original naive wall-clock text | A06 explicitly overrides generic epoch/UTC advice. SQL ignores supplied times. LUT is not SQL DTO creation/prior-access time. | Explicit timestamp contract change; never reinterpret stored local text as UTC |

All choices await group review. No comparative benchmark has run. Range
prefetching across calls, payload splitting, index/counter materialization,
compression, sharded allocators and advanced features are
`DEFERRED_OPTIMIZATION`, not adopted without evidence.

Pack 1 consolidation safety form (no fabricated “yes” from absent data):

| Required question | Disposition |
|---|---|
| Can one root hold p99 children within configured thresholds? | UNKNOWN under A01/A09; explicit reject-on-overflow is accepted behavior, not demonstrated p99 fit |
| Does full-set locality benefit the dominant read? | Full payload is required by API; dominance UNKNOWN. One payload read avoids child joins, not a measured latency win |
| Is same-key contention acceptable? | UNKNOWN; no contention band assigned or throughput guarantee |
| Are owned-child deletes simpler/safer? | YES for payload: one conditional question delete; session fence explicitly separate/open |
| Is fallback/migration specified? | YES for failure: reject oversize, surface partial state; structural split/sharding is deferred until measured trigger/budgets and renewed approval |

Pack 5 question-local disposition: caller-triggered synchronous conditional
delete, no child side-effect records or cleanup job set in this group; parent
tombstones are discovery markers. No async threshold, queue, reconciliation SLA
or hidden runner is invented. Full association fan-out/cleanup cannot be locked
until its own group; §8 is the required interface, not a completed cascade model.

## 3. Key and set contract (this group only)

Namespace is the explicit configured `namespace`; logical isolation is exact
`cache_scope` string equality, not authentication. Preserve scope Unicode code
points, whitespace and case; constructor rejects blank/whitespace-only strings.
Namespace separates identical scopes in different namespaces. No database,
tenant or session component is implicitly added.

Define `J(x)` = Python `json.dumps(x, ensure_ascii=True, separators=(",",":"),
allow_nan=False)` with no Unicode normalization, encoded as UTF-8 when bytes are
needed. Array inputs avoid dictionary-order ambiguity. User key is the resulting
**string**, including brackets/quotes/escapes; components are never concatenated
with an ambiguous delimiter or interpolated into AEL.

| Set | Key string (SDK `DataSet.of(namespace,set).id(key)`) | Record lifetime |
|---|---|---|
| `gptc_q` | `J(["gptc",1,cache_scope,"q",qid])`, qid a positive int64 JSON integer | Active/tombstone until explicit successful purge; never TTL |
| `gptc_meta` | `J(["gptc",1,cache_scope,"qid"])` | One fixed-size high water per initialized scope; never purge/reset/TTL |

Example keys: `["gptc",1,"demo","q",1]`, `["gptc",1,"demo","qid"]`.
Set names are fixed in this draft; arbitrary dynamic set-per-scope proliferation
is not introduced. Validate configured namespace and all returned scope/version/
identity bins. Aerospike digest placement does not imply record colocation.
Raw user-key hashing is the database's job; no application hash collision recipe.

Public key-bearing methods normalize `numbers.Integral` values, excluding
bool, to Python `int` after signed-int64 range validation; qid/sid/rid identities
then require `1..MAX`. This includes NumPy integer IDs from host vector paths.
Do not use a Python-int-only public-input check, accept float/string/object
`__int__` coercion, or broaden the safe answer codec allowlist. Preserve SQL
A03 truthy-filter selection first (e.g. key=0 means no key filter), then
normalize a selected key. Physical bins/raw acknowledgments remain exact
int-not-bool, not arbitrary numeric coercions.

Question identity contract: `1 <= qid <= 9223372036854775807` (signed int64; FAISS
sentinel -1 excluded). IDs are unique within `(namespace, cache_scope)` over
intact supported history (§14), not globally across scopes or after unsupported
external history loss. Matching vector/scalar managers must use the
same isolation. No deduplication by question content; identical inputs get
different IDs. Byte repetition cost is symbolic in §7; unknown repetition is
approved under A01, not a quantified small-ID savings claim.

## 4. Bins and safe payload encoding

All bin names are ≤15 ASCII characters. Unless marked optional, absence or wrong
type is corruption. Unknown versions fail, not cache misses. Format `1` governs
key recipe, bin types, payload codec and timestamp grammar; no permissive fallback.

### Set `gptc_q`

| Bin | Physical type / mutability | Purpose and removal impact / anchor |
|---|---|---|
| `fmt` | int64 = 1, immutable | Cross-client/version validation; without it incompatible codecs can be misread |
| `scope` | string, immutable | Exact scan isolation and point-read integrity; without it enumeration cannot filter, F05 |
| `qid` | int64 positive, immutable | Enumeration returns numeric IDs without requiring stored user key; F03/F05 |
| `state` | int64 0 or -1, one-way 0→-1 | Active/deleted visibility and counts/purge, F04/F05 |
| `payload` | blob, immutable | Full typed question/answer/dependency/embedding DTO; F01/F11/A07 |
| `create_on` | string, immutable | Naive insertion wall clock; DTO requires stable creation value, A06 |
| `last_access` | string, changes only on active read | Prior access returned while new time persisted, F04/A06 |
| `afmt` | int64 exactly 1, immutable after initialization | **Conditionally approved §12 control addition**; rejects incompatible admission protocols |
| `adm` | int64 0..MAX_INT64 | **Conditionally approved §12 control addition**; outstanding-admission accounting, not association cardinality |

No bin is merely a relational column copy: control bins are needed for filtering,
identity or atomic access; the repeating logical payload lives in one container.
Version is not a timestamp alias. There are no redundant `updated_at`/expiry bins.

### Set `gptc_meta`

| Bin | Physical type / mutability | Purpose and removal impact / anchor |
|---|---|---|
| `fmt` | int64 = 1, immutable | Reject incompatible allocator formats |
| `scope` | string, immutable | Validate allocator ownership; isolation F03/shared-writer requirement |
| `boot` | canonical UUID string, immutable | **New §14 draft:** must match initialization root for all allocator kinds |
| `hi` | int64 0..MAX_INT64, monotonically increases | Highest reserved question ID, **not count**; removing/resetting risks identity reuse |

Fixed bin count and bounded integer state: no IDs/names/payloads accumulated in
metadata. Separate metadata has an independent never-reset lifetime, satisfying
the bin-extraction lifecycle test. Payload/state/times remain together because
independent retention/access is not required; touch-write asymmetry remains a
known I/O risk, not measured justification for a split.

### Codec 1 (normative draft)

`payload` is UTF-8 bytes of compact JSON object with exactly:
`{"v":1,"question":str,"answers":[[answer_type,node],...],
"deps":[[name,data,dep_type],...],"embedding":null|{"dtype":"<f4","data":base64}}`.
Object member order is not semantic. Duplicate object members are invalid.
Question/name/dependency-data are strings; type tags are signed-int64 integers
(not bool), preserving host image/text tags, not silently coercing them.

Every answer value is a tagged array `node`:

| Python value | Node |
|---|---|
| `None` | `["n"]` |
| bool | `["b",true]` or `["b",false]` |
| signed-int64 int (not bool) | `["i","<canonical base-10 integer>"]` |
| finite Python float | `["f","<float.hex() text>"]`; preserves negative zero |
| str | `["s",value]` |
| bytes | `["y","<standard padded RFC4648 base64>"]` |
| list | `["l",[node,...]]` |
| string-keyed dict | `["d",[[key,node],...]]`, insertion order retained, unique keys |

No implicit tuple/set/datetime/object conversion, pickle/eval/import, cycles,
non-finite float, out-of-range int, non-string dict key or fallback to `str()`.
Unknown tag/arity/encoding is an explicit decode error. Use `ensure_ascii=True`,
`allow_nan=False`, no normalization; JSON escaping preserves code points even
when they are not raw UTF-8 characters. Empty strings, bytes, dicts and lists,
and answer `None`, are valid data, never missing-record signals.
Reject impossible depth/resource requests clearly before avoidable writes; do
not invent an accepted production depth/byte/count bound. Codec resource-limit
policy is a later implementation review item, not permission to truncate.

Answers/dependencies retain positional order, duplicates and exact type tags.
Absent/empty dependencies encode `[]`; reconstruct question as a string if
empty, otherwise `Question(content, ordered QuestionDep list)`, per A07.
Normalize answer input through the host `CacheData` semantics, not by treating a
list-valued answer as several answers after it is already wrapped in `Answer`.

Embedding is absent only when JSON `null` (return `None`). Otherwise encode
contiguous 1-D ordered float32 values as little-endian IEEE754 bytes, standard
base64; empty-present has empty data and returns an empty float32 array, not
`None`. Decode byte length must be divisible by 4; preserve float32 value order,
not ndarray strides, source dtype or arbitrary dimensions. Unsupported shape
fails explicitly. Do not use a vector-index/server vector capability for this
scalar blob. A07's finite-float restriction is for answer values; embedding
float32 bit patterns need not be filtered as though they were answers.

Timestamp grammar: `YYYY-MM-DDTHH:MM:SS.ffffff`, exactly 26 ASCII characters,
valid Python datetime calendar/time components, **no zone/offset/Z**. Encode
`datetime.now().isoformat(timespec="microseconds")`, decode to naive datetime.
Generate creation/access separately at insert; ignore both supplied DTO times.
Touch samples naive local now at request construction; it can be earlier than a
concurrent touch applied before it. No instant/monotonic ordering is promised.

### Per-set JSON examples

These are **synthetic shape examples only**, not measurements or workload bounds.
JSON cannot hold blob bytes: `"$blob_utf8"` below is display notation; actual
`payload` is bytes of the object shown, not a map or a nested `$blob_utf8` value.

```json
{
  "namespace": "<configured>",
  "set": "gptc_q",
  "key": "[\"gptc\",1,\"demo\",\"q\",1]",
  "bins": {
    "fmt": 1, "scope": "demo", "qid": 1, "state": 0, "afmt": 1, "adm": 0,
    "payload": {"$blob_utf8": {
      "v": 1, "question": "café",
      "answers": [[0, ["d", [["ok", ["b", true]], ["raw", ["y", "AA=="]]]]]],
      "deps": [], "embedding": {"dtype": "<f4", "data": "AACAPwAAAMA="}
    }},
    "create_on": "2026-01-02T03:04:05.123456",
    "last_access": "2026-01-02T03:04:05.123457"
  },
  "expiry": "explicit never-expire"
}
```

```json
{
  "namespace": "<configured>",
  "set": "gptc_meta",
  "key": "[\"gptc\",1,\"demo\",\"qid\"]",
  "bins": {"fmt": 1, "scope": "demo", "boot": "00000000-0000-4000-8000-000000000001", "hi": 1},
  "expiry": "explicit never-expire"
}
```

## 5. Access matrix and atomic operation contract

All SDK result streams must be drained, including errors raised after a prefix.
Check `RecordResult.is_ok`, embedded `exception`, `result_code`, `in_doubt`,
key/index correlation and expected row presence, not merely “execute returned”.
Empty/omitted/error rows are not success. No payload/secret is logged.
The API names below are inspected builder surfaces, not legacy client policies.
Static API inspection does not verify deployed server support.

**Keyed result contract (applies to every keyed call below):** use
`.include_missing_keys().fail_on_filtered_out().execute(on_error=ErrorStrategy.IN_STREAM)`
on the builder, including allocator reads/updates, create-only inserts,
read/touch, marks, guarded purge and diagnostic reads. Table `.execute()`
spellings are abbreviated by this rule, not permission to use defaults.
These flags and `ErrorStrategy` are actual SDK surfaces; IN_STREAM alone does
not force missing/filtered visibility. Drain and correlate each result to the
requested key/input (one row for a point call), retaining code, exception and
in-doubt information. Missing/duplicate/unexpected result rows or an interrupted
stream are explicit incomplete-outcome errors, never absence or success.
Execution/iteration can still raise outside the per-key result path: propagate
that failure and retain any known outcomes; do not fabricate the unobserved
suffix. Error diagnostics exclude payload/bin values.

| Access | Concrete path, read/write work | Outcome/failure boundary |
|---|---|---|
| `create` | **§14 supersedes initial bootstrap:** read ACTIVE root plus qid/sid/rid allocators and validate exact identity/types/boot; validation only | Explicit provider-only init/resume precedes manager construction; no ordinary create zero-fill, even on empty scope; missing/incompatible history fails |
| Allocate n for nonempty `batch_insert` | Prevalidate all DTOs; after §14 activation `session.update(meta_key).where(predicate).add("hi",n).get("hi").never_expire().with_txn(None).fail_on_filtered_out().execute()`; predicate scope/fmt/boot, valid hi >=0 and hi <= MAX_INT64-n | One atomic add/read under record lock returns H; IDs H-n+1..H. n>MAX rejected without arithmetic overflow; zero batch returns [] without allocation |
| Payload insert | Chained distinct-key segments: `session.insert(k0).put(b0).never_expire().with_txn(None).insert(k1).put(b1).never_expire()...execute()`; each segment maps its own DTO bins | Create-only prevents overwrite on allocator corruption/stale reservation. Chaining finalizes each segment; don't apply one put mapping to a heterogeneous key list |
| `get_data_by_id(qid)` | One update-only active-filtered operate on q key: ordered `.get("fmt").get("scope").get("qid").get("state").get("payload").get("create_on").get("last_access")` **before** `.put({"last_access":new_local_text})`, explicit never-expire/with_txn(None), effective max_retries=0 | One locked read-before-write returns prior access/payload; decode/validate; then session projection interface §8. Touch or association-read error propagates, even if touch persisted; never replay to recover an old time |
| `mark_deleted(keys)` | Deduplicate numeric keys, batch update-only `state=-1` with scope/fmt and valid state predicate; explicit never-expire | Missing is no-op; -1 replay safe; cannot create. Per-key errors/in-doubt visible; never replace payload |
| `get_ids(deleted)` | `session.query(DataSet.of(namespace,"gptc_q")).where(typed_scope_equality).bins(["fmt","scope","qid","state","afmt","adm"]).with_txn(None).execute()`; §6.1 SDK-only behavior/config required; validate every row's fields and physical identity, then use first validated observation per key (§5 below) | Complete distinct observed scoped ID list, no truncation/sort promise. True→-1, False→0; no duplicate IDs |
| `count(state,is_all)` | Same validated scoped scan and first-observation rule; count distinct observed question keys matching state, or either valid state for is_all | Excludes metadata/other groups. Never count deliveries; O(question set), no cached count or snapshot |
| `clear_deleted_data` | Scoped tombstone discovery; for each qid, invoke owned-association cleanup fence (§8), then conditional delete of q key only while scope/fmt/state=-1 | Failure keeps/reports tombstone and partial work. Missing after concurrent purge is safe; active is never removed; full cross-group purge proof deferred |
| `flush`, `close` | No buffer: acknowledged writes only; flush may be no-op, close lifecycle-owned SDK resources | No-op does not convert uncertain writes to success; repeated close safe, operations after close fail |

Payload batches require distinct bin maps per key. `insert(next_key)` finalizes
the previous segment and clears its operations, predicate, generation and TTL;
therefore explicitly repeat `.never_expire()` on **every** segment and predicates
on each relevant update/delete. `with_txn(None)` opts out for the shared builder.
Correlate result index and key to flattened segment/input order, never arrival
order. No speed or all-record-atomicity claim is made.

For allocation add/get, use raw `operation_result(1)` for returned H (slot 0 is
the write); assert int-not-bool/range/non-None. For read/touch, old access is raw
slot 6, new put slot 7 is not read as the old time. Avoid typed numeric helpers
that coerce None→0 or bool→int. Missing mandatory operation results are errors.

Predicates are typed expressions composed with SDK `Exp` (exact scope string,
fmt int, qid int/state), not interpolated raw user strings. Dataset query
`.where(typed_expr)` selects the common public typed-filter path with
`allow_scans_with_where=True`, not string AEL/SI selection. Released SDK has
no `.filter_expression`; do not call it. Indexes: **none in this group**. Scan
projection saves network, not device I/O. Validate state before local scan
selection so corrupt states are errors, not silent omissions. No scope/state SI without measured
access-path need and capability/readiness proof. Read scans validate versions;
do not filter away unknown versions and call them zero records.

### Point outcomes and diagnostics

Point read predicate includes typed fmt/scope/qid/state=0. Classify a correlated
row before accessing operation slots:

| Observed outcome | Required action |
|---|---|
| `is_ok` with required record/results | Validate identity/control fields and full payload/time codec; only then reconstruct DTO from captured slots and fetch §8 projection. Decode/validation failure raises even if access-time write persisted |
| Explicit `KEY_NOT_FOUND_ERROR`, no in-doubt or conflicting failure | Get → `None`; mark/purge → replay-safe no-op. Allocator missing on reopen → recovery error; only authorized pristine create may initialize |
| Explicit `FILTERED_OUT`, no in-doubt or conflicting failure | Never a miss/no-op by itself. Perform the diagnostic inspection below, preserving original keyed outcome |
| Other non-OK, embedded exception even with OK code, in-doubt, invalid/missing row/results, execution/iteration failure | Surface keyed failure/uncertainty; no DTO, `None`, empty success list or inferred write success. Do not inspect away a timeout/in-doubt into absence |

The expected SDK typed not-found/filtered exception accompanying its matching
non-OK code is classification evidence, not an independent transport failure.
Conflicting exception/code or in-doubt always fails closed.

Diagnostic inspection is a **read-only, unfiltered query of the exact requested
physical key**, with the keyed result contract above, not a dataset lookup or a
scope/version/state predicate that could hide corruption again. Validate the
returned key and full record (question fmt/scope/qid/state, mandatory times and
payload codec; metadata fmt/scope/boot/hi and §14 root). This is a later observation, not a
snapshot of the rejected operation; diagnostic failure propagates.

- Filtered get: validated tombstone or explicit diagnostic not-found → `None`;
  malformed/unknown-version/wrong scope or identity → corruption error;
  valid still-active → explicit race/predicate-rejection error, **no touch replay**.
- Filtered mark: validated tombstone or explicit diagnostic not-found →
  replay-safe no-op; valid active → explicit rejection/race error, not success.
- Filtered purge: explicit diagnostic not-found → replay-safe no-op; validated
  active → guarded non-deletion (not purged, surface the unexpected state);
  validated tombstone → explicit rejection/race error, not deletion success.
- Filtered allocator: valid hi above MAX_INT64-n → explicit exhaustion; valid
  hi still permitting n → rejection/race error; absent/corrupt → recovery error.
  A racing pristine create-only `KEY_EXISTS_ERROR` only permits a read/validation
  of the winner, never resetting hi. Payload create-only collision is an error.

These diagnostic observations can change under concurrent mark/purge. They do
not prove the original rejected state, repair a corrupt row, or reconstruct a
lost acknowledgment. Every other failure remains visible and correlated.

### Distinct observed scan identities, not a snapshot

For `get_ids`, `count` (including `is_all`) and purge discovery, drain the full
scoped dataset stream and validate **every** delivered row, including repeats,
before state selection/deduplication. Require usable returned key metadata:
`key.namespace == configured namespace`, `key.set_name == "gptc_q"`, exact
`scope == cache_scope`, fmt=1, positive int-not-bool qid in int64 range and
state int-not-bool in {0,-1}. Rebuild
`expected = DataSet.of(namespace,"gptc_q").id(J(["gptc",1,cache_scope,"q",qid]))`;
require returned digest equals `expected.digest` as well as matching namespace
and set. This binds the logical qid/scope to physical identity without requiring
stored user keys. Absent/unusable metadata, synthetic fallback keys, digest/
namespace/set mismatch, malformed bins or row/stream errors fail the call;
never use an advertised qid to redirect cleanup at another physical record.
The scope filter excludes other scopes; no other set/metadata is queried.

Deduplicate validated `(namespace,set_name,digest)` identities **before**
state selection: the first validated observation's state within this call wins.
Later valid observations of that same key may have changed state, but neither
replace that selection nor add another count/ID/candidate; they are still
validated so a corrupt repeat cannot be hidden. Thus count counts questions,
not scan deliveries, and `is_all` counts each observed key once. Purge uses
only distinct first-observed tombstones as candidates and still rechecks its
delete predicate after the §8 fence. No prefix is returned if iteration fails.

This is complete processing of delivered observations, **not** a cross-record
or end-of-call snapshot. Concurrent insert/mark/purge and migration can change
which keys/states are observed; an active-first/deleted-later key is active for
that call and not a purge candidate until rediscovered. Separate enumeration/
count calls need not agree or partition one common population. No claim that
all tombstones existing at completion were removed under concurrent mutation.
Quiescent successful calls must enumerate/count each matching question once.

### Strict previous-access return and no replay

The mixed read/touch must use effective **max_retries=0**, not merely an SDK
default, for its WRITE_NON_RETRYABLE/POINT policy and any applicable mode/
override/reload. A first attempt may commit while its reply is lost; repeating
would return this logical call's own new timestamp rather than prior access.
On timeout/lost reply/in-doubt or missing required slots, surface uncertainty
without retrying the operate, fabricating an old time, or returning a diagnostic
read as the original DTO. A caller's later fresh call is a new access, not
recovery of this call. Successful DTO access time comes only from raw slot 6 of
the acknowledged original attempt. Read-only diagnostics have their separate
read retry policy; no diagnostic is a replay of the touch.

Allocation is not a get/edit/put or generation-as-counter scheme. Atomic
guarded add checks overflow before mutation. No decrement, rewind or ID
reclamation, including failed batches. Confirm returned high water's type and
range. Unknown allocation outcome: no payload writes without a known reservation;
surface unknown reservation and abandon any possibly reserved range. Do not
resubmit an uncertain increment. A create-only collision is an integrity error,
never permission to overwrite or blindly try the same reservation elsewhere.

Known payload successes may remain on any failed insert batch; raise with
input-index→allocated-ID→known-success/failure/unknown correlation as available.
Rows never reported by an interrupted stream remain unknown/not attempted as
the SDK can distinguish; don't invent success. No automatic vector rollback.
Initial session failure can leave an active payload plus missing/unknown
association; report it, do not return a success ID list. Final batch contract
waits for the session group, not an invented cross-record transaction.

## 6. Capability, policy, retention and recovery gates

**No namespace mode or deployment capability is selected or presumed verified.**
Before activation, verify single-record conditional add/read, create-only,
update-only, typed filter, mixed read-before-write, dataset enumeration and
per-key results against actual supported client/server and configured namespace.
These primitives are statically SDK-surfaced, not live-validated here.
If any prerequisite cannot be verified, keep activation blocked and revise.

Shared-writer uniqueness assumes one authoritative record history. AP
split-brain/lost durable allocator state can violate permanent uniqueness even
with atomic adds/create-only inserts. Actual namespace consistency, replica
commit/read modes, partition behavior, persistence/backup/restore and operational
acceptance are A09 verification gates, **not a claim that SC is deployed** or a
capability-dependent design silently selected. Do not certify multiprocess
uniqueness under partitions until the chosen environment proves the required
semantics. Normal concurrent writers serialize at the same metadata key.

Policy intent, all paths and record kinds in this group:

- Creates, allocator increments, touches and marks explicitly call
  `.never_expire()` (TTL -1), never inherited namespace TTL/0.
- On applicable point/batch **read** policy surfaces require effective
  `Settings(read_touch_ttl_percent=-1)` and assert the mapped read-policy value.
  The inspected SDK forwards it for point/batch reads, **not** in
  `to_query_policy()`. Setting `reads_query` therefore is not wire-policy proof.
  Queries must use the SDK-only disabled-configuration route in §6.1; compiled
  released/development request/finite-TTL evidence exists, but released-server and final
  provider retention remain A09 activation gates. Do not infer query behavior
  from point read settings. Mixed read/touch is a write-policy operation with explicit
  never-expire; access-time bin mutation is not SDK TTL `touch`.
- Derive provider-owned effective `Behavior` from the connection session;
  verify final point/batch/query and retryable/nonretryable settings after
  overrides/config reload. Allocation/inserts and mixed read/touch get
  **max_retries=0**, including the actual WRITE_NON_RETRYABLE/POINT policy for
  touch; defaults alone do not satisfy this. No ambient
  implicit batch transactions (`with_txn(None)`), no hidden non-idempotent retry.
  Deterministic marks/deletes remain safely repeatable by caller.
- Deletion durability/SC restrictions and cold-restart resurrection need actual
  capability review. Do not unconditionally enable an edition-gated durable
  delete or claim its semantics without verification. Purge operational
  acceptance stays blocked until removal semantics are verified.
- Verify actual configured maximum record size, set/default expiry/read-touch,
  permissions and format compatibility. Inspect void-time after create/touch/
  mark/allocator update under finite namespace defaults in later authorized
  live validation; a short sleep is not proof.

Metadata must be backed up/restored consistently with all references and never
deleted by cache eviction. Missing allocator/corrupt hi on non-pristine scope
fails closed. Recovery requires offline/quiesced all-group inventory including
external vector/reference state; never rebuild hi merely from surviving
questions (purged IDs may still be referenced). No automatic recovery worker,
TTL safety valve or hidden allocator reset.

### 6.1 Binding SDK-only query/configuration contract (WM-4)

**Parent evidenced adoption, 2026-11-06:** the actual conditional user decision
in clarification's latest entry is satisfied at compiled-client/wire/TTL
capability level. The earlier native-policy bridge is **superseded**, not a
fallback. Released SDK lacks both `.with_policy` and `.filter_expression`.
Use public SDK Behavior/Settings and builders only: no PAC, native policy type,
private SDK mapper, underlying client CRUD or SDK patch.

**Required configuration, checked before write activation:** namespace
`default-read-touch-ttl-pct=0` exactly; relevant existing `gptc_q/gptc_s/gptc_r/
gptc_meta` set `default-read-touch-ttl-pct` overrides must be 0 or -1, never
positive. Absent sets inherit namespace 0 on creation. Use the public session:

```python
info = session.info()
namespace_by_node = info.info_on_all_nodes(
    "get-config:context=namespace;id=" + namespace
)
sets_by_node = info.info_on_all_nodes("sets/" + namespace)
```

Validate the configured namespace as an info-command component (reject command
delimiters/control characters, never interpolate arbitrary commands). Bind exact
namespace, applicable active-node membership/topology and both response node
sets, not merely one successful seed. Require complete nonempty successful
per-node replies, exact setting names/values and valid set entries. Missing
permission/node/field, malformed/ambiguous/duplicate/conflicting entries, mixed
node config or relevant positive/unsafe override is an actionable error **before
any root/allocator/data write or activation**. No namespace/set mutation, boolean
proof bypass or silent operator-proof fallback. Refuse if all-node coverage
cannot be established through the supported public SDK/deployment inventory.

Revalidate at provider create/activation, explicit operator init/resume/recovery
entry after each new clean incarnation, and finish/public reopen before launch
gate release. Changes to supported namespace/set/topology or effective caller
configuration require stopping compliant use and explicit revalidation before
resumption. Trusted operators maintain pinned supported configuration between
these boundaries. No automatic drift detector, continuous polling, atomic config
lease or malicious-admin resistance is claimed. A known unsafe/corrupt setting
or history always fails closed; this is not a config-drift safety guarantee.

Derive a provider child of the **effective caller Behavior**, not DEFAULT:

```python
candidate = caller.derive_with_changes(
    unique_name,
    reads=Settings(read_touch_ttl_percent=-1),
    reads_query=Settings(allow_scans_with_where=True, max_retries=0),
    writes=Settings(max_retries=0),
)
session = cluster.create_session(candidate)
rows = (session.query(dataset).where(combined_typed_expr)
        .bins(physical_bin_names).with_txn(None)
        .execute(on_error=ErrorStrategy.IN_STREAM))
```

Use public SDK `Exp`, `Settings`, `ErrorStrategy`; one combined **typed**
scope/type predicate, no string-AEL/SI reinterpretation. Validate state locally
before first-observation selection (§5); do not hide corruption by version/state
filtering. No restricted partitions, limits, hidden virtual projections or
pagination truncation for complete inventories. Physical projections are lists.
Point/batch reads send no-touch -1; every modifying builder segment explicitly
never-expires (-1), including metadata, touch, admission, mark and delete paths.
Query mapper sends native wire 0: this route defers to the verified disabled
namespace/set contract, not a claim Settings sends query -1.

Validate public `Behavior.get_settings` across actual READ/QUERY, point/batch
read and WRITE/WRITE_NON_RETRYABLE cells in applicable AP/SC modes after
overrides/config reload. Preserve every unrelated caller field: AP/SC consistency,
replica, total/socket deadlines, retry delay, compression/threshold, node/queue
settings, verbosity and release `timeout_delay`. Require zero effective modifying
retries and explicit transaction opt-out (including implicit batch transactions).
No field-dropping native-policy reconstruction. Reject incompatible/missing public
surfaces, dependency/version/provenance or settings; lazy-load only the selected
SDK and validate its declared installed transport, never choose whichever native
module happens to import or substitute a legacy client. No native type import is
needed for the data path.

Require a finite positive effective query total deadline; validate unsigned
32-bit milliseconds, reject negative/overflow/sub-ms-to-zero values. An explicit
provider bound may tighten, not silently lengthen a caller bound; absent/unbounded
deadlines require explicit finite configuration. Query retry 0 is **not zero
partition retries**. Fully drain, validate every physical/logical identity before
nonsnapshot dedup, and expose row and terminal iteration errors. A prefix is
never a complete count/inventory/purge.

Completed compiled released and development client proof observed all 4096
partitions, exact ids 1/2/3, nine q bins, bytes and namespace/set/digest identity,
normal terminal completion and literal wire query0/point-1/writeTTL-1. Foreign
finite controls did not extend with candidate/default0 reads; positive +50
extended. All nine owned schema-shaped markers survived >14.27 seconds against
test-only namespace TTL12 with ordinary point `ttl=None`. Release computed
void-time reads correctly mapped -1; development measurements avoided the old
read-only-operate WritePolicy0 trap. Production metadata reads use ordinary SDK
read no-touch -1/TTLNone, **not native-client measurement instrumentation**.
Config text also distinguishes query behavior; this proof does not establish
namespace0 as the causal explanation for every query's no-touch behavior.

WM-4's model/API feasibility is addressed, not a final provider/release-server
qualification. The released client ran against a **development** server below
published minimum 8.2.0+; actual supported released-server group04 and final
provider all-entity retention/error tests remain required. No compatibility,
SC-live, timeout fault-injection or final acceptance is inferred from this sample.

## 7. Symbolic sizing, contention and growth worksheets

All sizes here are logical/uncompressed. `L` = actual configured record maximum
(UNKNOWN); `T` = approved operational size target (UNKNOWN); overhead terms
require actual encoding/storage measurements. No capacity success asserted.

Let `Q` be encoded question bytes, `A_i` tagged answer encoded bytes, `D_j`
dependency tuple bytes, `d` embedding length, `S` scope bytes, `K` key bytes,
`H` record/bin/codec framing overhead. Base64 embedding bytes =
`4*ceil((4*d)/3)`; recursive native bytes have base64/JSON/tag overhead too.

| Worksheet | Formula / status |
|---|---|
| Question record | `R_q = H_q + K_q + S + Q + sum(A_i) + sum(D_j) + embedding_envelope + 2*26 + integer/control overhead`; compute from actual encoded payload; p50/p95/p99/max UNKNOWN |
| Payload fit | Must satisfy actual uncompressed encoded record ≤ L; client blob length alone is not full record size. No transparent split/chunk/truncate; server too-big → explicit failure, allocated IDs may be gaps |
| Above guide ~50 KiB review point | No observed distribution; large payload + read-touch rewrite rate must be measured. This is a review trigger, not an invented application cap or automatic split |
| Metadata | `R_m = H_m + K_m + S + fmt/hi bins`; fixed number of bins, not necessarily small if scope/key large; UNKNOWN byte total |
| PI cost | For `Nq` questions + `Ns` initialized scopes: `64*(Nq+Ns)*replicas` bytes; payload-only PI ratio `64/R_q`, metadata `64/R_m`; all populations/ratios UNKNOWN |
| Split alternative | `64*(Nq+sum(answer_count)+sum(dep_count)+embedding_records+Ns)*replicas`; adds key/assembly/write costs without independent child API |
| Rewrite cost | Approximately `R_q*(touches+marks+inserts)` plus storage/replica amplification; allocator `R_m*allocation_calls`; rates UNKNOWN |
| ID repetition | Per occurrence int64 bin = 8 value bytes plus storage framing; key decimal component 1..19 digits; number of future references p95/p99 UNKNOWN |
| Allocator contention | `avg_qps=events_per_scope_window/active_window_sec`; `burst_qps=avg_qps*burst_factor`; `lambda=burst_qps*contention_window_ms/1000`; `P(>=2)=1-exp(-lambda)*(1+lambda)`; every input/result UNKNOWN |
| Question contention | Same worksheet per qid using touches/marks; observed busy rate, write p95, growth horizon, SLO UNKNOWN; no band A/B/C awarded |
| Cleanup | Question-local children require one record delete, no child scan. Future association fan-out UNKNOWN; no cleanup lag/SLA claimed |

Payload lists have **immutable input-sized** growth, not append growth; their
physical ceiling is verified `L`, with explicit rejection at overflow.
Aggregate question population grows without TTL; scope metadata is permanent.
No bounded production retention horizon is implied by host eviction. Set
enumeration O(total questions across scopes), output memory O(scope results);
caller needs real budgets before production use.

Hot-key/overflow playbook: record operation latency, busy/error rates and logical
record sizes using synthetic or redacted data. Exhaustion at MAX_INT64 is a hard
error before add, not wraparound. Oversize rejects the affected insert; any
committed subset remains visible through failure correlation. Reopen D1/D3/D5
when actual size/write/scan/conflict budgets are supplied and violated; choose
numeric promotion thresholds then, not invented thresholds now. No background
sync→async promotion because A08 explicitly authorizes no hidden worker.

## 8. Cross-group interfaces only — not next-group physical design

Future groups must accept `(namespace, exact cache_scope, qid)` with positive
int64 identity and one-way parent state. No association/report physical schema
or allocation field is selected here.

1. **Association projection:** given scoped qid, return complete ordered-as-stored
   session-name rows compatible with one-column SQL rows (index/unpack/
   `.session_id`), retaining duplicates; no implicit sort. Get reads no
   cross-record snapshot. Failure is visible, not an empty invented projection.
2. **Initial association:** only truthy input session name; failures correlate
   to question ID/input. Later `list_sessions` must honor A03 name-precedence/
   truthiness, not intersection; association DTO is not a name row.
3. **Admission and purge fence:** missing/deleted parent rejects additions (A04).
   Purge must prevent late in-flight admission from leaving a successfully
   purged parent's association behind. Keep parent tombstone discoverable until
   owned-association cleanup and admission fencing complete; retry is safe.
   The session group's mechanism/proof is **open**; a pre-insert parent check
   alone is insufficient. No durable-delete/transaction capability assumed.
4. **Reports:** numeric references may survive parent purge, A05; report
   independence/retention does not reset question identity.
5. **Pristine initialization/recovery:** “scope empty” checks all persistent
   record kinds, never only `gptc_q`. Reopening missing metadata is a recovery
   error. Final initialization protocol must include later-group records.

These are compatibility obligations for later design, not evidence that a
cross-group operation already works. Full purge/batch acceptance stays open
until those groups resolve the interfaces.

## 9. Developer walkthroughs (architect-only, not live tests)

### W1 — create, typed insert and read/touch

Synthetic scope `demo`, pristine namespace/scope verified at activation. Two
initializers race to create meta key `["gptc",1,"demo","qid"]` hi=0; one succeeds,
the other validates exists without overwrite. For one DTO, prevalidate full
allowlist and embedding before guarded add n=1. Returned H=1 determines qid=1
and exact q key in §3. Create-only insert stores immutable payload and separately
generated naive times, ignoring supplied DTO dates. Atomic active read gets old
time/payload before writing new local time; decode reconstructs exact native
types and optional float32. No associations means projection `[]`, not `None`.
Filtered tombstone is explicitly emitted then inspected; missing is explicitly
correlated; corrupt/failed inspection raises. Lost touch reply surfaces doubt
with zero retry, never returns this call's newly written time.
**Revised architect structural trace passes**; activation and future projection proofs remain
explicit gates. Normal engineering: per-row decode/correlation/error wrapping.

### W2 — independent processes, reservation and failed batch

Synthetic hi=10; process P reserves n=2, process Q n=1. Atomic guarded
add/get returns either P=12 then Q=13, or Q=11 then P=13; reserved ranges
never overlap in an authoritative record history. Each process binds its
own DTOs to its range in input order. If P first payload commits and next fails,
raise with input/ID outcomes; don't return a truncated success list, decrement
hi or overwrite Q. Unknown allocation acknowledgment means no payload insert
using a guessed range. Retry is a new caller decision, may duplicate content,
never reuse identity. **Structural trace passes for normal shared writers**;
Heterogeneous batch API binding was independently source-reviewed; this revision
awaits recheck. Partition/durability and executed batch behavior remain live
verification gates, not certified as tested.

### W3 — mark/get/purge race and interruption

Existing qid=1 active. Mark uses update-only state=-1; a simultaneous atomic
read either precedes mark (old DTO/time returned) or is filtered and verifies
tombstone (returns None). Missing mark cannot create a ghost. Purge discovers
tombstone via scoped query, invokes §8 cleanup fence then conditional delete.
If cleanup fails, tombstone remains discoverable; replay resumes and never
touches active/other-scope records or allocator hi. After successful deletion
missing reads return None; decoding corruption/transport errors never do.
Duplicate scan deliveries use first validated state per physical key for both
IDs/counts; a mismatched qid/digest raises before any cleanup candidate is used.
Concurrent state observations are not a shared snapshot; delete still rechecks.
**Question-local trace passes; cross-group cascade is explicitly NOT closed**.
The session group must prove the fence, including an add that observed active
before mark but finishes after cleanup; no pretense of an atomic global purge.

## 10. Failure-mode self-check, validation and migration

| Guide detect test | Draft result / remaining work |
|---|---|
| One set per domain noun | No: owned payload co-located; metadata split justified by durable independent lifetime |
| SI as hot read mechanism | None; ID get/touch by key. Enumeration scan cost accepted exploratory-only |
| Client whole-collection read/modify/write | None for immutable payload; hi atomic add; state/touch server operations; future association mutation not designed |
| Bins scaling with user data | No: fixed bin count; repeating values are safely encoded container |
| Extra record just to assemble payload | None for owned payload. Session projection is a real cross-group read, not hidden success/cost |
| Unbounded collection registry | None; payload immutable but not known to fit; verify L/reject oversize; metadata fixed hi |
| Small-entity PI cost ignored | Explicit Ns/R_m worksheet; unknown byte/population inputs not silently accepted as efficient |
| Checklist run as monolithic template | Only first group drafted; all later physical patterns absent; independent revision recheck + stakeholder pause pending |
| False missing/TTL/scope success | Decode/auth/filter ambiguity fail; explicit no-expiry policies, effective config/live checks still pending |

Required validation **not executed in this modeling pass**: source-shaped SDK
builder assertions (ordered gets before touch, guarded add, per-key bin binding,
visible missing/filtered keyed results, unfiltered diagnostic classification,
implicit transaction opt-out and final zero-retry touch policies across mode/
override/reload); lost-reply touch without replay; typed codec fixtures for every
allowlisted type/empty/native nested/Unicode/base64/float32; corruption/version/
missing vs filtered errors; two-process ID allocation; timeout/in-doubt/partial
stream outcomes; numeric exhaustion; too-big with real configured limit; scan
isolation/completeness, repeated same/different-state first-observation counts
and IDs including is_all, corrupt repeated rows and absent/mismatched scan
namespace/set/digest/qid metadata; point/batch read-touch mapping plus §6.1's
released-pair/final-provider query transport/void-time validation (development
construction proof is not that gate); no-expiry under finite defaults; cold restart/partition/
restore behavior; later association cleanup fence. No zero-test or mocked
“live pass” substitutes. Numeric production thresholds remain unavailable.

No path expressions, expression indexes, vector type/index, UDF, multi-record
transactions, compression, set-index requirement or deployment-specific
durable-delete choice is needed/selected. Any future use requires fresh
capability and operational verification under A09.

Version migration: unknown fmt fails closed; do not silently reinterpret
timestamp timezone, tagged codec, key or hi. Introduce a new explicit format,
quiesce old writers, inventory all groups/vector references and migrate with
validated ID preservation. Do not dual-write allocators that can allocate the
same numeric space independently. No released existing schema requires online
backfill today; this draft is not migration approval. Reopen on any key/bin/type/
time/retention change; regenerate summary only after complete guide approval.

**First-group gate update:** independent follow-up `ec5f6dbc` found this group
ready with qualifications; actual “Approve group 1” was recorded at
2026-11-05T18:33:44Z. Historical pending-gate wording in §§2/9/10 describes the
draft/revision process, superseded only for this group's review status by the
actual decision in the review ledger. No contract, live-test outcome or
cross-group proof is changed. Session modeling may now start.

## 11. Group 2 — session associations: bounded-scope draft, NOT APPROVED

**Original candidate status: `REVISION_CONFLICT`; replacement status:
`DRAFT_AWAITING_INDEPENDENT_REVIEW` in §12.**
Group 1's actual exploratory approval permits this group-specific analysis.
The original group-1 contract in §§1–10 is preserved except for explicitly
marked **unapproved** control additions in §4 and §12's replacement guards.
The candidate association layout below is **not a deployable model**. The
unchanged-parent variant failed add-versus-purge; §12 now drafts the authorized
parent-control revision. No read/touch ordering, payload/state/time change is
silently introduced; no revised implementer/stakeholder pass is asserted.
Reports (group 3) have **no physical schema yet**.

### 11.1 Group clarification and decision-pack input disposition

| Required input | Confirmed value / disposition |
|---|---|
| Entities, ownership, traversal | Association instance owns its ID, scoped qid, exact session name and original question text; question owns its purge lifetime. Name is a value, not a new session-account entity. Both question→associations and name→associations plus all-associations reads required, F06–F09 |
| Query direction dominance/frequency, full-set/subset shape | Both directions required; complete filtered/unfiltered list API, no paging/truncation. Relative dominance/frequency UNKNOWN, approved A01 |
| Edge attributes and mutations | `.id`, `.question_id`, `.session_id`, `.session_question`; immutable after append; independent association-ID deletion. No required association creation/update timestamp or metadata update API |
| Uniqueness/idempotency | A02: repeated identical `(qid,name,question)` calls are distinct independently deletable associations. No pair uniqueness or deduplication by content; appends are not idempotent |
| Degree and size distributions | Both-side min/p50/p95/p99/max degrees, scope/name/question bytes, edge bytes, aggregate bytes, ID repetition UNKNOWN, A01; no accepted fan-out ceiling |
| Shared sizing/growth | Population, horizons, retention duration, target size, configured maximum, projected p99 record size UNKNOWN, A01/A09; all records have no expiry |
| Shared contention | Events/root/active window, window seconds, contention window, burst, skew, churn, create/delete/update rates, p95 latency and conflict budgets UNKNOWN, A01. No numeric guide default is a workload observation |
| Measured evidence | No group-2 load test; observed busy/error rates and write p95 UNKNOWN. No sizing/contention band or fit verdict awarded |
| Lifecycle/visibility | A04: missing/deleted parent rejects admission, existing associations remain listable through soft deletion, physical purge removes owned associations before success. Name drop removes only IDs observed, not future admissions |
| Cleanup inputs | Fan-out, latency/lag budgets, batch budget, retry/escalation/alert thresholds UNKNOWN, A01/A08. Caller invokes cleanup; no runner, bounded lag, implicit queue or asynchronous promotion authorized |
| Errors and recovery | A08: known/failed/unknown outcomes visible and correlated; no blind non-idempotent replay. Repeating ID deletions/purge must be safe. Replacement admission/recovery draft in §12 is not approved; no permission for orphan success |
| Capabilities | A09: consistency/history, retention/void-times, limits, delete durability, transactions/index permissions and released compatibility UNVERIFIED; source surfaces alone do not establish deployment support |
| Newly resolved requirement | Actual “Allow quiesced recovery” permits the exceptional operational prerequisite and reopening fixed-size parent controls, not approval of §12 or deployment capabilities; recorded 2026-11-05T23:19:11Z |

Step 0's A01–A09 dispositions remain accepted. Do not ask again for measured
fan-out as a prerequisite to this exploratory analysis. Do not treat A01 as
approval of a growing unbounded collection or measured evidence for a split.
The historical recovery/control question is retained in §11.7; its actual answer
permits §12's revision, which still requires renewed group-1/2 review.

### 11.2 Decision worksheet, alternatives and growth

| Decision | Candidate / alternatives / reconsideration evidence |
|---|---|
| G2-D1 granularity | **Provisional one record per association instance**, not per endpoint pair. Independent deletion by association ID and duplicate-instance identity are first-class access paths. Arbitrarily accumulated hits have no accepted per-parent/name ceiling; a single CDT cannot be declared safely bounded. This is a requirement-driven exploratory candidate, not a claim that fan-out is measured high or edge PI cost favorable |
| Consolidation alternative | A parent map keyed by association ID could supply atomic parent-state/admission/cascade locality with server-side CDT put/remove, but changes group 1 and needs a decided size/count overflow policy. Unknown fan-out does not authorize silently rejecting a new accumulated edge, overwriting an older one, or transparently sharding. A name-side CDT adds another multi-record consistency problem. Reconsider if bounded append behavior is explicitly accepted and sizing is available |
| Companion/sharded alternative | Bounded companions can reduce PI overhead for many tiny associations, but shard directory/growth, reverse ID deletion, discovery and admission fencing must be proved. Unknown degree/size and no read budget do not justify an invented shard count/directory. `DEFERRED_OPTIMIZATION`, not an unbounded registry escape hatch |
| Hybrid/dual adjacency | No required low-latency reverse-read SLO justifies duplicated adjacency or extra reconciliation today. `DEFERRED_OPTIMIZATION`; reopen with measured scan/latency pressure and consistency proof |
| G2-D2 lookup/index | Scoped association dataset scans for by-name/by-question/all and cascade discovery; key deletes by association ID. No SI, expression index, name registry or materialized count selected. Scope/name/qid indexes need actual access budgets, selectivity, memory/permission evidence and A09 checks |
| G2-D3 identity | Candidate positive int64 association ID, separate never-reset allocator key. Manager receives it in `.id` and feeds it back unchanged; not a qid or `(qid,name)` key. Numeric form matches SQL-facing identity without imposing global uniqueness or endpoint deduplication |
| ID alternative / judgment call | Association IDs, unlike qids, have **no FAISS numeric requirement**. Opaque UUIDv4 instance IDs with create-only collision handling could remove the extra allocator/hot key; content hashes or endpoint-pair keys would wrongly collapse duplicates. Numeric choice here is provisional SQL-shape alignment, not a confirmed numeric constraint or measured savings. Revisit before locking this blocked candidate; do not add allocation infrastructure merely because group 1 has it |
| G2-D4 cleanup | Candidate caller-synchronous full cleanup; no success until owned records are gone and late admission excluded. Partial failure leaves parent tombstone discoverable. **Not locked**: a scan followed by parent delete lacks the admission fence. No async thresholds/SLA invented under A08 |
| G2-D5 consistency/control | Original independent-write candidate fails (§11.5). Actual recovery decision permits the §12 fixed-size parent-control draft. No assumed MRT, AP worker, finite lease or permanent-tombstone success; renewed review/approval required |

Symbolic logical/uncompressed worksheet, all distributions/results UNKNOWN:

- `R_s = H_s + K_s + scope_bytes + name_bytes + question_bytes + fmt/sid/qid
  framing`. Each create rewrites `R_s`, each query reads full physical `R_s`.
  Arbitrary single strings may still exceed configured `L`; reject clearly,
  without truncation/chunking. No observed p99 fit or operational target `T`.
- For `E` associations and `C` initialized scopes, incremental candidate PI =
  `64*(E+C)*replicas` bytes, excluding group 1. Per-record ratio `64/R_s`;
  p99 parent contribution `64*degree_q_p99*replicas`; all values UNKNOWN.
- Parent CDT comparison: no extra PI when embedded, but approximate physical
  size `R_q + sum(edge_bytes + map_key/framing)` and every append rewrites that
  aggregate. Separate single companion: `64` extra PI per parent per replica,
  same aggregate growth problem. Split costs are accepted as a review question,
  not demonstrated savings. Tiny-record overhead explicitly remains a risk.
- Per-root contention: `avg_qps=events/window_sec`,
  `burst_qps=avg_qps*burst_factor`,
  `lambda=burst_qps*contention_window_ms/1000`,
  `P(>=2)=1-exp(-lambda)*(1+lambda)`; every input/output UNKNOWN. Separate
  association records do not remove allocator or proposed parent-fence hot keys.
- Candidate scans cost O(all association records in the fixed set), not just
  result fan-out; client result memory O(returned associations). Parent purge
  adds discovery/read/delete work proportional to scanned set/owned results.
  Full drain required; batching alone is not a completeness proof.
- Population and cleanup time grow without TTL. There is no collection bin,
  singleton ID/name registry, chosen shard count or automatic overflow path.
  Reopen when actual record/scan/PI/write/conflict budgets are supplied, or any
  oversize/pressure violates caller requirements; select numerical thresholds
  then. No production capacity or cleanup-time claim is made.

### 11.3 Candidate version, keys, types and JSON shapes

This is a reviewable candidate only, not authorization to create a set or mutate
the approved parent. Reuse `J` exactly from §3, namespace/configured scope
semantics unchanged. `DataSet.of(namespace,"gptc_s").id(J(...))` constructs keys.

| Set | Candidate key | Lifetime |
|---|---|---|
| `gptc_s` | `J(["gptc",1,cache_scope,"s",sid])` | One association instance until explicit ID delete or successful parent purge; never TTL |
| `gptc_meta` | `J(["gptc",1,cache_scope,"sid"])` | Fixed-size association high water, never reset/deleted by eviction/name drop/purge/TTL |

`sid` is the **association ID**, positive signed int64, not bool; DTO `.id=sid`.
DTO `.session_id` is the **name string**, not `sid`. Distinct instances may have
equal names/qids/questions and must still have distinct IDs. IDs are unique for
intact supported history (§14) within `(namespace,scope,association kind)`; equal numeric qid and sid
are harmless because key kinds/sets differ. No requirement for contiguous IDs.
This candidate does not borrow, reset or add a bin to the question allocator.

| `gptc_s` bin | Type / mutability / requirement |
|---|---|
| `fmt` | int64 exactly 1; immutable, format validation |
| `scope` | exact string; immutable, all-path isolation |
| `sid` | positive int64; immutable, returned instance identity/key validation |
| `qid` | positive int64; immutable, owner and question filter/cascade F06/F09 |
| `sname` | string, including empty string; immutable, exact name filter/DTO |
| `squestion` | string, including empty string; immutable, original pre-embedding question text for hit checks, not reconstructed from cached payload |

All are required, wrong type/unknown fmt is an error. Strings retain exact code
points, case and whitespace; no executable codec, coercion or normalization.
No collection grows inside a record, and no extra timestamp/state bin is
invented. Association timestamps are not required by a DTO/access path.
The candidate association meta record has the same `fmt:int64=1`,
`scope:string`, required root-matching `boot` (§14), `hi:int64 0..MAX_INT64`
type contract as §4, at its distinct key.

Synthetic shape examples, not measured values:

```json
{
  "namespace": "<configured>", "set": "gptc_s",
  "key": "[\"gptc\",1,\"demo\",\"s\",41]",
  "bins": {"fmt": 1, "scope": "demo", "sid": 41, "qid": 7,
           "sname": "chat", "squestion": "original café question"},
  "expiry": "explicit never-expire"
}
```

```json
{
  "namespace": "<configured>", "set": "gptc_meta",
  "key": "[\"gptc\",1,\"demo\",\"sid\"]",
  "bins": {"fmt": 1, "scope": "demo", "boot": "00000000-0000-4000-8000-000000000001", "hi": 41},
  "expiry": "explicit never-expire"
}
```

Historical candidate initialization below is superseded by §14. Candidate ID
allocation: validate input first; use explicit operator-owned create-only meta
initialization and existing-only guarded `hi<=MAX_INT64-n` atomic `.add("hi",n)`
then `.get("hi")` (raw slot 1, exact int validation) as in §5. All ownership,
format/range, original-outcome and uncertainty checks apply. Unknown reservation
means no guessed IDs/child writes. Leave gaps, fail exhaustion before add, no
decrement/reuse. A failed parent admission can waste a reserved association ID.
Pristine initialization/reopen must inventory `gptc_s` and both allocators as
well as all future kinds/external references; no automatic reconstruction from
surviving associations/questions. Final all-kind initialization remains open.

### 11.4 Candidate access paths, projections and result semantics

All keyed operations inherit §5's visible missing/filtered outcomes,
`execute(on_error=ErrorStrategy.IN_STREAM)`, complete correlated-row validation
and unfiltered exact-key diagnostics; no empty stream as invented success.
For all create/update segments, repeat `.never_expire()`; nontransactional
candidate explicitly uses `.with_txn(None)`, no implicit MRT. Effective zero
retries for allocation/append, and all group-1 retry/policy contracts remain.
Point/batch read-touch disabling and **unverified dataset-query wire/void-time
behavior under A09** remain unchanged. No query-retention proof is claimed here.

Scan implementation: typed `FilterExpression` exact-scope predicates, never
interpolated raw names/scopes in AEL. Every delivered row must validate namespace,
fixed set, full bin types/ranges, exact scope and actual digest equal to the
reconstructed `gptc_s` key before it can supply an ID/filter/cleanup target.
Missing/synthetic physical metadata fails closed. Validate repeats before
deduplication by physical association identity, not `(qid,name,question)`;
immutable repeats with inconsistent bins are corruption. Drain iteration and
all errors before returning; no partial-success list. No shared snapshot, no
sorted order promised; concurrent add/delete may change observations. A
quiescent complete stream must yield every matching distinct association.

| Host path | Candidate key/query / returned value and outcomes |
|---|---|
| Initial association in `batch_insert` | Only truthy input session name; `squestion` is inserted string question or `Question.content`, per SQL. After payload known persisted, reserve instance ID and follow §12's draft admission protocol. No truthy session means no association. Failure reports input position/qid/known payload status/sid if allocated/association status separately; never return a complete-success ID list with a failed/unknown association |
| `add_session(qid,name,original_question)` | Explicit append accepts string names (including empty), unlike truthy-only initial association. Missing/deleted parent rejects. Each acknowledged call creates a distinct instance via create-only insert under §12's admission/release draft; `.where(...)` on a child cannot inspect the parent. Not precheck/write/postcheck |
| `list_sessions(session_id,key)` with truthy name | Scope + exact `sname`; **ignore key**, even if supplied/truthy. Return full association DTOs, including duplicate instances and those whose parents are tombstoned; no parent join/hiding |
| Falsy name + truthy key | Scope + exact qid; full association DTOs. An empty name is not an exact-empty-name query. Valid positive numeric keys retain qid typing; truthiness does not authorize coercion |
| Both falsy (`None`, empty-name, key=0 cases) | Scope-only complete associations; no implicit question filter. `key=0` is the SQL-compatible absent-filter case, not an allocated qid |
| DTO construction | Tuple-compatible four fields in SQL column order `(id,question_id,session_id,session_question)` with named attributes. Manager key-path consumes full rows; name-only path extracts `.id`; neither extracts distinct names. Both supplied: scalar name precedence remains even though manager's truthy key branch returns full rows |
| `get_data_by_id(qid)` projection | Group-1 acknowledged active read/touch remains exactly unchanged, including raw original slot 6. Then scope+qid association query projects one-column tuple-compatible rows `(name,)`, `[0]`, unpacking and `.session_id`, preserving duplicates. Query failure raises, not `[]` or a successful DTO; already-applied access update is disclosed. Concurrent mark/purge may yield fewer associations after the active read; no cross-record snapshot/new touch replay |
| `delete_session(ids)` | Derive exact scoped `gptc_s` keys from **association IDs only**, never qids/name search. Empty input is no-op; repeated IDs can be coalesced. Validate exact unfiltered reads, then existing-only conditional `.delete_record()` guarded by validated identity/bin values and returned generation where used. Repeat per-segment predicate; include no-expiry policy on write segments. Known missing is idempotent success; filtered/generation conflict requires diagnostic/error, transport/uncertain failure stays visible. No parent-existence guard: deleting owned records during/after mark must work. No question/meta/report/vector mutation |
| Manager name drop | Complete name listing yields sid list, then exact deletes above. A new sid added later is not in that list and may survive; A04 explicitly accepts this, not permanent name revocation |
| `mark_deleted(qid)` | Group-1 state 0→-1, no association rewrite; already-owned rows remain listable. No new state value, touch order, timestamp or immutable-payload mutation |
| `clear_deleted_data()` | Group-1 validated first-observed tombstone candidates → §12's closed/zero admission fence → scope+qid complete discovery/delete/empty verification → existing-only guarded parent delete. Any incomplete/unknown stage is a visible failure, retains parent where still controllable, never success-shaped completion. Physical-parent-gone plus surviving child is an invariant breach, not successful purge |

These paths introduce one repeated dataset-scan family rather than treating
scans as proven cheap/hot-path compliant. Session projection/hit/name reads have
no supplied rates/SLO; key redesign/index evaluation is required if measured
dominance/budgets reject scans. No third-party SI is created in this task.

### 11.5 Historical unchanged-parent candidate — independently confirmed failure

Retained as the counterexample motivating reopening. The replacement draft is
§12; this section is not its admission algorithm.

Individual record atomicity, filters and generation CAS do not make a read of
`gptc_q` and a create of `gptc_s` atomic. Generation guards apply to the record
being mutated; a child insert cannot compare another record's generation.
Additional postchecks or “scan until empty” do not close these schedules:

1. **Late child:** writer W reads active q7; purger P marks q7, scans no
   association, deletes q7 and returns success; W creates s41 afterwards.
   Child predicate/creation-only/TTL all work locally, yet ownership is violated.
2. **Postcheck plus process crash:** W reads active, creates s41, then crashes
   before checking/compensating. P's discovery can already have passed that key
   (or precede its write) and can delete q7. There is now a physical child with
   no parent and no durable discoverable parent cleanup marker. A hidden worker,
   successful read-time hiding, or TTL would not satisfy successful physical purge.
3. **Check again before parent delete:** even after an empty verification scan,
   W may be paused before the child write. No finite series of independent
   empty scans proves absence of an admitted late writer. A scan failure must
   raise, but successful scan completion alone is not an admission fence.
4. **Interrupted cascade:** if P deletes some existing children then fails while
   parent remains tombstoned, another caller can repeat scoped deletes safely.
   That covers a crash *before parent deletion only if admission is already
   closed*. It does not repair the first two schedules. An uncertain parent
   delete must be reported as such, never inferred safe from later absence.
5. **Counter/lease shortcuts:** adding a parent in-flight counter could prevent
   deletion while nonzero, but a crashed writer can leave it stuck. Resetting it
   on timeout, or decrementing after an unknown child operation without proving
   completion, permits the same late write. A finite lease/epoch in the child
   alone cannot fence its physical create against a parent check.

**Conflict is relative to the unchanged approved parent plus independent
record writes, not a claim that the requirements are globally impossible.**
The candidate does not pass the developer lifecycle walkthrough. No approval
can be requested as if a complete correct session model already existed.
Failure visibility A04/A08 permits stopping/failing safely; it does **not**
permit P to return success in schedules 1–3. Ordinary repeated caller purge
cannot prove paused/dead writers have stopped solely from this schema.

### 11.6 Historical candidate walkthroughs — superseded by §12.8

- **S1 create/read/duplicates:** synthetic active q7; reserve sid41, create
  `["gptc",1,"demo","s",41]` with name `chat` and original text; a second
  call gets sid42 even for identical values. By-qid gives two full DTOs,
  projection gives `[(chat,),(chat,)]`; truthy `chat` with key8 still selects
  by name. Falsy name/key gives all. **Read/shape/identity trace passes
  conditionally; end-to-end append does NOT pass until admission is resolved.**
  No actual server/codec test or independent pass is claimed.
- **S2 manager drop/partial/unknown:** manager lists `chat`, obtains 41/42,
  deletes scoped association keys. If 41 succeeds and 42 is in-doubt, raise
  correlated known/unknown outcomes; repeat same ID deletion safely, no new
  allocation. New sid43 concurrently added may remain, not a drop-contract
  failure. Payload/vector/qid high water unchanged. **ID deletion and
  nonsnapshot name-drop trace passes at paper level**; effective builder
  policies, immutable integrity checks and transport outcomes need tests.
- **S3 mark/purge/crash:** execute each §11.5 schedule, including crash between
  parent-check/child-write/postcheck, crash during cascade and lost acknowledgments.
  Tombstone permits rediscovery of interrupted existing-child cleanup, but
  cannot stop late child after its own deletion. **FAIL: schema/control gap (a),
  not normal engineering (b).** No group-2 walkthrough pass, stakeholder-ready
  verdict, automatic recovery, successful orphan purge or final model claimed.

Required later tests (not executed): independent process interleavings with
deterministic pause/crash at every boundary; parent missing/deleted admission;
initial truthy/empty-name behavior; identical duplicate associations; every
A03 filter combination and manager ID deletion; query physical identity and
repeated/corrupt rows; incomplete iteration; no-prefix errors; per-segment
no-expiry/zero-retry/transaction opt-out; lost child/allocator/delete reply;
partial cascade retry; finite-default void-times and actual query wire
retention; durability/restart/restore; too-big strings and numeric exhaustion.
Any adopted fence needs its own executable crash/recovery proof before acceptance.

### 11.7 Historical requirements question/options — actual response now recorded

**Actual response recorded 2026-11-05T23:19:11Z: “Allow quiesced recovery”.**
Option 1 below was selected as a requirements decision and permission to reopen
parent controls, **not schema approval**. Option 2/3 were not selected. The
independent inspection `0595eced`, observed at 2026-11-05T18:42:31Z, confirmed
G2-C1 with a minimal-baseline qualification. Architect accepts that finding:
precheck/postcheck, child CAS and repeated empty scans are not an atomic fence;
MRT-only/global-impossibility claims would be too broad. See §12 for the draft
and the review ledger for G2-R1–R5 triage. The question/options below are history,
not an outstanding request to repeat the user's answer.

Keep A04's successful-purge ownership contract. Ask for the missing operational
requirement, not an arbitrary preferred storage mechanism:

> After a crashed or in-doubt session addition, may purge fail closed with its
> parent tombstone retained until caller-driven recovery can verifiably stop
> **all** writers and settle pending database commands, or must recovery work
> while other writers continue? If quiescence is acceptable, may we reopen
> group 1's parent control contract for a fixed-size admission fence and
> re-review its effects, without changing immutable payload/state/timestamps?

Disposition choices and consequences:

1. **Quiesced exceptional recovery acceptable:** reopen group 1 to evaluate
   fixed-size parent admission accounting guarded atomically by active state;
   mark closes admission, purge cannot delete while outstanding admissions
   remain. Crash/unknown outcomes may intentionally block purge until verified
   quiescence and settlement/inventory allow safe caller recovery. This is a
   proposed requirement/control revision, **not adopted here**. No timeout
   reset, bounded SLA, unbounded pending-ID collection or lease safety assumption.
   Must prove admission-release uncertainty, counter exhaustion, interrupted
   recovery and all-writer exclusion; re-run group-1/2 walkthroughs and independent
   reviews before stakeholder model approval. A09 history/durability still gates it.
2. **Online crash recovery required:** leave group 2 blocked and first obtain
   separately authorized A09 capability inspection. If deployment supports
   suitable transactions, evaluate parent-state participation/child commit and
   mark conflict semantics, streaming/cascade boundaries, crash/commit-in-doubt
   recovery; do not wrap an unknown-fanout purge in an assumed giant transaction.
   SDK transaction surfaces are not capability proof; no MRT/SC/index dependency
   is selected or activation allowed now. No network/start/install is authorized.
3. **Bound accumulated association growth explicitly:** a new requirement could
   authorize a per-question count/byte ceiling and explicit overflow rejection.
   Then evaluate parent-local CDT mutation/cascade (reopening group 1), reverse
   ID deletion and sizing costs. No cap exists today; do not infer one from A01
   or promise single-record fit. This is an alternative requirements revision,
   not the default or a reason to discard existing duplicates.

Independent implementer inspection of this conflict/candidate must precede
presenting a session-model approval checkpoint. The reviewer may confirm or
refute the gap with actual source support; the author cannot award their own
pass. If a requirements decision is obtained, record the actual response,
revise/re-review the affected groups and re-present group-2 approval. **Do not
start group 3**, derive a schema summary, write final evidence approvals or
complete task 01-02.2 while this conflict remains.

## 12. Reopened parent controls + session admission — conditionally approved

### 12.1 Scope, decision forms and explicit limits

This revision addresses G2-C1 under the actual quiesced-recovery decision.
Groups 1/2 are **APPROVED_CONDITIONAL_EXPLORATORY_ONLY**, not implementation-ready.
Only parent control fields/guards and association lifecycle change. The key,
`fmt=1`, immutable payload, qid allocation, state values `0/-1`, one-way mark,
SQL naive local times and seven-get/put touch order are binding. §11.3's
association keys/bins/JSON and positive numeric instance IDs remain the draft;
no tuple deduplication, new report design, SI, MRT or registry is added.

Pack 1: question-owned associations have shared purge lifetime, parent/full-set
and independent ID-delete reads; one constant-size control on the existing
owner is necessary to fence its lifetime. Pack 2: duplicate instance identity,
name/question/all reads and independent ID deletion retain §11.2's provisional
per-association granularity and symbolic PI comparison; **not** one record per
endpoint pair. Pack 5: caller-synchronous cleanup, discoverable tombstone on
failure, no runner/lag promise. Newly accepted input: exceptional all-writer
quiescence plus command settlement may be required. Rates, degrees, bytes,
contention windows, budgets, capabilities and measured results remain UNKNOWN.
These are completed exploratory dispositions, not passed production sizing.

Judgment G1-D7/G2-D5: use a bounded scalar outstanding-admission count, not
per-writer tokens. Acknowledged admission protects a paused writer. Unknown
entry/release may strand accounting; safety wins over online progress. Rejected:
unbounded token map, timeout lease/reset, repeated empty scans as a fence,
blind decrement, implicit transaction/reconciler. Reopen if online recovery,
bounded recovery time, new API/privilege requirements, unsupported authoritative
history or measured hot-key costs conflict. If a binding property cannot be
supported, report `REVISION_CONFLICT` with options, not a relaxed contract.

### 12.2 Exact control schema, initialization and validation

Add **two required bins** on `gptc_q`, no extra set, key or per-writer registry:

| Bin | Type / purpose / mutations |
|---|---|
| `afmt` | int-not-bool exactly 1; admission protocol discriminator; immutable after pristine insert or explicitly reviewed quiesced migration |
| `adm` | int-not-bool in `0..MAX_INT64`; outstanding accounting, **not child count**. Initialize 0; guarded atomic +1/-1 only in normal operation; exceptional reset only §12.7 |

Both names are below 15 bytes. Extraction justification: `afmt` rejects missing/
incompatible controls and `adm` is used by atomic admission/release/delete
predicates. Neither is encoded inside the immutable payload. Example amendment
to the existing §4 question JSON (all original bins remain):

```json
{"key": "[\"gptc\",1,\"demo\",\"q\",7]",
 "additional_bins": {"afmt": 1, "adm": 0},
 "expiry": "explicit never-expire"}
```

§4's complete example includes these conditionally approved control additions; this is
not a new set. Every new create-only question insert initializes them together
with its full payload. All parent mutations are existing-only; never upsert,
replace the record, reset the counter on mark, or implicitly add missing bins.
All diagnostic reads validate these controls along with §5's full record/key
checks. Parent dataset projections add `afmt,adm` and validate every delivered
row, including repeats, before the unchanged R3 first-observation selection.
The touch predicate adds typed valid controls; the existing seven read slots
remain unchanged, raw slot 6 alone supplies prior access, write slot 7 touches
only `last_access`. Filtered controls are corruption/incompatibility, not a miss.
Mark predicates add valid controls but change only `state=-1`.

Do not default a missing `adm` to zero, infer it from child count, or treat a
legacy record as migrated. No deployed schema/migration is asserted. If legacy
records exist, activation blocks until old writers are permanently excluded,
all commands settled, all records/owners/external references inventoried, full
legacy data validated, and a separately reviewed quiesced migration initializes
controls with no remaining admissions. Old processes cannot later resume;
new instances must enforce this protocol. No online mixed-version backfill.
Missing/corrupt counters require integrity recovery, not §12.7's reset shortcut.

### 12.3 Invariants and linearization

Let A(q) be `adm`. Let U(q) be admitted operations which may still issue or have
unsettled child modifications. Under one authoritative durable record history:

1. **A(q) >= |U(q)| >= 0.** It may overcount abandoned/uncertain admissions.
   It is neither cardinality of children nor evidence of which writer owns one.
2. The **entry operate**, not any prior read, is admission. Entry and mark
   serialize on the same parent. Entry before mark may finish its one child
   command after mark; entry after mark is rejected. No promise that a concurrent
   mark cancels already-admitted work. A call begun after known mark cannot enter.
3. An operation never issues a child create without its own acknowledged entry,
   issues at most one child create under that entry, and relinquishes protection
   only after irrevocably forbidding future use and settling that command.
4. `state=-1` closes entry permanently. With A=0 there can be no protected
   delayed child creator; an unknown entry cannot authorize a child. Parent
   delete needs closed admission, A=0 and complete settled owned-child cleanup.
5. Nonzero/unresolved accounting retains the physical tombstone and yields a
   visible recovery-required failure, not completed purge. No orphan may remain
   after successful physical purge. Payload, timestamps and state semantics do
   not change to encode a lease, recovery state or new lifecycle.

Only normal release or externally justified exceptional recovery can remove
accounting. Corrupt state, history loss, split-brain, restore or resurrection
invalidates this argument: fail closed under A09, never claim the counter fixes it.

### 12.4 Exact primitive sequence and owner responsibility

`V(q)` denotes typed equality on fmt/scope/qid/afmt plus valid `state` and
`0<=adm<=MAX_INT64`, with physical key derived/validated as §5. Typed SDK `Exp`
predicates must compare the bins on the record being mutated. Not raw user AEL.
Table `&` notation means SDK `Exp.and_([...])`, not an assumed Python `&`
overload. For example active state is
`Exp.eq(Exp.int_bin("state"), Exp.int_val(0))`, capacity is
`Exp.lt(Exp.int_bin("adm"), Exp.int_val(MAX_INT64))`; use the same typed
`int_bin/int_val` equality/range comparisons and `string_bin/string_val` exact
scope equality for V. Integer particle reads reject missing/wrong-typed bins,
not coercion of boolean/string counters. Diagnostics still validate full bins.
All calls use §5 keyed visibility + IN_STREAM + complete correlation, explicit
`.never_expire().with_txn(None)` on **every write segment**. Effective
WRITE_NON_RETRYABLE/POINT **max_retries=0** is mandatory for entry, child create
and release, through overrides/reload/transport/application wrappers; not merely
the SDK default. No automatic transaction/convenience retry runner.

| Phase | Exact key and operation ordering | Required result / next action |
|---|---|---|
| Prepare | Validate inputs; reserve known sid via §11.3 before entry | Unknown sid allocation: no admission or child write, surface uncertainty; gaps allowed |
| Enter | `session.update(qkey).where(V & state==0 & adm<MAX_INT64).add("adm",1).get("adm")` | One atomic guarded existing-only add/read. Raw slot 1 must be exact int in `1..MAX_INT64`, known OK/no doubt. Only then authorize one child create |
| Child | `session.insert(skey).put(all_immutable_s_bins)` | One create-only attempt, scoped identity, no retry. Known success or proven non-applied terminal rejection can proceed to release; unresolved command cannot |
| Release | First irrevocably close this local admission to future child sends; then `session.update(qkey).where(V & adm>0).add("adm",-1).get("adm")` | One attempt, works at state 0 or -1. Raw slot 1 exact int in `0..MAX_INT64-1`; known OK completes release. Never decrement another time for this admission |
| Mark | Existing-only `where(V).put({"state":-1})` | Idempotent one-way close, preserves current adm and all immutable/time bins |
| Final parent delete | Existing-only `where(V & state==-1 & adm==0).delete_record()` **after §12.6** | Explicit OK or known absent after compliant concurrent purge; otherwise error/doubt, never infer deletion success |

Release is the **owner's** responsibility only for its own known admission.
Record the local phase transition before sending release; no generic `finally`
that decrements after arbitrary exceptions, no shared/copyable/reusable permit,
and no child-command retry after release is attempted. This phase is volatile;
a crash intentionally abandons accounting, not an invitation to recreate a
release from a child ID/counter read. Process restart cannot resume that permit.
If a known-admitted call chooses not to send any child, it can close locally
and release once. A known terminal child rejection still reports append failure
even when release succeeds. If child succeeded but release fails/unknown, report
known child persistence plus accounting uncertainty/recovery requirement, **not**
a successful append or duplicate append retry. Initial association errors retain
input/qid/sid/payload correlation and never return a full-success batch ID list.
Append success requires both known child persistence **and** known release.
A caller explicitly choosing a later fresh `add_session` gets a new sid and
admission; it may duplicate the first call's persisted association. Disclose
that possibility, never make the new call an automatic retry, reuse an abandoned
permit, or silently compensate by deleting an association whose status is unknown.

### 12.5 Unknown outcomes and all boundary crash/pause schedules

| Pause/crash/lost reply boundary | Safety argument / permitted continuation |
|---|---|
| Before entry send (including stale active precheck) | No authority to create. If mark wins before the eventual entry, guard rejects. Crash has no admission to release |
| Entry sent, reply lost or result missing/malformed | Do not create or decrement, even if later A>0. Entry may not apply, may apply before mark and strand +1, or reject after mark. Surface UNKNOWN_ENTRY. No replay/guessed ownership |
| Entry acknowledged, before child send | A>0 protects paused writer even if marked. Crash strands count. Ordinary purge fails closed. A resumed live writer may send its single child or close-and-release without a send |
| Child command sent, no terminal proof (timeout, cancellation, crash, lost reply) | Retain protection; no compensating delete followed by decrement while create might still arrive. No blind child retry, no release. Surface UNKNOWN_CHILD if caller survives |
| Child settled, before release | Success may remain visible through soft deletion; failure may have no child. A>0 blocks parent removal in either case. Crash strands count, safely |
| Release sent or acknowledged but caller crashes/loses reply | Child sends already permanently disabled and settled. Decrement may apply once or not. No replay: a second decrement could consume another live writer's admission. Surface UNKNOWN_RELEASE; later reading A or child cannot identify this decrement |
| Mark pause/crash/lost reply | All entry/mark orderings are serialized. Unknown mark is an explicit error; caller may idempotently mark again, never reset A. Parent discovery alone is not a fence |
| Cascade pause/crash after deleting a subset | Parent remains tombstone, admission closed; repeat complete discovery and exact deletes. No success from prefix. No child commands can land once closed/A=0 |
| Final parent delete sent, lost reply/crash | Closed/A=0/settled children already established. No owned child can appear later. Surface unknown deletion; caller can retry idempotent purge/diagnose exact absence, not rewrite history as acknowledged success |
| Recovery pause/crash, including reset reply lost | §12.7 external exclusion must survive operator failure. Keep writers stopped, re-establish settlement and repeat inventory/cleanup; idempotent reset to zero only within revalidated recovery context |

At A=MAX_INT64 reject new entry **before** add; no overflow/wrap/reset. Known
absence/deletion rejects entry; filtered entry uses unfiltered diagnostics to
distinguish tombstone/capacity/corruption/rejection-race. A filtered entry that
is definitively non-applied may be reported rejected; it conveys no permit.
No later read repairs an unknown entry/release. Missing parent during a
known-admitted release is an invariant breach, not ordinary idempotent success.
Filtered/generation/error/missing result on release fails visibly with no retry.
Bounds are representation limits on simultaneous accounting, not a fabricated
cap on accumulated associations. Leaked counts may exhaust admission: fail closed.

### 12.6 Ordinary caller purge and concurrent purgers

`clear_deleted_data()` keeps §5's complete validated first-observed tombstone
discovery. For each qid, read/validate its exact parent unfiltered. Known
absence from another compliant purger is idempotent; active is never deleted.
For a valid tombstone with A>0 return a correlated recovery-required failure
(other independently completed qids remain disclosed); **do not decrement/reset
or conclude a writer died**. Ordinary purge has no quiescence shortcut.

For a valid tombstone with A=0: admission is permanently closed. Drain the
scope+qid association query, validate every row's physical namespace/set/digest,
fmt/scope/sid/qid/immutable fields, deduplicate by instance identity and delete
only those exact scoped association IDs with their existing-only guards.
Complete all delete outcomes. A second complete validating scope+qid scan must
be empty before final parent delete. This scan verifies cleanup **after the
counter fence**; it is not itself the fence. Stream failure, unknown delete or
remaining child retains tombstone/fails; exact ID-delete retry is allowed after
settlement. No outstanding child-delete command is ignored at final success.

Concurrent compliant purgers can see the same child and delete it twice; known
missing is success and no purger touches adm. Guarded final parent deletes
serialize; known absent is replay-safe. Neither parent nor sid is ever reused.
Other scoped qids/writers can continue. Name-drop can only remove rows, not open
admission. No snapshot over unrelated records is introduced. A09 must establish
that a quiescent complete scoped enumeration actually covers all owned rows and
that delete/restart histories cannot resurrect them. Otherwise activation blocks.

### 12.7 Exact exceptional recovery entrypoint and external proof obligations

**Accepted backend-specific operator entrypoint (not implemented; §14 qualifies
synthetic feasibility proof, not final-provider capability):**
`gptcache.manager.scalar_data.aerospike_storage.AerospikeStorage.recover_session_purge(qids, *, recovery_context)`.
It is NOT a new `CacheStorage` ABC/manager method, not `flush`, ordinary purge,
an environment flag, or a worker. It completes physical purge only for explicit
scoped **already-tombstoned** qids; it never resets active-parent accounting.
No additional database/user privilege is presumed. A need to change common API
or privileges is a new stakeholder decision, not implementation discretion.

`recovery_context` is a reference to an operator-controlled, independently
reviewed evidence/verification procedure, **not** `quiesced=True`. Before any
reset/delete, that procedure must establish and retain:

1. Exact namespace/scope, targeted qids and protocol/deployment inventory:
   **all** processes/hosts/threads/versions, callers with retained admissions,
   queued/retrying commands, scheduled/restart paths and direct writers able to
   modify these records. If isolation cannot exclude other scopes safely, the
   exclusion domain must be broader; do not guess scope enforcement.
2. Actual enforceable exclusion: those writers have irreversibly lost the
   ability to resume/submit old work, and no replacement/autorestart can act
   until recovery completion. Include ordinary purgers/name drops/other
   recovery operators for a stable inventory. Suspending one process, taking a
   local mutex, closing one client, no observed traffic, or a signed checkbox
   is insufficient. The evidence must name the real existing exclusion
   mechanism, its coverage and who verifies it.
3. Settlement of **every previously issued modifying command**, including
   entry, create, release, mark, delete, retries/batches/transport queues:
   each has completed or is authoritatively prevented from later applying.
   Evidence must name the actual supported transport/server drain/barrier or
   terminal-outcome mechanism and its authoritative-history/durability scope.
   An SDK timeout, socket close, process kill, sleep or empty scan proves none
   of this. Do not invent a server “drain” API or finite safe wait.
4. Exclusion continuity across operator/recovery crash. A new operator must
   reverify the same scope coverage and command settlement before proceeding.
   If exclusion is lost, stop: no reset on stale evidence, no restart of old
   admissions. Recovery's own uncertain commands must also settle before the
   next inventory. Evidence identifies verifier, artifacts and the maintained
   exclusion boundary; it is not an unbounded database token registry.

**§14 supplies the exact supported construction**, with completed synthetic
controller/runtime preflight supporting model feasibility. Final-provider
integration is future implementation/test work, not an unresolved impossible
model prerequisite. The actual helper at invocation must verify 1–4; a context
object alone proves nothing. If those checks fail it must refuse mutation;
tombstones remain and ordinary purge may fail indefinitely. Do not ship a
boolean-only bypass or an always-refusing “completed feature”. If the
authorized environment cannot meet these preconditions without new privileges/
API/capabilities, surface `REVISION_CONFLICT`: retain blocked recovery, request
explicitly authorized capability/operational change, or ask for a revised
requirement. Do not silently offer online reset or permanent-tombstone “success”.

Under proven and continuously maintained exclusion/settlement, the operator:

1. Validates the entire exact parent/key/controls and tombstone; inventories all
   scoped owned associations using §11.4. Unknown/corrupt owner/key fails closed.
2. Deletes all owned association IDs; settles every outcome and completes an
   empty verification query. No child create or old release can subsequently land.
3. Existing-only guarded `where(V & state==-1).put({"adm":0}).get("adm")` on that
   exact parent, explicit never-expire and txn opt-out. Reset is idempotent only
   inside this externally verified context; no arithmetic replay. Require raw
   slot 1 exact zero. On doubt stop/re-establish settlement before reinspection.
4. Revalidates absence of owned children and does §12.4's guarded final delete.
   Reports known/failed/unknown results per qid. On interruption leave exclusion
   in effect and retry the full validated procedure, not a remembered decrement.
5. Ends exclusion only after all recovery commands are settled and outcomes/
   remaining tombstones disclosed. Future **fresh** protocol-compliant writers
   may run; old processes/queues/admissions must never regain permission.

This is caller/operator-driven recovery, no automatic escalation, SLA or
guarantee that the local deployment can supply the prerequisites.

### 12.8 Revised developer walkthroughs, costs and review handoff

**W/S1 — create, read, duplicates and drop.** Synthetic q7 is inserted with
afmt=1/adm=0 and unchanged payload/naive times. Known sid41 reservation → parent
entry returns 1 → create s41 → terminal acknowledgment → release returns 0.
Repeat identical append with sid42 gives two instances. §11.4's four-column DTO,
duplicate `(name,)` projections, truthy name precedence (even with truthy key),
falsy-name/key-zero behavior and manager deletion of **association IDs** stay
unchanged. Touch still returns its acknowledged original slot 6, not a later
read. Drop 41 leaves 42; a later 43 can survive name-drop's captured-ID list.
**Paper structural trace supported; no independent revised pass/live test.**

**W/S2 — mark races and clean purge.** W enters q7 (A=1), pauses before child.
P marks q7, sees A=1, fails visibly with tombstone retained. W may create s41
after mark because entry linearized before mark; after settlement its single
release gives A=0. P retries: closed/zero → complete discovery/exact child
deletes/empty verification → guarded parent delete. If mark won first, W's
entry rejects and no child is issued. Multiple W admissions counted separately;
two purgers can safely duplicate ID deletes. **Old late-child schedule excluded
at paper level by shared-parent entry, not child CAS or empty-scan repetition.**

**W/S3 — crash, lost release and operator recovery.** W1/W2 enter (A=2).
W1's child settles, release applies but reply is lost (A=1). W1 cannot replay:
that would hide W2. Mark closes entry; W2 paused/in-doubt/crashed prevents
ordinary purge. Only §12.7's actually established exclusion **and settlement**
allows all children to be cleaned and adm reset, then parent deleted. If no
external settlement proof exists, recovery refuses and retains tombstone.
Crash before/after each recovery mutation repeats full inventory under
maintained exclusion. **Safety paper trace conditional on explicit external
preconditions; operational availability/durability proof remains OPEN A09.**

Cost amendment: `R_q' = R_q + framing(afmt,adm) + two int64 values`; no per-writer
collection/PI entries. Normal append adds two full-parent rewrites/round trips
(entry/release), approximately `2*R_q'` logical write bytes per association
besides `R_s` and allocator writes. Parent event rate now includes `2*appends`
plus touches/marks/creates; use §7's symbolic contention formula with that
unknown rate. A small bin update still rewrites the whole immutable payload:
large/hot questions need measured justification, no production capacity claim.
Recovery scans/deletes scale with set population/owned fan-out, not a bounded
lag; admission saturation/oversize rejects explicitly. No hidden optimization.

Required renewed independent review: actual builder predicates/update-only
semantics and slots, entry-before/after-mark, every §12.5 boundary, uncertain
decrement no-replay, concurrent purgers, migration/no mixed writers, physical
identity/filter guards, all-record no-expiry and external recovery obligations.
Actual source support inspected: `session_shared.py:386–460` (verbs);
`operations_shared.py:154–161,416–428,514–550,573–585,621–664,678+` (existence,
TTL/filter/CAS/results/numeric add/put/read/delete); `query_shared.py:712–746,
1549–1577,1918–1967` (txn opt-out, segment resets, policy fields);
`sync/operations/query.py:493–624` and
`sync/operations/query_dispatch.py:409–449,876–909` (sync promotion and single-key
operate/error control flow); `record_result.py:124–143,193–220` (OK/raw slots).
All SDK paths are relative to the inspected checkout; source support is not
deployed capability or command-settlement evidence. R1–R5 remain binding.

Later executable tests must pause/kill/lose replies at **every** boundary in
§12.5; exercise max counter/underflow, corrupt/missing controls, old writer
refusal, duplicate rows/name projections, partial cleanup/recovery interruption,
effective policies and finite-default/query void-times/durable restart. None
ran in this revision. No new reviewer verdict, stakeholder schema approval,
summary, final evidence or task completion is asserted. **Stop for independent
implementer review; then actual approval of reopened controls and group 2 before
group 3.** That historical stop is superseded by §1's actual conditional
approval; its implementation/capability conditions remain binding.

## 13. Report history — third/final group conditionally approved

### 13.1 Entry gate, requirements and routed input audit

**Draft recorded 2026-11-05T23:34:56Z UTC** (recording time only).
The real conditional groups-1/2 decision permits this group. It does not clear
RA-1/RA-2 or A09, or authorize persistent implementation. §12 parent admission
is unchanged. This section is an incremental candidate, not a final schema
summary, overall review or retention/compliance certification.

| Required input / route | Source / disposition |
|---|---|
| Ownership/lifetime, reference after purge | F09/F10, A05: independent copied history survives question/session purge, no TTL or report cleanup/query API in MVP |
| Pack 4 event read shape | Append-only `report_cache`, no timeline/page/unread/mark-read access. Recipient/actor entities, ordering, pagination and element-0 sort choice: NOT APPLICABLE, not missing new product requirements |
| Pack 4 rates/volume/bytes/horizon/retention/read-write targets | All min/p50/p95/p99/max, burst/skew and operational budgets UNKNOWN under A01. No finite retention window, events/day, bucket width or count cap can be derived |
| Shared sizing §5.1 | Three copied values plus numeric fields/type envelope; actual logical encoded bytes, distribution and projected aggregate UNKNOWN |
| Shared contention §5.2 | Events/window, active window, contention window, burst factor, average payload, projected count/size, horizon, p95 SLO, observed key-busy/write latency all UNKNOWN; load test NOT RUN |
| Pack 5 side effects | Report creation has zero required question/session writes; purge has zero report deletions. No report reconciliation runner, deletion lag or retry budget invented; failed/in-doubt append stays caller-visible A08 |
| Identity/small independent entity gate | Numeric internal report instance ID, distinct scoped allocator; symbolic PI/packing comparison §13.6. No measured acceptable-overhead verdict |
| Safe types/time | A07 native-node allowlist; actual caller passes `.answers[0].answer` when str, otherwise `""`, not the Answer wrapper. Elapsed time is numeric seconds, not `datetime.timedelta`; generated local report time follows SQL, not UTC |
| Capabilities | Current §§6.1/14 supersede this group's original unresolved-feasibility status: qualified completed synthetic proof supports model feasibility; final-provider/released-server tests and final review/approval remain required |

Actual host evidence: `base.py:16–25,148–157` (Answer value's Any annotation and six-argument
scalar signature); `data_manager.py:393–409` forwards unchanged and returns
no report identity; `adapter.py:173–175,220–231,439–441,483–494` supplies
`float(rank)`, numeric elapsed seconds, cached numeric ID and **the plain
`.answer` string**, otherwise `""`, not the Answer object. Both paths are covered;
their different report-enable condition is host behavior, not this backend's
new policy. `sql_storage.py:139–175,387–398` stores six fields and generates
naive local `cache_time`; its String column and Answer.answer's `Any` annotation
are not permission to stringify arbitrary objects. No answer_type argument is
supplied by this API; do not invent/persist a cached Answer type tag by fetching
the parent. Plain text and direct A07 native values are the supported surfaces.

### 13.2 Granularity, assumptions and rejected alternatives

Baseline **one immutable complete report per append**, separate from its
question, plus a fixed-size report-ID high water in existing `gptc_meta`.
This is driven by independent history and append isolation, not a SQL-table
translation. Each input event is finite but not arbitrarily capped; reject a
too-large event clearly. No record accumulates all events.

| Decision / reasoning | Alternatives rejected or deferred | Evidence to reopen |
|---|---|---|
| G3-D1 independent complete copied event | Parent embedding violates A05 when purged and grows per hit; a reference-only event loses historical values after purge | Changed retention/erasure or actual timeline requirements, renewed stakeholder decision |
| G3-D2 per-event create-only record | One scope/question list or singleton registry grows unbounded; time buckets without retention/rate/size inputs have no justified bounded width; hash buckets need a measured population/overflow plan and couple append failures | Measured PI pressure/size/rates plus bounded rollover, atomic append-isolation/recovery proof and review |
| G3-D3 one immutable typed payload blob | Extracting copied questions/answer/score/time into indexable bins adds no required access path; no field-level mutations or independent lifecycle exist | Required server filter/projection/index/update, not speculative reporting |
| G3-D4 distinct positive-int64 rid high water | Question/sid are different identities; content-key dedup loses duplicate appends; process counters/random unchecked puts can overwrite. Same proven conditional allocator primitive avoids a new ID algorithm | Actual unsupported durability/history, exhaustion, measured allocator contention; do not recycle |
| G3-D5 no secondary/expression index or materialized timeline | No host report reads/query/cleanup path; an index creates write/PI/permission/readiness burden for no requirement | Actual approved inverse/timeline/erasure access path with cost evidence |

The small-record gate is **exploratory unresolved for production**: unknown
population/bytes do not prove “modest” population or affordable PI. D2 is a
safe finite-event baseline under A01, not a measured optimal model. Consolidation
is **DEFERRED_OPTIMIZATION**, not a hidden assumption that events are large.
No MRT, path expressions, expression indexes, nested CDT, ordered-CDT persist
index or time-series bucketing is used: no required read/mutation needs them.

### 13.3 Exact scope/key, bins, codec and examples

Use §3's configured namespace, exact scope string and canonical `J` recipe:
`json.dumps(parts, ensure_ascii=True, separators=(",",":"), allow_nan=False)`.
Key format version is 1. No delimiter concatenation, time component or inferred
tenant. `DataSet.of(namespace,"gptc_r").id(J(["gptc",1,scope,"r",rid]))` identifies a
report. Its allocator is
`DataSet.of(namespace,"gptc_meta").id(J(["gptc",1,scope,"rid"]))`.
`rid` is int-not-bool in `1..9223372036854775807`, unique over intact supported history (§14) within
namespace/scope/report-kind, independently of qid/sid. Gaps are allowed.
The same number in another scope/namespace/kind is a different identity.
No public report ID/read method is added.

| `gptc_r` required bin | Physical type / mutability / why necessary |
|---|---|
| `fmt` | int-not-bool exactly 1, immutable; record/codec/key compatibility |
| `scope` | exact string, immutable; all-record ownership/integrity and scoped inventory |
| `rid` | positive int64, immutable; append identity and physical digest validation |
| `payload` | blob of typed JSON below, immutable; all six supplied fields plus generated report time, copied independently of parent |

No optional bins. Unknown/missing/extra codec fields or malformed types/version
are explicit incompatibility/corruption, not a miss. The allocator has only
§4's `fmt=1`, `scope`, required root-matching `boot` (§14), `hi` int-not-bool `0..MAX_INT64`, immutable identity and
monotonically increasing hi. All names ≤15 ASCII bytes. `qid` inside payload
is a historical scoped identifier, **not a foreign-key constraint** or a signal
to read/touch/admit/decrement the parent.

`payload` is compact UTF-8 JSON bytes, `ensure_ascii=True`, `allow_nan=False`,
with exactly these members (unique keys; object order not semantic):

`{"v":1,"user_question":str,"cache_question":str,"cache_question_id":int,
"cache_answer":node,"similarity":node,"cache_delta_time":node,
"cache_time":str}`.

- Both question strings are required, empty allowed, exact Unicode preserved.
  The cached ID obeys the positive-int64 qid type boundary, not an existence
  check. Reports may be appended after physical parent deletion.
- `cache_answer` uses **exactly §4's native-node codec** for the supplied
  A07 value, including plain str, None, bytes, empty values, signed-int64 int,
  finite float/negative zero, bool, list and string-keyed dict with order.
  Adapter `"cached"` becomes `["s","cached"]`; it is not an Answer wrapper.
  Report format/native node tags preserve the types actually supplied.
  No answer_type is supplied, inferred or copied from the cached parent;
  the report API cannot preserve an unsupplied DataType tag. Direct `Answer`
  DTO objects are unsupported here, as are nested Answer values: no duck typing,
  arbitrary class, `__dict__`, dataclass recursion, pickle, object fallback
  or `str(obj)`. Text/image strings/bytes/reference values can be copied,
  never fetched or executed. `Any` does not expand the native allowlist.
- Similarity and elapsed `cache_delta_time` are finite Python floats or
  signed-int64 ints (not bool), encoded as §4 `["f",float.hex()]` or `["i",text]`
  nodes. No string/Decimal/numpy-scalar/object coercion. Adapter already casts
  rank to Python float; `round(time.time()-start_time,6)` is Python float seconds.
  Name `cache_delta_time` does **not** authorize `datetime.timedelta` or a
  datetime answer. No [0,1] similarity or nonnegative elapsed restriction is
  invented: evaluators vary and wall clocks can roll back. Preserve supplied
  numeric value, not source decimal spelling.
- Generate `cache_time` once for this attempted append using naive client-local
  `datetime.now()`, §4's exact microsecond grammar. It is SQL-style wall time,
  not authoritative ordering or a caller-supplied instant; no extra supplied
  timestamp argument exists. No unused SQL `extra` bin is copied.
- Prevalidate and encode the complete event before avoidable allocation/writes.
  A07 recursion/cycle/resource failure and oversize are clear errors, never
  truncation/chunking/dropping a field; no invented accepted depth/size cap.
  Actual configured record limit includes bins/key/record overhead; if local
  size cannot be proved safe, authoritative server oversize failure is surfaced.

Synthetic examples, **not measurements, bounds or an extra display codec**.
`$blob_utf8` denotes bytes of the shown JSON, as in §4:

```json
{
  "namespace": "<configured>", "set": "gptc_r",
  "key": "[\"gptc\",1,\"demo\",\"r\",81]",
  "bins": {
    "fmt": 1, "scope": "demo", "rid": 81,
    "payload": {"$blob_utf8": {
      "v": 1, "user_question": "user café", "cache_question": "cached café",
      "cache_question_id": 7, "cache_answer": ["s","cached"],
      "similarity": ["f","0x1.8000000000000p-1"],
      "cache_delta_time": ["f","0x1.0000000000000p-3"],
      "cache_time": "2026-01-02T03:04:05.123456"
    }}
  },
  "expiry": "explicit never-expire"
}
```

```json
{
  "namespace": "<configured>", "set": "gptc_meta",
  "key": "[\"gptc\",1,\"demo\",\"rid\"]",
  "bins": {"fmt":1,"scope":"demo","boot":"00000000-0000-4000-8000-000000000001","hi":81},
  "expiry": "explicit never-expire"
}
```

### 13.4 Access/atomicity, SDK mapping and result contract

Every keyed operation inherits §5's explicit missing/filtered visibility,
`ErrorStrategy.IN_STREAM`, complete draining, key/index correlation, embedded
exception/`is_ok`/result-code/in-doubt checks and redacted diagnostics.
Implicit transactions are opted out with `.with_txn(None)`; effective
WRITE_NON_RETRYABLE/POINT retry count is **zero**, including configured
overrides/reloads/wrappers. Defaults alone do not prove it.

| Path | Concrete operations / projections / outcome |
|---|---|
| `create` report allocator | §14 validation-only create reads ACTIVE root and all three allocators incl. matching boot, point read-touch disabled and exact physical identity; only explicit operator init/resume may create missing phase0 owned metadata. ACTIVE missing rid is always history error, never zero/rebuild |
| `report_cache(user_question, cache_question, cache_question_id, cache_answer, similarity_value, cache_delta_time)` reserve | After §14 activation and complete local validation/encoding, existing-only `session.update(rid_key).where(typed_predicate).add("hi",1).get("hi").never_expire().with_txn(None)` with §5 visibility. Predicate exact scope, fmt=1, boot match, typed `0 <= hi < MAX_INT64`; atomic returned H at **raw slot 1**, write slot 0 ignored. Validate int-not-bool `1..MAX`; no guessed ID from later reads |
| Append | `session.insert(report_key).put(full_bin_mapping).never_expire().with_txn(None)` with §5 visibility. One create-only same-record operate writes every bin, atomically publishing complete immutable event. No parent lookup/write, cross-record transaction or CDT append needed |
| Success | Exactly the original correlated acknowledged OK outcome with no exception/in-doubt, fully drained; return `None` as SQL does, not rid/list/success boolean. Write-only slot None is normal, not a requirement to return full payload |
| `get_data_by_id`, `list_sessions`, count/IDs | No report read/projection, no joins or change to DTO. Question-only count/IDs never includes `gptc_r` or any allocator |
| mark / question purge / session drop / session-purge recovery | No report mutation or deletion. All-group initialization/history recovery still inventories report identities and metadata. §12 fences and owned-sid cleanup remain mandatory; report writes do not acquire admissions or prevent parent deletion |
| `flush` / `close` | Synchronous acknowledgment only, no hidden buffer. Flush cannot settle/replay uncertain append. Close releases the existing lifecycle-owned session, never deletes report/meta data or heals uncertainty |

No normal report read/query API is added. **Validation-only** diagnostic point
reads by an already known rid may request `fmt,scope,rid,payload`; they validate
the complete payload and reconstructed physical key/digest before interpreting
it. No parent assembly is necessary. Pristine/recovery inventories include the
report set, root and all three allocator keys; project identity/version/scope then full
payload wherever integrity is being certified. All-record completeness and
external references remain actual-evidence obligations, not a new cleanup API.

Source-verified SDK shapes, not legacy-client guesses: `dataset.py:95–114`
key construction; `session_shared.py:386–460` create-only insert/update-only;
`operations_shared.py:
149–159` existence mapping, `416–428` never-expire, `573–585` visibility,
`621–634` per-bin puts, `654–664` ordered add/get; `query_shared.py:712–746`
txn opt-out and `1918–1967` nonretryable policy/TTL/predicate resolution;
`sync/operations/query.py:529–550` IN_STREAM avoids default fast bypass;
`record_result.py:124–143,193–220` OK requires no exception and raw positional
results preserve None. Paths are SDK package-relative. Actual transport/server,
restart, limits, read-touch and released compatibility remain A09 gates.

### 13.5 Failures, recovery and independent retained history

| Boundary | Visible outcome / allowed continuation |
|---|---|
| Invalid safe value/tag/question/ID/number or clear local oversize | Raise before allocation where avoidable, no success-shaped return or silent field drop |
| Reservation proven rejected or exhausted | No report send; error correlated to this append. Exhaustion requires reviewed migration, never wrap/reset |
| Reservation issued, reply missing/error/in-doubt/malformed | No guessed rid and no report send. Range may be consumed; disclose uncertainty, never replay reservation to “recover” it |
| Reservation acknowledged, failure before report send | Known reserved rid/gap; no report persistence claimed. Process crash can leave the gap forever |
| Report create proven non-applied rejection | Raise with rid correlation and known rejection. Allocator stays advanced; no rollback/reuse |
| Report exists unexpectedly | Integrity/reuse conflict, never overwrite, content dedup or silent success; stop and investigate allocator/history |
| Report send timeout/crash/lost/malformed reply/interrupted stream | Original create may persist fully or not. Raise/retain unknown outcome with known rid/key/call context where available; no automatic retry/new ID/compensating delete. Later existence is not its original acknowledgment |
| Create acknowledged, caller loses response or elects another call | Event is retained; a new explicit invocation gets a new rid and can duplicate content. No exactly-once/idempotency-token or public replay guarantee |

Report failure correlation exposes namespace/scope identity, kind, attempted rid
when known, operation phase, known/unknown persistence, code and in-doubt status,
**not** copied text/answer/blob or raw result repr. An iteration/transport
failure with insufficient terminal evidence is not proven rejection merely
because an SDK row lacks `in_doubt=True`. Cross-record allocator/event atomicity
is neither required nor claimed; unused IDs are harmless gaps.

Pristine initialization **must include retained reports, rid/sid/qid high waters,
questions/sessions and external IDs**. A scope with no questions but retained
history is not pristine. Do not reset/reconstruct high water from observed
maxima or initialize missing metadata automatically. History loss/restore,
mixed writers or unsupported durability blocks use pending reviewed integrity
recovery/revision. No recovery permission changes or mechanism is invented.

Reports are complete historical copies at append time. Parent q7 can be marked,
physically purged and never reused; report r81 retains its values without
dereferencing q7. The API has no report read contract that could return a broken
question DTO. Its historical ID may refer to no current question, intentionally.
Reports can contain user text/image references after cache deletion; cache purge
is **not erasure of copied history**. A05 explicitly excludes report TTL/query/
cleanup in MVP. Do not call this a legal-retention, right-to-erasure, backup
retention or compliance solution. Any such requirement needs a new lifecycle/
access-path decision before implementation, not hidden expiration or pruning.

### 13.6 Growth, PI, write-amplification and hot-key worksheet

Let R(T) be retained reports in a scope after horizon T, S the number of scopes,
RF actual replication factor, B the distribution of **logical uncompressed**
full report bytes including metadata/encoded payload, K repeated scope/key bytes,
M fixed rid-allocator bytes. All actual values/distributions below are UNKNOWN.

| Worksheet item | Formula / disposition |
|---|---|
| Event encoded size | B = overhead + UTF8(J(payload)) bytes + fmt/scope/rid/bin overhead; escaping/base64/type tags and repeated copied text must be measured, not source string length |
| Growth / retention | `R(T)=R(0)+integral(accepted_append_rate(t),0..T)` absent external loss; no expiry subtraction or finite horizon assumed. Failed-unknown writes may also contribute; observed exact R(T) not supplied |
| Data/storage projection | `sum(B_i)+S*M` logical bytes, multiplied by RF for replicated logical data; compression/physical/defrag overhead separately measured. No forecast number or capacity guarantee |
| PI cost | Incremental report+allocator cluster PI `64*RF*(R(T)+S)` bytes; ratio per report `64/B`, aggregate `64*R/sum(B_i)` before allocator cost. Unknown, potentially double-digit for small events |
| Repeated-ID/scope bytes | `R(T)*K` plus qid representation in each payload; K and repetition p95/p99 UNKNOWN. Numeric rid is interoperable identity, not a measured byte-savings claim |
| Consolidation comparator | For hypothetical b events/record, PI `64*RF*ceil(R/b)` plus metadata; record size approximately b*B and each append rewrites that record. b, safe growth/rollover and benefits UNKNOWN; not adopted |
| Atomic work per append | One fixed-meta guarded add/read plus one full event create, zero report rewrites thereafter. Allocator is per-scope shared hot key; all report payload keys distribute independently |
| Contention | `qps=events/active_window`, `burst_qps=qps*burst_factor`, `lambda=burst_qps*contention_window_ms/1000`, `P(>=2)=1-exp(-lambda)*(1+lambda)`. Inputs/outputs/SLO/busy/latency all UNKNOWN, no benchmark run |
| Size thresholds | Compare measured B p50/p95/p99/max to guide §Storage/Record size limits, desired low-single-digit-KiB bulk and actual configured max-record-size. No numeric application cap derived. Large >~50KiB events need explicit measured write-once I/O justification; not target-size evidence |
| Overflow response | Known local resource/encoding/size errors or server record-too-big/capacity failures raise clearly. No truncation/chunking, dropping reports, overwriting old events, hidden TTL or silently treating failure as success |
| Reopen triggers | Measured PI/capacity pressure, allocator busy/latency, payload size tails/rejection rate, exhaustion, actual retention/erasure/query needs or unsupported deployment guarantees; obtain new evidence and review, not automatic sharding/cleanup |

**Production sizing verdict: BLOCKED_MISSING_INPUT under A01's approved
exploratory disposition.** Unknowns are allowed for this draft, not production
admission limits. No application collection bin grows per append; nested native
containers are finite immutable event inputs subject to clear size/resource
failure, not persistent unbounded report lists. Global set growth is explicitly
unbounded by retention and must be capacity-planned before deployment.

### 13.7 Developer walkthroughs — author paper traces only

**H1 ordinary adapter hit + diagnostics (create/read).** In scope `"demo"`,
qid 7, adapter passes the plain answer string `"cached"`, score 0.75,
elapsed 0.125 seconds.
Validate all six values, capture naive cache_time, encode the exact example.
Guarded rid meta add/get returns raw slot 1 = 81; create-only r81 atomically
stores every bin, never-expire, txn opted out, effective zero retry, §5 visible
result. After full acknowledged success return None. No public read follows;
validation-only known-key read decodes the complete event independently of q7
with read-touch disabled. Direct `""`/native values use §4 nodes, not an invented
Answer/type-tag wrapper. Unsupported object/Answer/NaN/timedelta fails before allocation.
**Paper result: supported; codec plumbing/policy enforcement are normal (b)
implementation work, not a new access path or executed test.**

**H2 shared append, scope isolation, lost replies (mutation/failure).** Two
processes in demo atomically reserve 82 and 83 even for identical fields;
scope `"other"` may independently have rid 82 at a different key. No qid/sid
allocator is consumed. P1 loses r82's create reply: unknown original outcome,
rid 82 retained in error correlation, no blind re-send or compensation. P2's
r83 acknowledged result succeeds independently; a transport failure after a
prefix is not all-success. An explicit later caller invocation can create r84
with identical content and must be understood as a potentially duplicate event.
Lost allocator reply authorizes no guessed rid/event. Raw True/None/out-of-range
slot fails, not coerced to an ID. **Paper result: supported under authoritative
durable allocator history; RA-2 remains open, not runtime proof.**

**H3 question purge, no broken history, reopen/recovery (cleanup).** r81 exists
and another report creation pauses. q7 marks -1; §12 blocks new session entries,
waits/fails closed for unresolved admissions, cleans owned sids only with its
closed/zero fence and complete settled cleanup, then guarded-deletes q7.
Neither r81 nor pending report append consumes an admission; the pending append
may succeed after deletion with its copied values/qid7. Question counts exclude
both reports. Name-drop/flush/close do not delete history. Reopen finding no
questions but r81 must not zero any allocator; missing rid metadata is recovery
error. Scope-other records are untouched. Exceptional session recovery still
refuses all mutations without actual exclusion/settlement proof, unrelated to
report retention. **Paper result: supported conditionally; all HIGH capability
blockers remain unresolved and no parent fence is weakened.**

### 13.8 Author self-check and historical independent-review stop

Failure-mode checks: granularity follows independent history/append isolation
with explicit PI alternative, not domain nouns; no speculative SI; no collection
read-modify-write; fixed bin count/names; copied history needs no parent join;
no growing scope registry or report list; symbolic tiny-record PI cost remains
production-blocked; prior conditional checkpoint is genuine and this group has
not been awarded independent/stakeholder approval. Exact scoped keys, all six
inputs, plain-string/native-node encoding, elapsed seconds, generated wall time, raw
allocation slots, no-expiry/read-touch rules, partial failures and non-reuse
are specified. This is an **author self-check**, not independent acceptance.

The read-only AST/source assertion caught an initial author misreading of the
adapter argument as the Answer wrapper. This draft was corrected before
handoff: both call sites pass `.answers[0].answer`, not `.answers[0]`. No
wrapper encoding or unsupplied Answer type tag remains in the report contract.

**Historical stop, superseded by the actual report checkpoint in §1 —
DRAFT_AWAITING_INDEPENDENT_IMPLEMENTER_REVIEW.** A fresh reviewer who
did not design this group must inspect §13 against A01/A05/A07/A08/A09, actual
host callers and SDK builders/results/policies, and the binding §12 cross-group
interface. Triage every actual finding with spec+requirement anchors. Only then
ask the actual stakeholder to approve/revise/ask follow-ups/explicitly defer
this exploratory report model with retention/capacity/duplicate limitations.
No actual report approval or independent verdict is recorded here. Final
derivation/overall review, schema summary, model-evidence pass, plan SUMMARY
and persistent/test code all remain prohibited at this checkpoint.

## 14. Proposed supported operator contract — pending final review

This section is an **unimplemented contract-design revision**. Completed
synthetic primitive/controller preflight, including corrected P2 successor
handling, supports WM-1/WM-2/WM-3 model feasibility, not final-provider tests
or independent closure. Clarification §8 is the actual approved envelope;
this root record, helper interface and protocol still need independent review
and final user acceptance. §12 remains normative for admission and ordinary
purge; failed historical §11.5–11.7 never becomes an implementation algorithm.
§14 overrides conflicting bootstrap, universal-history and unspecified recovery
context language elsewhere in the guide.

### 14.1 Supported history and configuration boundary

Support normal concurrent CRUD with trusted protocol-compliant writers and
operators, intact persistent scalar history, a matching new or consistently
restored vector store, and verified effective configuration. Identity never
wraps, resets or reuses a reservation during that history. Reports remain
independent retained history. Known missing/corrupt root, allocator, controls or
inconsistent restore fails closed; stop the affected scope and investigate.
No automatic missing-metadata=0, survivor-max reconstruction or implicit
reinitialization is permitted, even when all question records are gone.

The initial concrete maintenance adapter is **single-host, single-node,
file-backed Docker**, with a complete trusted launch/admin inventory. It is not
a generic cluster/cloud recovery protocol. Normal public service is allowed;
exceptional recovery requires the whole-deployment outage/transition below,
not merely isolating one client from the still-public service. Uninventoried
remote nodes, shared storage writers, XDR/import/restore, external namespaces
with incompatible shutdown needs or multiple independent control planes refuse
this adapter pending separately reviewed support.

Effective activation checks must bind namespace mode/RF, active nodes and
partition availability, replica/read/commit policies, zero retry on all
non-idempotent paths, no read-touch/expiry, durable delete, complete enumeration,
record size and permissions. The observed development AP RF=1 configuration
demonstrates selected guarded operations and clean-restart behavior only.
COMMIT_ALL and create-only do not turn AP into partition-safe allocation.
Do not write through a known unavailable/partitioned history; no guarantee of
available partition writes or automatic partition detection/reconciliation is
offered. Power-loss durability, stale-node history, out-of-band metadata
deletion, inconsistent restores and malicious Docker/root administrators are
outside the approved guarantee. Successful clean stops are required for this
recovery adapter; forced/crashed stops are not a supported substitute.

Trusted operators preserve root, all three allocators (including unused
reservations), q controls/payloads, s/r records and corresponding vector IDs
together. A restore must explicitly establish a consistent scalar/vector
recovery point while writers are excluded, discard later vector references and
never mix old scalar history with newer vectors. An unverified/inconsistent
restore cannot be repaired by bootstrap or session-purge recovery. The provider
does not implement vector backups or prove that an administrator told the truth.
No production partition/power-loss/restore campaign is inferred from capability
inspection. Released `aerospike-sdk` / `aerospike-server` compatibility remains a
separate gate; this draft states no verified released version.

### 14.2 Minimal bootstrap root and owned allocator additions

Add **one** constant-size header in existing `gptc_meta`, not a scope registry.
Key `J(["gptc",1,cache_scope,"root"])`; root is never purged/TTL'd/reset.
The ordinary metadata key recipes for qid/sid/rid are unchanged.

| Root bin | Exact physical type / contract |
|---|---|
| `fmt` | int64 exactly 1; root grammar selected by `"root"` key |
| `scope` | exact configured string |
| `boot` | canonical lowercase UUID string, immutable initialization identity (authority is operator intent, not UUID randomness) |
| `bind` | lowercase 64-hex SHA-256 of canonical non-secret deployment/vector binding manifest; immutable |
| `phase` | int64 0 INITIALIZING or 1 ACTIVE, single one-way 0→1 |

All qid/sid/rid allocators add required immutable `boot` with the root's exact
value, alongside existing `fmt,scope,hi`. Missing boot is incompatible, not an
automatic migration default. Root and allocators use never-expire, transaction
opt-out, original correlated outcomes and effective zero retries as in §5.
Unknown bins/versions or wrong exact types fail validation as specified for
metadata. The `bind` digest is integrity correlation, not authentication.
No IDs/writers/targets grow in these records; the operator journal is external.

Example additional metadata record (other sets retain their existing examples):

```json
{"set":"gptc_meta","key":"[\"gptc\",1,\"demo\",\"root\"]","bins":{"fmt":1,"scope":"demo","boot":"00000000-0000-4000-8000-000000000001","bind":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","phase":0}}
```

An initialized allocator example is existing qid key with
`{"fmt":1,"scope":"demo","boot":"00000000-0000-4000-8000-000000000001","hi":0}`.
One extra primary-index entry per scope costs `64*RF` bytes; all metadata PI is
now `4*64*RF*scope_count`, plus small fixed payloads. No normal hot-path root
rewrite: create validates it; allocation still contends on each kind's own key.
Update prior three-metadata sizing formulas by adding one root per scope.
Strings boot/bind are bounded, scope follows existing exact-string validation.

### 14.3 Exact provider-only surface and activation state machine

Proposed new module `gptcache.manager.scalar_data.aerospike_operator` (no file
implemented here) exposes:

* `DockerMaintenanceOperator.from_manifest(manifest_path, *, journal_dir)`
* `operator.initialize_scope(*, namespace, cache_scope, intent_id, vector_binding)`
* `operator.resume_initialization(*, intent_id)`
* `operator.recover_session_purge(*, namespace, cache_scope, qids)`
* `operator.finish_maintenance()` and `operator.status()` (status never mutates).

Source-file handoff: public API/CLI/controller in
`gptcache/manager/scalar_data/aerospike_operator.py`; private fixed runtime/
context in `gptcache/manager/scalar_data/_aerospike_operator_runtime.py`;
provider invocation/activation in existing `aerospike_storage.py`; new operator
unit suite `tests/unit_tests/manager/test_aerospike_operator.py` plus existing
provider suite. These are authorized later task 01-02.3 files, not implemented
here. Optional operator tooling is not loaded/provisioned by ordinary CRUD.

CLI is proposed `python -m gptcache.manager.scalar_data.aerospike_operator`
with fixed subcommands `status`, `init-new`, `resume-init`, `recover-purge`,
`finish`; structured `--manifest`, `--journal-dir`, `--intent-id`,
`--namespace`, `--cache-scope`, `--vector-binding` and `--qids-file` inputs.
`init-new` additionally requires literal `--declare-new-logical-cache`.
All schemas/options are allowlisted; no shell, eval, command/entrypoint option,
Python import/factory hook or arbitrary Docker argv in user input. These are
new provider-specific interfaces, not flags on shared host code.

Before constructing the manager (which calls create/get_ids during startup),
an operator must run init-new/finish successfully, or resume/finish a known
interrupted initialization. Provider `create()` becomes validation-only:
point-read root and all three allocators; require ACTIVE, matching boot, exact
identity/types and intact history. No writes/zero-fill from create. All
persistent methods including get_ids/count and any allocator entry require
successful activation validation on that provider instance; close still works
and flush is no settlement barrier. Metadata loss noticed later invalidates
activation. Concurrent normal constructors may all validate ACTIVE without
ownership or resetting anything. Shared `CacheStorage` API stays unchanged.

Initialization under exclusive maintenance ownership:

1. Acquire deployment lock/gate (§14.4), durably record explicit NEW intent,
   boot, canonical binding and target namespace/scope **before** root creation.
   Operator declares this a new logical cache with new/consistent vector store.
   Inventory q/s/r/all four exact metadata keys for conflicting/corrupt owned
   data; survivors conflict, but absence alone is not historical authority.
2. With no prior intent/root, create root phase=0 **create-only**. If any
   allocator already exists without this root/intent, refuse; never adopt it.
   A lost create reply requires settlement before exact-root inspection.
   A recorded root-create attempt may resume if its exact owned root exists;
   if absent after any prior attempt, the initial adapter conservatively
   **refuses**, even if no root was acknowledged. Safe refusal is supported,
   not an implemented/proven absent-root retry capability or a recovery SLA.
   Any future retry capability requires separately reviewed proof. If acknowledged
   root disappears, fail closed as lost history, not new intent.
3. Only a matching durable intent + exact phase=0 owned root authorizes
   create-only absent qid/sid/rid records with hi=0, boot match, full bins.
   Validate existing ones have the exact boot and hi=0; never overwrite.
   Crash after 0/1/2/3 allocator creates resumes this same procedure after a
   new settlement boundary, under the same intent. No q/s/r may exist; normal
   operations cannot reserve against phase=0. hi>0 or any owned data refuses.
4. Re-read all three complete allocators, validate ownership/zero and complete
   all-group inventory; then existing-only guarded root update
   `where(fmt/scope/boot/bind exact AND typed phase==0).put({"phase":1})`,
   never-expire/txn opt-out. Original known OK activates; uncertainty keeps
   maintenance closed until settlement and exact reinspection. ACTIVE with
   all intact metadata may complete the journal; it never authorizes zero-fill.
5. Once ACTIVE, missing/corrupt **any** allocator is history error even if hi
   would plausibly be zero. No new intent can turn ACTIVE back to INITIALIZING.
   Keep the durable initialization receipt/binding; release launch gate only
   after outcomes are settled. Later constructors only validate.

Concurrent initializers serialize through the deployment-wide host lock and
root create-only ownership. A second intent seeing another root refuses;
the same intent validates the winner, never resets it. Different host locks
against one deployment are unsupported; trusted admins share the one gate.
Empty surviving records do not entitle a different intent to “take over”.
No root/allocator automatic upgrade of pre-root data is specified; migration
requires separate explicit versioned, quiesced review.

### 14.4 Cross-operator gate, deployment binding and real transition

The supported adapter uses local Docker **CLI**, not an assumed Docker Python
SDK or provider-side access to the daemon. Operator already has authorized
Docker control of the named deployment; namespace clients are numeric
unprivileged users. Do not grant Docker socket/root/network capabilities to
provider processes or mount the Docker socket into a container.

Manifest binds daemon/host identity, exact original container ID, image content
ID (not mutable tag), canonical persistent file mount identity/source/target,
namespace/storage config hash, validated network-only maintenance config delta,
server entrypoint, provider/runtime payload hashes and numeric uid/gid. Also
bind namespace/scopes/vector store identity, all launchers/writer versions/
queues/direct tools/admins, scheduler/restart disable actions and original
restore settings. No passwords/TLS keys or payload text in this journal.
Scope/qids are validated structured input bound to a per-attempt nonce.
Do not follow symlink-changing mount/config/runtime paths or accept a new
manifest identity mid-attempt. Inventory must cover Docker-admin exec/run,
host clients of public ports, service managers, application queues and manual
launch paths. Trusted admins explicitly honor the gate, including no alternate
container mounting the data. This is not protection from malicious root.

One stable deployment-owned host directory, restrictive ownership/mode,
contains the lock and atomically written/fsynced maintenance journal (including
directory fsync). `flock(LOCK_EX|LOCK_NB)` on its fixed lock inode serializes
operators; never unlink/replace that lock file. Owner/trusted admins use the
same directory/daemon identity. Journal state `MAINTENANCE` survives process
death; disappearance of the lock holder does **not** release the launch gate.
All trusted launchers/admins must refuse application/server restart while that
journal is set; no gate compliance is inferred from Docker inspect alone.
Durability here covers operator-process crash with intact host storage, not
power loss. A missing/corrupt previously known journal fails closed.

Concrete entry from an owned public service, not from an already-hermetic toy:

1. Under the lock, persist MAINTENANCE and complete launcher/admin inventory;
   disable service/scheduler launch paths by their reviewed deployment actions.
   Set original container restart policy `no`; stop all inventoried old writers
   and invalidate their queues/admissions. Do not merely pause then resume them.
   Unreviewed launcher adapters or undeclared host privileges are a refusal.
2. Fsync stop intent **before** `docker stop --time 60 <original-id>`, binding
   exact container/image, StartedAt, server PID and old process inventory.
   Live completion **and already-stopped successor** require exited/not running/
   restarting/dead, exact integer PID0/exit0/noOOM, valid nanosecond UTC
   FinishedAt > StartedAt, matching durable intent and terminal old processes.
   Use `docker logs --timestamps --since <StartedAt> --until <FinishedAt>`;
   independently verify clean-marker timestamps within that incarnation interval.
   Reinspect exact state/identity after logs to reject restart/inspect races.
   Verify inventoried exec/host writers cannot restart through trusted paths.
   Missing intent/evidence, exit137/forced kill or ambiguity refuses. Merely
   already-stopped/exit0, an old clean substring or elapsed time is not settlement.
3. Public PortBindings cannot be wished away on the existing container.
   Keep it stopped/gated; prepare with `docker create --pull=never --network none
   --restart no ...` for a **new maintenance container**, no published ports,
   same authorized persistent file mount, exact bound image and validated
   namespace/storage configuration (network-only loopback fabric delta).
   Precreating this container **STOPPED** is equivalent and permitted. Every
   maintenance **START** must follow the verified public/current-owner clean
   stop; creation-after-stop is not itself a safety requirement.
   No second server opens this data concurrently. Preserve original container
   as stopped inventory; no removal/truncate/copy-as-empty/disk reformat.
4. Start maintenance server only. Inspect exact ID/image/mount/config, network
   none/no port bindings/no restart and allowed process inventory. Persist fresh
   server incarnation/evidence before running namespace clients over container
   loopback. Old server/writer TCP/exec processes are gone; no old request can
   resume in this new server incarnation. Original history survives via the
   **same** bound data, not a new empty namespace. Verify read-only readiness,
   namespace settings and intact root/allocators before any recovery write.

Whole-process termination + clean history transition supplies settlement, not
the host lock. Isolation + compliant launch/admin gate excludes fresh writers.
This supports a real operator path against authorized persistent data; future
verification must instead use ONLY NEW disposable original/maintenance data.

### 14.5 Actual SDK runtime, context construction and purge sequence

Only the operator controller launches a fixed, hashed provider runtime payload
using `docker exec --user <bound-numeric-uid:gid> ... <fixed-interpreter-argv>
-m gptcache.manager.scalar_data.aerospike_operator <internal-fixed-mode>`.
Internal mode receives a bounded structured request via stdin/pipe, not a shell
command or credential-bearing argv. Manifest cannot provide arbitrary program
names. Read-only code/config/runtime mounts and minimal writable temp paths
must be preflighted; no network install or image pull at recovery time.
The SDK cluster definition inside that runtime is concretely
`aerospike_sdk.sync.ClusterDefinition("127.0.0.1", 3000)` connected there,
not the old host public endpoint. Session/policies obey all existing model
rules; exact public SDK-only configuration/query contract is specified in §6.1.

The controller creates an internal `DockerMaintenanceContext` only after live
lock ownership, journal, identity/isolation/process checks and fresh settlement
evidence succeed. It binds controller attempt/nonce, daemon/container/data/config/
runtime identity, namespace/scope, exact qid set, boot, current server incarnation
and sole runtime PID. It is not deserialized from a user “verified” object or
bool, nor reusable in ordinary host-provider instances. The fixed runtime
constructs a new provider attached to the isolated loopback server, validates
activation and invokes the retained provider method
`recover_session_purge(qids, *, recovery_context=context)`.
Controller retains lock/gate for the entire invocation and rechecks boundary
around dispatch/completion. Trusted-admin gate covers the interval between
checks; no unattested instantaneous Docker security barrier is claimed.

The provider validates exact context/target/instance, not a Python truth flag;
then §12.7's sequence remains: exact tombstoned parent validation; complete
owned-sid inventory; exact existing-only deletes with settled results; complete
empty verification; guarded typed tombstone `put adm=0/get adm` (raw slot1 exact
int zero); reverify empty; guarded closed/zero final delete. No active-parent
reset, report/vector/meta mutation, remembered decrement or hidden retry.
Per-qid known/failed/unknown phases are returned, never full-success for a
prefix. A target genuinely absent after valid settled history is idempotently
absent, not a reason to create a parent. Invalid root/allocators refuse recovery.

Operator crash, lost runtime output, uncertain mutation or lost boundary check:
keep MAINTENANCE, no application restart; successor reacquires the SAME lock,
invalidates all prior contexts, inventories/stops any surviving recovery runtime
and **clean-stops/restarts the entire current maintenance server again**.
Already-stopped successors use the same persisted-intent/log/terminal-process/
reinspect validation as live stops; missing evidence refuses. Verify new
current-incarnation evidence before re-inventory/reset. Even a prior
remembered zero/delete is not trusted settlement. If no clean boundary can be
established, retain gate/tombstones and refuse; do not manufacture success.
The same rule applies to interrupted bootstrap, including uncertain activation.

`finish` requires no outstanding runtime/command outcome, clean stop of the
maintenance incarnation, verified preserved data and disclosed per-target
status. Persist completion, restore reviewed original launch settings, start
only original server on the same data (never simultaneously), verify effective
config/metadata, then clear MAINTENANCE and permit only fresh compliant
application processes/queues. A crash at any finish stage retains/reenters
maintenance and requires a fresh clean settlement cycle before further changes.
Original application processes/admissions are never resumed. Partial targets
remain visible tombstones; finish must not relabel them as recovered.

### 14.6 Allowlist, refusal tests, evidence and feasibility prerequisites

Allowed Docker verbs: inspect/image inspect/top/logs; update restart=no (and
reviewed restore); stop/start on bound IDs; create maintenance container with
fixed image/mount/config/network-none/no-ports/restart=no; exec of the fixed
runtime. No `sh -c`, arbitrary shell strings, Docker API socket proxy, pull,
privileged mode, host networking/PID namespace, raw block devices, volume
prune, rm/truncate or arbitrary info/set-config command. Launcher adapters are
explicit fixed actions approved in the manifest, not arbitrary shell strings.
If a real deployment needs an absent adapter/permission, report that exact
feasibility gap and stop. Current approval does not grant new sudo privileges.

Do not log raw Docker inspect environment, request payloads, raw database repr,
stdout/stderr containing credentials, authorization/TLS keys or user texts.
Persist redacted allowlisted identity/status/exit/time/hash fields, operation
phase/qid correlation and structured terminal errors. Runtime/config files may
reference protected local credentials without embedding their contents or
paths containing secrets in argv. Unexpected output is a redacted failure,
not wholesale capture to a public evidence file.

Final-provider integration/unit cases required in 01-02.3/01-03 (not executed
by this document; synthetic feasibility evidence is qualified below):

* Public-port original → clean stop → network-none maintenance using SAME
  disposable file mount → purge → clean finish/reopen; prove old admitted
  writer cannot resume, no late child, intact allocator/report/vector fixture.
* Concurrent init/operator attempts; competing intent refusal; crash after
  root and each allocator, lost create/activation reply; matching phase0 resume;
  phase1 missing allocator and missing known root always fail, no zero-fill.
* Controller death with live runtime; successor gate reacquisition then new
  clean incarnation before inventory/reset; crash during reset/delete/finish;
  no remembered decrement/replay or accidental public application restart.
* Forced kill/unclean exit, stale log, port/network/restart drift, wrong image/
  mount/namespace/boot/scope/qids/runtime/uid, undeclared writer/launcher/admin,
  conflicting root, corrupt journal, incomplete scan/delete, raw bool/None slot,
  missing per-key outcome and unknown write all refuse or disclose partial work.
* SQL A03 precedence/truthiness/full vs name-only row tuples, A06 naive
  datetime.now/ignore supplied times/original prior access, numeric int64 bounds,
  no TTL/read-touch, physical-identity dedup, no snapshot and R1–R5 regression.
  Use §6.1's exact SDK-only route for WM-4 tests, including actual supported
  released-server execution; client API proof is not that release gate.

Source/observation basis (SDK package-relative): dataset.py:95–114 keys;
session_shared.py:386–460 insert/update; operations_shared.py:500–589 guards/
generation/result visibility and :611–699 put/add/get/delete;
record_result.py:124–171,193–220 status/doubt/raw slots. The capability inspection
actually exercised guarded add/get, mark/release, durable delete, network-none
whole-container clean stop/restart and an unprivileged loopback SDK client.
These establish a direction, not this reusable adapter's existence.

**Evidenced feasibility disposition:** completed OWN-NEW synthetic controller
preflight exercised fixed hashed unprivileged SDK runtime/loader mounts,
same-data public→isolated→public transition, stable lock/fsynced launch gate,
partial bootstrap, six live-runtime controller deaths and interrupted finish.
Historical 50 case/fixture entries and 21 boundaries are not 50 provider tests.
P1's precreated-STOPPED invariant is adopted above. P3's conservative absent-root
refusal is explicitly supported, not a proven retry capability.
P2's formerly unchecked already-stopped branch was corrected: 21 runner
fixtures (19 expected refusals) and 15 actual NEW-controller cases include two
real controller crashes after stop/before acknowledgment. Stop-intent fsync,
incarnation-bounded timestamped clean logs, terminal process inventory and
reinspect race checks are now the contract for every stop.

WM-1 usable protocol/runtime/continuity, WM-2 explicit authority/partial
bootstrap and WM-3 bounded history are addressed at **model-contract and
feasibility** level within the actual approved envelope. No malicious-root,
arbitrary rollback, power-loss, partition availability or all-fault recoverability
requirement is added. Known unsafe/corrupt history and unsupported runtime/
launcher/permission profiles still refuse; no scope-proof boolean bypass or
permanent always-refusing feature counts as implementation. Real provider
context/CLI/launcher integration, full codecs/policies/fault tests belong to
01-02.3/01-03, not an impossible preimplementation proof of code that does not
yet exist. Final supported released-server validation remains group04.
Fresh independent exact-pair review and actual full-contract approval remain
pending; no independent pass, model-validator pass or task completion is claimed.

### 14.7 Author developer walkthroughs and decision log

**B1 — first use / interrupted bootstrap (paper only).** In demo, operator
intent I creates root boot B phase0, creates qid/sid hi0 and dies before rid.
All manager constructors reject phase0 before startup get_ids. Successor
reacquires same gate, clean-restarts isolated server, validates I/B and qid/sid
zero, creates rid hi0, inventories then guards phase0→1. Two constructors now
validate the same ACTIVE B without writing. Another intent J cannot own B.
Later purge empties q/s but leaves r and metadata; deleting rid out-of-band is
unsupported, and a noticed missing rid still errors, never reinitializes.
No physical API/input is unresolved in this trace; actual fault tests pending.

**B2 — stranded admission / successor (paper only).** q7 is closed with adm1;
old writer W has an admission. Trusted gate stops W/public server cleanly.
Maintenance opens same data in isolated new incarnation, runtime inventories
s41, deletes/settles, sees empty, issues put-zero then controller loses reply.
Journal stays MAINTENANCE. Successor cannot trust remembered zero or merely
reacquire flock: it terminates runtime/server through another verified clean
stop, restarts isolated, revalidates q7/boot/s41 inventory then repeats guarded
put-zero/empty/final-delete. No W admission survives as a runnable command.
An exit137 or stale log stops the trace before reset. Final contract review
must independently challenge this continuity and data-preservation reasoning.

**B3 — public return / intact history (paper only).** After settled per-qid
results, clean-stop maintenance, start original on same bound data under the
still-held launch gate; activation sees ACTIVE and all monotonic high waters.
Fresh writers reserve above prior highs; reports survive q7. Crash before
gate-clear cannot authorize old queue replay: successor repeats settlement.
Normal SQL timestamp/tuple/partial/no-snapshot semantics are unchanged.

| Decision / alternative | Basis / reopen trigger |
|---|---|
| One root in existing meta + immutable allocator boot, not registry or shared API parameter | Distinguish partial bootstrap from active lost history with fixed-size metadata; reopen on new migration/ownership requirements |
| Explicit trusted NEW intent, not empty-scan historical proof | Actual clarification §8; reopen if automatic external-reference/history attestation is newly required |
| Whole owned deployment clean-incarnation boundary, not SDK close/quiesce/sleep | Actual capability boundary and known non-barriers; reopen if real transition/runtime/launcher preflight fails |
| Host flock plus durable trusted launch gate, not per-writer database leases | Actual trusted-admin envelope and operator-crash continuity; broader/malicious multi-host control requires separate design |
| Narrow history envelope, not SC/MRT mandatory or claimed power-loss guarantee | Actual user decision; new production partition/durability/restore requirement reopens model/configuration evidence |

These are author traces/assumptions, not an independent pass or accepted model.
