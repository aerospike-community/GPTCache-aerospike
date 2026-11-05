# Aerospike scalar schema summary — DRAFT_PENDING_FINAL_REVIEW

## Current SC/MRT and vector extension

Derived from the guide's **Current SC/MRT and native-vector implementation**
section. It supersedes conflicting historical AP-only summaries below without
changing their historical review status.

- SC public mutation/read-touch calls use one explicit MRT, including initial
  sessions and allocator reservations; nested calls join. Transaction sessions
  are context-local. Failed operations abort, never commit partial chunks.
- Fresh empty SC scopes self-initialize root/allocators atomically; existing
  missing history is never backfilled. Automatic root `bind` is SHA-256 of UTF-8
  `J(["gptc",1,namespace,cache_scope])`. AP retains its existing operator path.
- SC session append atomically writes its parent and association; `adm=0`.
  Nonzero legacy admissions still require recovery. Query candidates are
  re-read by primary key inside MRT; enumeration is not a snapshot.
- Error phases: `ABORTED_TRANSACTION` for confirmed rollback,
  `UNKNOWN_TRANSACTION` for uncertain commit/abort. Only confirmed aborts whose
  failures are transient MRT conflicts are retried, with jittered backoff, at
  most 32 attempts and the effective SC query timeout as the retry-admission
  budget. Unknown/abandoned commit or abort is never replayed; operation
  acknowledgments are not commit receipts.
- Nonpositive signed-int64 scalar neighbor IDs return misses; mutations require
  positive IDs. Omitted policies supply a fifteen-second query deadline;
  explicit caller deadlines remain unchanged.
- No-touch retention can be provisioned on all five owned sets instead of
  changing a shared namespace default. Disabled set overrides may appear as
  `-1` or `4294967295`; inheriting/absent sets require namespace default zero.
  The backend never changes those settings and still needs all-node visibility.
- SC count/ID queries project only `fmt`, `scope`, `qid`, `state`.

| Set | Key | Bins |
|---|---|---|
| `gptc_meta` | `J(["gptc",1,cache_scope,"vmeta"])` | `fmt=1`, `scope`, integer `dim>0` |
| `gptc_v` | `J(["gptc",1,cache_scope,"v",qid])` | `fmt=1`, `scope`, positive int64 `qid`, integer `dim>0`, FLOAT32 VECTOR `vec` |

Vectors/schema never expire. One bounded embedding per record; dimensions
must match the scope schema. Vector batches are atomic on SC. Exact server-side
scan-and-rank Top-K returns `(squared_l2, id)`, `(1-cosine, id)`, or
`(-dot_product, id)`, closest first. Native Top-K pushdown handles limits up to
1000; larger positive int64 limits stream scoped distance/ID projections and
select exact nearest results locally, with metadata transfer/memory proportional
to the matching set. Neither path is ANN. Inputs must be
finite 1-D float32-representable vectors; cosine excludes zero vectors.
No vector secondary index or separate service. Required native capabilities
must be verified on the actual client/server build.

`VectorBase("aerospike", scalar_storage=scalar, dimension=...)` shares but does
not close the scalar connection. Standalone construction owns its connection.
The existing manager's scalar/vector calls are still separate transactions.

**Not implementation-ready.** All three entity groups have actual conditional
exploratory approvals, not whole-model acceptance. Actual conditional constraint
approval and parent evidenced SDK-only adoption supplement clarification §8.
Guide §6.1 and §14 address WM-1–4 model feasibility with qualified completed
synthetic preflight, not final-provider implementation tests. Historical
whole-model independent verdict was BLOCKED; fresh exact-pair review and final
actual stakeholder acceptance remain. No validator pass or task .2 completion.
This is a derived operational reference, not an independent schema or approval.
If it differs from the guide, the guide wins; regenerate after guide changes.

## Derivation provenance and source mapping

Derived by source-section condensation after approved report commit
`c649d526f6df0bd0f799edd3e52f461cc6205be9` (groups 1/2: `43fe23d`).
Re-condensed from the revised §14 guide after the separately committed actual
envelope clarification; the revised model itself is uncommitted/unaccepted.
No automated generator or executed `derive-summary` command is claimed.
Author condensation/document checks are not independent review or runtime tests.

Authoritative input: `docs/aerospike-schema.md`, SHA-256
`a40f3cff0557b3cd602627c2cb0f0471cca5f408e607c12d7c56aab5a3f9c35b`.
Historical failed §11.5–11.7 is not an implementation algorithm;
§12 overrides the earlier parent/session control candidate; §14 overrides
earlier bootstrap, unconditional-history and unspecified-context language.

| Summary contract | Guide source |
|---|---|
| Approval boundary / unresolved activation gates | §1, §6, §12.7, §13.1/13.5/13.8, §14.1/14.6 |
| Keys, question and metadata bins | §3–4, §11.3, §12.2, §13.3, §14.2 |
| Payload/native codec and timestamps | §4, §13.3 |
| Method-to-model matrix and results | §5, §11.4, §12.4–12.7, §13.4–13.5 |
| Scan identity, deduplication and non-snapshot meaning | §5, §11.4, §12.6 |
| Session fence and recovery | §12.2–12.7 |
| Initialization/history/migration/operator context | §14.1–14.6 supersedes conflicting earlier language |
| Growth, limits, indexing and policy constraints | §6–7, §10, §11.2, §12.8, §13.2/13.6 |
| Exact public query policy / compatibility / retry limits | §6.1 (supersedes earlier pending WM-4 instructions) |

## Namespace, keys and identities

Explicit configured namespace and exact `cache_scope` string define isolation.
Reject blank/whitespace-only scope; otherwise preserve case, whitespace and
Unicode code points. No inferred tenant, authentication or cross-scope operation.
`J(parts)` is Python
`json.dumps(parts, ensure_ascii=True, separators=(",",":"), allow_nan=False)`;
the resulting JSON **string** is the SDK user key, not concatenated components.
Use `DataSet.of(namespace,set).id(J(parts))`.

`MAX=9223372036854775807`. Allocated qid/sid/rid are int-not-bool in `1..MAX`,
non-reused over intact supported history (§14) within namespace/scope/kind. Different kinds/scopes
may share a number. Gaps are legal; sid is association identity, not a name/qid.
Public key inputs accept `numbers.Integral` (including NumPy integer), excluding
bool; validate signed-int64 range, normalize to Python int, then require positive
identity. Preserve SQL A03 truthy filter selection before selected-key validation.
No float/string/arbitrary `__int__` coercion or safe-answer allowlist expansion.
Validate returned namespace/set/digest against the reconstructed key plus all
required bins/types/version/scope; absent/synthetic key metadata is an error.
Generation, when used, is same-record CAS, not an allocator or durable epoch.

## Set `gptc_q` — one complete question payload plus lifecycle controls

Key: `J(["gptc",1,cache_scope,"q",qid])`. Never expires. All bins required.

| Bin | Physical type / constraint | Purpose / mutation |
|---|---|---|
| `fmt` | int-not-bool = 1 | Immutable format |
| `scope` | exact string | Immutable ownership |
| `qid` | positive int64 | Immutable question/vector identity |
| `state` | int-not-bool 0 or -1 | Active → tombstone only |
| `payload` | blob, codec 1 | Immutable complete ordered payload |
| `create_on` | naive wall-time string below | Immutable generated creation |
| `last_access` | naive wall-time string below | Only active read/touch changes it |
| `afmt` | int-not-bool = 1 | Immutable admission protocol |
| `adm` | int-not-bool `0..MAX` | Outstanding-admission accounting, not child count |

## Set `gptc_meta` — initialization root plus three per-scope high waters

Root key `J(["gptc",1,cache_scope,"root"])`. Never expires/deletes/resets.

| Root bin | Physical type / constraint | Purpose / mutation |
|---|---|---|
| `fmt` | int-not-bool = 1 | Root format |
| `scope` | exact string | Immutable ownership |
| `boot` | canonical lowercase UUID string | Immutable initialization identity |
| `bind` | lowercase 64-hex SHA-256 | Immutable non-secret deployment/vector manifest digest |
| `phase` | int-not-bool 0 or 1 | INITIALIZING → ACTIVE only |

Allocator keys: `J(["gptc",1,cache_scope,"qid"])`, same recipe with `"sid"` or `"rid"`.
Never expire/delete/reset/reclaim during eviction, name drop or purge.

| Bin | Physical type / constraint | Purpose / mutation |
|---|---|---|
| `fmt` | int-not-bool = 1 | Immutable allocator format |
| `scope` | exact string | Immutable ownership |
| `boot` | exact root UUID | Immutable initialization ownership |
| `hi` | int-not-bool `0..MAX` | Monotonic highest reserved ID for this kind |

Reserve n only after activation on existing valid metadata: typed scope/fmt/boot/range predicate,
`hi<=MAX-n`, atomic `.add("hi",n).get("hi")`. Raw operation slot 1 is returned
H; require exact integer in range; IDs are `H-n+1..H`. No later read recovers
an unknown reservation. No rewind, max-survivor reconstruction or overflow.
Empty question batch returns `[]` without reservation. sid/rid allocation uses
its own metadata and the same checks; no shared numeric space between kinds.

## Set `gptc_s` — one immutable session association instance

Key: `J(["gptc",1,cache_scope,"s",sid])`. Never expires. All bins required.

| Bin | Physical type / constraint | Purpose |
|---|---|---|
| `fmt` | int-not-bool = 1 | Format |
| `scope` | exact string | Ownership |
| `sid` | positive int64 | Association identity / DTO `.id` |
| `qid` | positive int64 | Owner / DTO `.question_id` |
| `sname` | string, empty allowed | Session name / DTO `.session_id` |
| `squestion` | string, empty allowed | Original question / DTO `.session_question` |

Equal qid/name/question values remain separate instances. No parent join hides
associations after mark. Only exact sid deletion or successful owner purge
removes them; no timestamp/state/collection bin.

## Set `gptc_r` — one immutable independent historical report

Key: `J(["gptc",1,cache_scope,"r",rid])`. Never expires. All bins required.

| Bin | Physical type / constraint | Purpose |
|---|---|---|
| `fmt` | int-not-bool = 1 | Record/codec format |
| `scope` | exact string | History ownership/inventory |
| `rid` | positive int64 | Append identity |
| `payload` | blob, report codec below | All six supplied fields and generated time |

No parent lookup/admission/existence constraint. Historical qid may no longer
exist. Mark, question purge, session drop and session-purge recovery never
mutate/delete reports. All-group initialization/history integrity inventory
still includes reports and all three high waters. No report query/delete API,
TTL, hidden retention cleanup or erasure/compliance guarantee.

## Codec and timestamp contract

Question `payload` is compact UTF-8 JSON bytes, unique object members, exactly
`{"v":1,"question":str,"answers":[[answer_type,node],...],
"deps":[[name,data,dep_type],...],"embedding":null|{"dtype":"<f4","data":base64}}`.
Question/dependency name/data are strings; type tags are signed-int64, not bool.
Preserve ordered answers/dependencies, duplicates and exact tags. Empty deps
reconstruct string question; nonempty deps reconstruct host `Question` and
ordered `QuestionDep`s. Normalize inputs through host `CacheData` semantics;
an already wrapped list-valued Answer remains one answer.

| Native value | Exact tagged node |
|---|---|
| None | `["n"]` |
| bool | `["b",true]` / `["b",false]` |
| signed-int64 int, not bool | `["i","canonical base-10 integer"]` |
| finite Python float | `["f","float.hex() text"]`, preserve negative zero |
| str | `["s",value]` |
| bytes | `["y","standard padded RFC4648 base64"]` |
| list | `["l",[node,...]]` |
| string-keyed dict | `["d",[[key,node],...]]`, unique keys, insertion order |

No executable decoding, arbitrary object/tuple/set/datetime coercion, cycles,
nonfinite answer floats, integer overflow, non-string dict keys or str fallback.
Unknown tag/arity/version, malformed encoding, duplicate/extra codec fields or
resource failure raises. Empty/None values are data, not absence. No truncation.
JSON uses `ensure_ascii=True`, `allow_nan=False`, no Unicode normalization.

Embedding null → None. Otherwise contiguous ordered 1-D float32 little-endian
IEEE754 bytes in base64; decoded byte length multiple of four. Empty-present
returns empty float32 array, not None. Preserve values/order, not arbitrary
shape/strides/source dtype; embedding bits need not satisfy answer float rules.

Time grammar: exactly `YYYY-MM-DDTHH:MM:SS.ffffff` (26 ASCII characters, valid
calendar/time, no zone/Z/offset), decoded to naive datetime. Generate with
client-local `datetime.now().isoformat(timespec="microseconds")`; creation and
access sampled separately, **ignore supplied DTO times**. Touch samples before
send; no monotonic/cross-process/UTC-instant guarantee. Creation never changes.

Report payload exact unique members:
`{"v":1,"user_question":str,"cache_question":str,"cache_question_id":int,
"cache_answer":node,"similarity":node,"cache_delta_time":node,"cache_time":str}`.
Question strings exact, empty allowed; qid positive int64, no existence check.
Answer is the supplied safe native node (adapter passes plain `.answer` string
or `""`), **not** an Answer DTO or inferred type tag. Similarity/elapsed are
finite Python float or signed-int64 int, not bool/coerced numpy/Decimal/string;
elapsed is numeric **seconds**, not timedelta. No [0,1]/nonnegative restriction.
Generate report `cache_time` once using the same naive wall-time grammar.
Validate/encode entire DTO/event before avoidable reservation/writes.

## Every-operation visibility, atomicity and failure rules

Keyed builders explicitly use `.include_missing_keys().fail_on_filtered_out()`
and `.execute(on_error=ErrorStrategy.IN_STREAM)`. Drain every result/stream;
correlate original key/index to input, not arrival order. Check `is_ok`, embedded
exception, result code, `in_doubt`, required row/slot presence and identity.
Missing/duplicate/unexpected rows, embedded failures even with OK code, malformed
raw slots or interrupted iteration are errors/uncertainty, never success.
Redact payload/text/blob/raw repr from correlated error context.

Every write segment explicitly uses `.never_expire()` and
`.with_txn(None)` (no ambient implicit MRT). Chaining a new insert finalizes/
resets segment operations/predicate/generation/TTL: repeat policy and guards per
segment, use distinct bin mappings. No cross-record atomicity or automatic
vector rollback. Effective POINT/WRITE_NON_RETRYABLE retries must be **zero**
for allocation, create, read/touch, admission and release through overrides,
reload, transport and application wrappers. No hidden replay/new-ID retry.

Explicit proven not-found without doubt/conflict: get None; mark/exact delete
idempotent no-op; missing reopen allocator is recovery error. FILTERED_OUT
alone is never a miss/success: perform unfiltered exact-key diagnostic validating
full bins/controls/payload/times/physical identity. Valid tombstone/known absent
can explain get None or mark no-op; still-active rejection is error/no touch
replay. Corrupt/unknown format/scope is error. Diagnostics are later observations,
never recovery of the original acknowledgment. Unknown reservation sends no
child/payload/event; unknown non-idempotent write is not automatically retried.
Failed batch reports each input→ID→known/failed/unknown phase; known successful
payloads/children/events may remain, never return a full-success ID list.

## Thirteen-method-to-model implementation matrix

All rows inherit the keys, policies, codecs, scope/identity and error rules above.

| Method | Records / atomic work | Required result / boundary |
|---|---|---|
| `create` | Validation-only ACTIVE root plus all qid/sid/rid metadata, exact identity/types/matching boot | Explicit provider-only operator init/resume/finish BEFORE manager construction/get_ids; INITIALIZING/missing/corrupt root or any allocator fails, no automatic zero-fill |
| `batch_insert` | Prevalidate; reserve ordered qids; create-only full q bins incl. afmt=1/adm=0; for truthy input session name only, after known payload success reserve sid and run fence below | Ordered IDs correspond to inputs only on full acknowledged success; initial squestion is string question/Question.content; partial/unknown association failure discloses persisted payload |
| `get_data_by_id` | Active/valid-control existing-only q operate: ordered gets fmt,scope,qid,state,payload,create_on,last_access then put new last_access; then scoped qid session-name projection | Original raw slot 6 is previous access, slot 7 is put; validate full q/controls without changing slots. Duplicate-preserving tuple-compatible `(name,)` rows with `[0]`, unpacking and `.session_id`; projection/decode error raises despite persisted touch; absence/tombstone None |
| `mark_deleted` | Deduplicate numeric qids, existing-only typed q identity/valid-state/control guard, put state=-1 | Preserve adm/payload/times; no upsert; repeat safe, per-key errors/doubt visible |
| `clear_deleted_data` | First-observed scoped tombstones; exact parent validation; closed/adm=0 fence; full owned-sid delete/empty verification; guarded existing-only closed/zero q delete | adm>0 → recovery-required, keep tombstone; unknown/incomplete cleanup fails with correlated partial work; active never deleted; reports/meta/vectors untouched |
| `get_ids(deleted=True)` | Full scoped q scan projecting fmt,scope,qid,state,afmt,adm | True selects -1, False selects 0; distinct first-observed identities, no prefix/sort/snapshot |
| `count(state=0,is_all=False)` | Same complete validating q scan, not deliveries/materialized count | State selection, or both valid states for is_all; no s/r/meta included |
| `flush` | No buffer / no persistence operation required | No-op only for synchronous acknowledged model; never settles/replays uncertain commands |
| `add_session` | Reserve sid, guarded parent admission, one full create-only s, one settled owner release | Explicit empty name allowed; missing/deleted parent rejects; duplicates get new sid; success requires known child AND known release |
| `list_sessions(session_id,key)` | Full validating scoped s enumeration with SQL filters below | Four-field tuple-compatible `(id,question_id,session_id,session_question)` plus named attrs, duplicate instances, no parent join |
| `delete_session(keys)` | Exact scoped **sid** reads then existing-only guarded exact s deletes, validated immutable identity/bin values and generation where used | Empty no-op; known missing safe; filtered/CAS errors diagnose/fail; no parent-existence guard or q/meta/r/vector mutation |
| `report_cache` | Validate/encode all six args; reserve rid; one create-only full immutable r insert | Original correlated acknowledged complete result returns None as SQL, not rid/bool; doubt/partial failure raises; new explicit call may duplicate content |
| `close` | Release lifecycle-owned SDK resources only | Repeat safe; operations after close fail; no delete, buffer flush fiction or uncertainty healing |

SQL A03 list filters: truthy name → exact scope/name, **ignore key**; falsy name
and truthy key → scope/qid; both falsy (including empty name/key=0) → all scoped
associations. Truthiness never authorizes invalid qid coercion. Scalar always
returns full rows; manager name-only branch extracts sid `.id`, key branch full
rows, neither branch distinct names. Name drop lists then deletes captured sids;
later new associations may survive. The read DTO's `(name,)` projection is
distinct from these four-field list rows.

## Scan semantics

No application secondary/expression/set index is required/created. Use public
dataset `.where(typed_expr)` for exact scope with allow_scans_with_where=True
(not string AEL/SI selection). Question projections include controls; validate every delivered
row including repeats **before** deduplication/state selection. Deduplicate
physical namespace/set/digest identity; first validated q state wins per call.
Later valid state changes do not replace it; corrupt repeats still fail.
Session repeats must agree on immutable fields; equal values at different sids
are not duplicates to discard. Process streams to completion, never truncate.

Enumeration/count/listing are not cross-record/end-of-call snapshots; separate
calls may disagree during mutation. Quiescent successful enumeration must
cover all matching distinct records. Cleanup empty verification is meaningful
only **after** the admission fence and settled deletes, not as the fence itself.
Projection reduces network bytes, not full-record device I/O.

## Session fence and exceptional recovery

`V` = typed valid fmt/scope/qid/afmt/state/adm plus exact physical identity.
Reserve sid before entry. Existing-only `where(V & state==0 & adm<MAX)`
`.add("adm",1).get("adm")`; exact raw slot 1 `1..MAX` and original known OK
authorizes **one** child create. Unknown entry → no child/no decrement/no replay.
Mark serializes on same q, closes new entry, preserves adm.

After child is authoritatively terminal success or non-applied rejection, first
irrevocably disable all future child sends, then owner releases once via
existing-only `where(V & adm>0).add("adm",-1).get("adm")`; slot 1 `0..MAX-1`.
No generic finally, inferred permit, restart resume, timeout reset, compensating
delete-then-release or replay of an unknown decrement. Unknown child retains
accounting. Known child + unknown release is append failure with known
persistence disclosed. A pre-mark admitted live owner can still create once.
Missing known-admitted parent is an invariant breach.

Ordinary purge: validate tombstone, require adm=0; fully enumerate/validate
scope/qid children, delete exact sids and settle all outcomes; complete empty
verification; existing-only `where(V & state==-1 & adm==0).delete_record()`.
Concurrent known absence is idempotent; nonzero accounting retains tombstone
and surfaces recovery-required. No reset/age inference during ordinary purge.

Backend-only proposed, not implemented (§14 supplies context construction):
`AerospikeStorage.recover_session_purge(qids, *, recovery_context)`.
No common API/worker or undeclared privilege. Only explicit already-tombstoned targets.
Before **any recovery mutation**, independently reviewed executable procedure
must establish complete writer/version/queue/restart/direct-writer inventory,
enforceable exclusion of old and replacement writers (including purgers/name
drops/recovery operators), settlement or authoritative prevention of every old
modifying command, and exclusion continuity across recovery failure.
Timeout/client-kill/socket-close/sleep/empty scan/boolean context is not proof.
Clean whole-server-incarnation termination inside the approved isolated
deployment, together with the trusted cross-operator launch gate below, is
the proposed concrete settlement/exclusion path, not a process-kill shortcut.

With that proof retained: validate parents/inventory all owned sids, delete/
settle/verify empty; guarded existing-only tombstone put adm=0/get adm (raw slot
1 exact zero); reverify empty; guarded final delete; settle recovery commands
before admitting only fresh compliant writers. On interruption maintain
exclusion, reverify settlement/inventory; no remembered decrement. Without
proof refuse mutation and retain tombstones. Qualified synthetic preflight exists;
unsupported prerequisites/new capabilities require REVISION_CONFLICT, not a
permanent always-refusing “completed recovery” or successful purge claim.

## Provider-only operator protocol — derived from guide §14

New proposed module `gptcache.manager.scalar_data.aerospike_operator`:
`DockerMaintenanceOperator.from_manifest(manifest_path, *, journal_dir)`;
`initialize_scope(*, namespace, cache_scope, intent_id, vector_binding)`;
`resume_initialization(*, intent_id)`;
`recover_session_purge(*, namespace, cache_scope, qids)`;
`finish_maintenance()` / `status()`.
CLI `python -m gptcache.manager.scalar_data.aerospike_operator` has fixed
`status/init-new/resume-init/recover-purge/finish` subcommands, structured
manifest/journal/intent/namespace/scope/vector-binding/qids-file options;
init-new requires `--declare-new-logical-cache`. No arbitrary commands/hooks.
Source handoff: `gptcache/manager/scalar_data/aerospike_operator.py` public
API/CLI/controller; `_aerospike_operator_runtime.py` private fixed runtime/context;
`aerospike_storage.py` provider; unit `tests/unit_tests/manager/
test_aerospike_operator.py` plus provider suite. Optional tooling not loaded or
provisioned by ordinary CRUD; implementation after final gates only.

Initialization: exclusive deployment lock plus persistent MAINTENANCE gate;
durable explicit NEW logical-cache intent (new/consistent vector binding),
boot and bind BEFORE root create-only phase0. All-kind conflict inventory
required but empty scans are not historical authority. Resume only same intent
and exact owned phase0 root: create-only absent qid/sid/rid allocators hi=0;
validate existing ones exact boot/zero, no q/s/r allowed. Unknown commands
settle before reinspection. After full three-allocator/inventory validation,
existing-only guarded root phase0→1. ACTIVE never permits zero-fill; missing
any allocator always errors. Known acknowledged root loss also errors.
Absent root after any prior create attempt conservatively refuses, even if
unacknowledged; no absent-root retry capability or recovery SLA is promised.
Concurrent init serializes under same
deployment lock; another intent refuses, same-intent winner is validated.
Normal create is validation-only and get_ids cannot bypass activation.

Initial recovery adapter is single-host/single-node file-backed Docker.
Host flock on stable inode + fsynced persistent journal binds daemon/host,
container/image IDs, same persistent data mount, namespace/config/runtime
hashes, scope/qids/boot, numeric uid/gid and ALL trusted launch/admin/queue paths.
Journal MAINTENANCE survives controller death; lock release is not gate release.
Trust-compliant launchers/admins cannot exec/run/restart alternate writers.
No malicious-root protection, shared control plane or uninventoried cluster.

Public-service transition: gate/disable inventoried launch paths; restart=no;
terminate old writers/queues, clean-stop original whole container with fresh
incarnation log/exit0/PID0/no-OOM/forced-kill checks. Fsync bound stop intent
before stop. Live AND already-stopped successors require matching intent,
StartedAt/FinishedAt (finish>start), terminal old processes, timestamped clean
marker in exact `logs --since StartedAt --until FinishedAt` interval, then
reinspect to exclude races. Missing intent/unclean/stale/ambiguous evidence refuses.
Keep original stopped. Prepare maintenance (`--pull=never --network none --restart no`,
no ports) on SAME authorized file-backed cache data, exact image/storage config,
reviewed network-only loopback delta. Precreation STOPPED permitted; EVERY
maintenance START follows verified clean stop of prior owner. No concurrent
server opens the data.
Verify isolation, data identity, current server incarnation and readiness.
Run fixed hashed runtime via allowlisted unprivileged Docker exec/interpreter,
SDK sync `ClusterDefinition("127.0.0.1",3000)` **inside** maintenance container,
never old public host endpoint. Exact SDK-only query/config contract is guide §6.1.

Controller holds lock/gate and constructs internal bound
`DockerMaintenanceContext`, not user bool/serialized verification; sole fixed
runtime builds provider and invokes retained backend method. Bind current
incarnation, identity, nonce, targets and runtime PID, verify around dispatch.
Perform validated inventory/delete/empty/reset(raw slot1=0)/empty/final-delete.
Unknown output/boundary/controller failure retains gate. Successor reacquires
same lock and clean-stops/restarts current maintenance server (including old
recovery runtime) AGAIN before any inventory/reset; invalidates old contexts.
Never replay remembered decrements. Forced kill/stale clean log refuses.

Finish only after settled commands; clean-stop maintenance, restore/restart
original server on same data, verify metadata/config before clearing journal
gate and launching FRESH compliant writers. Interrupted finish reenters fresh
settlement, never resumes old queues/admissions or hides remaining tombstones.

Docker allowlist: inspect/image inspect/top/logs; reviewed restart update;
stop/start bound IDs; fixed maintenance create; fixed runtime exec.
No shell strings, socket in provider/container, pull, privilege escalation,
host networking/PID, raw disk, prune/rm/truncate. Launcher actions require
reviewed adapters. Redact credentials/TLS secrets/payload/raw inspect env/
raw result repr; persist only allowlisted identity/hash/status/phase fields.
Refuse wrong identity/data/config/runtime/uid/targets, unknown launchers,
lost gate, corrupt history/journal, unclean stop, incomplete stream or unknown
outcomes. No permanent-refusal stub counts as completed usable recovery.

Qualified synthetic preflight supports fixed runtime/mount/uid feasibility,
NEW public→isolated→public SAME-data transition, gate/lock, partial bootstrap,
six live-runtime controller crashes and interrupted finish. Historical 50 entries/
21 boundaries remain distinct from corrected P2's 21 runner fixtures (19 expected
refusals) and 15 actual new-controller cases, including two post-stop/pre-ack
crashes. Not final-provider helper tests. Real CLI/context/launcher integration,
full codecs/policies/fault cases belong to 01-02.3/01-03; supported released-server
proof remains group04. Unsupported deployment/runtime/permissions still refuse.

## Initialization, policy, growth and unresolved gates

- NEW authority is explicit trusted operator intent, not proof against all
  historical external references. Intact root/qid/sid/rid/q/s/r and consistent
  vector/reference history must be preserved/restored together. Known missing/
  corrupt history fails closed; no maxima rebuild. Power loss, malicious admins,
  out-of-band deletion, inconsistent restores and available AP-partition writes
  have no guarantee. This is actual clarification §8, not silent relaxation.
- No silent control default/backfill/mixed incompatible writers. Unknown
  format fails; explicit versioned migration requires quiescing/excluding old
  writers, command settlement, full all-group/external-ID inventory, validated
  payloads and preserved non-reused IDs. No competing allocators in one space.
- Before write activation, public SDK `session.info().info_on_all_nodes`
  checks `get-config:context=namespace;id=<namespace>` and `sets/<namespace>`.
  Validate namespace command component, complete applicable-node membership and
  matching response node sets, success/nonempty/well-formed exact settings.
  Require namespace `default-read-touch-ttl-pct=0`, relevant existing q/s/r/meta
  sets override 0 or -1; absent sets inherit 0. Missing permission/node/field,
  malformed/ambiguous/mixed/unsafe values refuse BEFORE writes. No constructor
  namespace mutation or silent proof fallback. Revalidate create/activation,
  init/resume/recovery after clean incarnation, finish/public reopen and explicit
  config/topology/effective-behavior changes before resumption. Operator maintains
  pinned supported config between boundaries; no automatic drift detector/lease.
- Every modifying segment `.never_expire()` (-1); point/batch reads no-touch -1.
  SDK-only queries `.where(combined_typed_expr).bins(list).with_txn(None)`,
  `ErrorStrategy.IN_STREAM`, full drain; child effective caller Behavior:
  reads no-touch -1, reads_query allow_scans_with_where=True/max_retries=0,
  writes max_retries=0. Verify all actual READ/WRITE_NON_RETRYABLE point/batch/
  query AP/SC cells, preserve unrelated caller fields including total/socket,
  retry delay, consistency/replica, compression/queue/concurrency/verbosity and
  release timeout_delay. Query wire0 defers to verified disabled configuration,
  not a claim Settings maps query -1. Require finite positive bounded total
  deadline, valid unsigned32-ms/rounding and no silently lengthened caller bound.
  No `.with_policy`, `.filter_expression`, PAC/native policy type, private mapper,
  underlying CRUD, legacy fallback or SDK patch. Lazy selected-SDK/dependency
  provenance validation; incompatible/missing surfaces/settings refuse.
- Query `max_retries=0` is **not zero partition retries**. Bounded deadlines,
  per-delivery physical validation before nonsnapshot ID/digest deduplication,
  visible row errors and terminal iteration exceptions are required. Never
  accept a partial prefix as complete. Both compiled clients exercised all4096/
  ids1,2,3/ninebins/digest/bytes/terminal completion, wire query0/point-1/writeTTL-1;
  finite query/default0 controls did not extend, +50 did, nine owned markers
  survived >14.27s vs test TTL12 with TTLNone. Released void-time mapping is -1;
  dev measurements avoid old read-only-operate WritePolicy0. Provider uses
  ordinary SDK read no-touch metadata, no native measurement hack.
  Released client against DEVELOPMENT server below published SDK 8.2.0+ minimum
  is API capability ONLY; actual supported released-server group04 and final
  provider all-entity retention/error validation remain required.
- Verify actual supported released `aerospike-sdk` / `aerospike-server`
  compatibility, namespace mode, effective retry/read/replica-commit policies
  including overrides, permissions, configured max record size, no-expiry,
  complete quiescent enumeration, durable histories and deletion/restart/restore
  semantics within §14's approved envelope. No SC/MRT requirement is selected.
  Development durable-delete/clean-incarnation samples are observed, not
  released/production qualifications. AP defaults/COMMIT_ALL/create-only do
  not certify partition safety or history after excluded failures.
- **WM-1–3:** concrete protocol and qualified synthetic preflight address model
  feasibility within approved trusted/intact-history/consistent-restore envelope.
  P1 STOPPED precreate accepted; P2 fixed/retested; P3 absent-root safe refusal,
  not retry proof. **WM-4:** adopted compiled/wire/TTL-proven SDK-only constraint.
  No author independent-pass claim. Real provider integration/fault tests and
  released-server evidence remain future gates, not impossible preimplementation
  proof. Fresh independent exact-pair review and full actual user approval OPEN.
- Record-size distributions, volume/rates/skew, scope count, retention horizon,
  RF, conflict/latency/cleanup budgets and allocator contention are UNKNOWN.
  No business cap, accepted production sizing or performance claim. Reject
  oversize/resource/capacity errors visibly without truncation/chunking/field loss.
- Metadata count is four per scope (`4*64*RF*scope_count` PI); no growing registry.
  Fixed-bin q payload collections are immutable but can be arbitrarily large at
  input; reject overflow. s/r records do not accumulate members; total retained
  s/history volume is unbounded without explicit deletion/capacity. No hidden
  report pruning. Metadata is fixed-size; MAX exhaustion fails before add.
- Measure logical uncompressed encoded bytes (JSON/base64/escaping/keys/bins),
  p50/p99 and full-record rewrite rates; >~50 KiB requires update-rate review.
  Touch/mark/entry/release rewrite whole q; each append adds two parent rewrites
  plus s and metadata writes. Hot per-scope qid/sid/rid allocators and hot
  parents, scan cost, PI `64*RF*record_count`, report growth and recurring
  oversize/capacity failures trigger renewed model/sizing/index review.
  No automatic splitting, sharding, extra index, compression or range prefetch.

**Exit remains blocked:** fresh independent whole-contract review of guide
**and this derived summary** against the completed qualified evidence,
triage of every anchored finding, then final actual stakeholder acceptance.
No task 01-02.2 completion, plan SUMMARY, task .3/provider/test implementation
or evidence-gate pass is asserted by this document.
