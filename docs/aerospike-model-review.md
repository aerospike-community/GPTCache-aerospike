# Aerospike model review — incremental gate record

## FINAL decision — approved implementation contract

Actual stakeholder selected **“Approve and implement”**, recorded
**2026-11-06T04:43:56Z UTC** (recording clock, not an invented message-event
timestamp). This is final FULL revised-contract acceptance within clarification
§8's approved envelope, not another conditional exploratory group decision.

| Approved, independently reviewed artifact | SHA-256 |
|---|---|
| `aerospike-schema.md` | `b11fa455ebaae64b2dd166c2f60d498447d59ed7451f3eb7ab08c03ec1780334` |
| `aerospike-schema-summary.md` | `f823ae15921f5f9c0dc9bd2675ff4924a0aac98ef9a4e3cfba94817b758799e6` |

The original independent whole-model reviewer (parent dispatch **6abcd24c**)
rechecked this exact pair at **2026-11-06T01:23:30Z** before final acceptance.
Actual reviewer identity: “the original independent whole-model reviewer above,
not a model, operator-preflight or query-preflight author.” Actual verdict:
**“IMPLEMENTATION_CONTRACT_READY — for final stakeholder consideration within
the explicitly approved supported envelope.”** Actual finding: **“No remaining
blocking model/derived-summary implementability defect was found.”**
WM-1–4 close at **model-contract/feasibility** only; all thirteen methods/ABI
were rechecked. WM-5 remains ordinary implementation obligations, not a blocker.

**Status precedence:** this actual final decision explicitly supersedes the
guide/summary's historical DRAFT_PENDING_FINAL_REVIEW/not-implementation-ready
authoring metadata and earlier blocked/pending statuses below. The exact
reviewed guide/summary bytes are intentionally retained; no normative model
semantics changed after review. Current normative precedence remains guide
§6.1, §12 and §14, not failed §11 or the superseded native-policy bridge.
The summary was **manually source-condensed**, not generator output; no executed
`derive-summary` is claimed. A provenance verification command checks document
consistency, not human judgment or new provider tests.

Historical approvals remain exactly as observed: group 1 “Approve group 1”
(18:33:44Z recording); reopened controls/group 2 “Approve conditional model”
(23:33:39Z recording); group 3 “Approve report model” (23:45:08Z recording),
all on 2026-11-05. Final overall acceptance closes their preimplementation
model-design conditions; it does not backdate unconditional group approvals.
Where exact group start times were not recorded, ordered recorded draft
observations are used, not invented wall-clock starts.

**Verification debt — OPEN; no later gates waived:**
- Implement all thirteen real persistent methods, safe codec, result/identity/
  doubt correlation, SQL A03/A06, no-TTL policies and fixed-size admission fence;
  run full provider and new operator unit suites, including prior tracer tests.
- Implement the actual provider-only operator/CLI/private runtime/context,
  fsynced gate/lock/stop-intent/clean-incarnation/successor/finish protocol,
  root/boot/phase bootstrap/resume and conservative unsafe-history refusal.
  Synthetic preflight is feasibility, not final integration or provider proof.
- Verify public SDK namespace0/set0-or-1 all-node activation before writes,
  caller Behavior/deadlines and revalidation on actual final provider code.
- Execute final-provider live process/concurrency/crash/TTL/retention/failure
  and manual-inspection evidence in its ordered integration/runbook gates.
- Execute supported released-client/server compatibility and quickstart
  (published minimum server 8.2.0+), actual Python floor tests and final checks.
  Prior release-client/development-server probes do not satisfy that gate.
- Workload, capacity, retention budgets and benchmark/production sizing remain
  unmeasured; no benchmark result or production guarantee is inferred.

Only task01-02.2 acceptance is addressed here, subject to actual local artifact
integrity validation. Task01-02.3 and later gates remain separate; no full-plan
completion or implementation result is asserted by this decision.

## Current final-revision checkpoint — SDK-only adoption and bounded feasibility

Actual user conditional “Allow verified constraint” / “Allow isolated runtimes”
recorded 2026-11-06T01:05:26.763355+00:00; clarification-only commit `eb8f223`.
Parent now adopts the compiled/wire/TTL-proven SDK-only configuration constraint.
The earlier public native-policy bridge is SUPERSEDED: released SDK lacks its
attachment. Historical decisions/old hashes below remain audit, never current
instructions or acceptance of this revised pair.

| Finding | Current architect/parent disposition (not independent closure) |
|---|---|
| WM-1 / RA-1 | Model-contract feasibility addressed by guide §14 exact operator API/CLI/source handoff, persistent lock/gate, bound runtime/context, SAME-data public→isolated→public boundary and successor/finish protocol. Completed synthetic controller proves fixed loader/mount/uid, six live-runtime controller deaths and interrupted finish, not final-provider helper tests. P1 accepts STOPPED precreate only when EVERY maintenance START follows verified clean prior-owner stop. P2 fixed/retested: fsynced stop intent before stop, StartedAt/FinishedAt/PID0/exact exit0/noOOM, bounded timestamped clean log, terminal old inventory and reinspect race check, including already-stopped successors. |
| WM-2 | §14 root phase0→1, matching immutable allocator boot, durable explicit NEW intent/vector binding, partial resume and activation-only ordinary create specified. Synthetic root/1/2/3 allocator and lost-activation-result successors exercised. P3 is safe conservative absent-root-after-attempt REFUSAL, not a proven retry capability; no all-fault recovery/SLA required. |
| WM-3 / RA-2 | Actual trusted-administrator/intact-history/consistent scalar-vector restore envelope explicitly adopted. Known unsafe/corrupt history refuses. No impossible malicious-root/arbitrary rollback/power-loss/partition-availability pre-code gate. Effective settings, complete inventory, clean restart and retention observations support bounded feasibility; final integration and released-server tests remain. |
| WM-4 / R5/A09 | Guide §6.1 now SDK-only typed where/bins/txn opt-out plus public Behavior/Settings; namespace0/set0-or-1 verified public info all-node fail-before-write contract, pinned operator configuration/no automatic drift claim. Both compiled clients and literal wire/full4096/bytes/digest/nine-marker TTL controls support capability. Release client on development server is NOT supported released-server compatibility; published minimum8.2.0+, actual group04 still required. |
| WM-5 / R1–R5 | Ordinary implementation warnings retained: visibility and full drain/identity correlation; original raw access slot6, no write replay; scoped validating first-observation nonsnapshot dedup; no synthetic key/payload fallback. SQL A03 truthiness/name precedence/tuple rows and A06 naive datetime.now/ignored supplied times unchanged. Public keys normalize numbers.Integral excluding bool within int64 (including NumPy host IDs), without broadening safe answers. |

Operator historical 50 case/fixture entries /21 boundaries are separate from
P2's 21 runner fixtures/19 expected refusals and 15 actual NEW-controller cases,
including two post-stop/pre-ack controller deaths. No inflated “provider test”
count. No implementation or runtime work was done by this document revision.

**Remaining gate:** FRESH independent exact guide/derived-summary pair review,
triage residual findings, then actual user FULL revised-contract approval.
Final-provider/operator integration tests belong to 01-02.3/01-03 and supported
released-server tests to 01-04; no source implementation, model-validator pass,
independent-ready verdict or task .2 completion is claimed. Progress unchanged:
01-02 1/3 tasks, 1/6 plans. Current pair/source/evidence hashes are in local
`01-FINAL-REVISION-INVENTORY.md`; final-evidence JSON is deliberately not emitted.

## Historical contract-design checkpoint — superseded audit, not live policy

**Recorded 2026-11-06T00:20:02Z UTC**, author recording clock, not a supplied
message-event timestamp. Actual user selected **“Approve minimal envelope”**
and **“Allow scoped tooling”** as recorded in clarification §8, committed
separately at `34c915a` (requirements clarification only). This supersedes
unconditional all-time/history interpretations by explicit user decision,
not by an author's silent relaxation. It does **not** approve revised §14.

Actual fresh independent whole-model review is **BLOCKED BEFORE IMPLEMENTATION /
final implementation-contract acceptance**, inspection clock
2026-11-05T23:52:14Z. The reviewer identifies itself as a separately dispatched
OpenAI coding subagent without an exposed separate agent ID; none is invented
here. The local whole-model review retains its original hashes/anchors/verdict.
The new author revisions have **not** received independent acceptance.

Completed capability MD/JSON and boundary controller/client/config/results
were reviewed for this design. They demonstrate guarded operations and a
network-none/no-ports/restart-disabled clean whole-container incarnation
boundary for a paused admitted writer. The ordinary public service is not
hermetic. No reusable provider-context/host-lock/launch-gate/same-data public
transition or crash-successor helper was tested by that experiment.

| Actual finding | Architect disposition / precise remaining gate |
|---|---|
| WM-1 HIGH / RA-1: usable settlement/context/continuity adapter missing | **ADDRESS IN CONTRACT DRAFT, OPEN.** Guide §14.4–14.6 now specifies trusted single-host Docker host lock + durable launch gate, public original clean-stop, new isolated maintenance container on SAME data, fixed unprivileged SDK runtime and internal bound context, successor new clean incarnation and finish. §12.7 purge ordering preserved. Actual reusable preflight/runtime packaging/launcher compliance and independent review pending; no boolean bypass/always-refusing completion |
| WM-2 HIGH: bootstrap/history authority and partial allocator resume missing | **ADDRESS IN CONTRACT DRAFT, OPEN.** §14.2–14.3 adds fixed-size root phase0→1 and immutable boot on three allocators, explicit provider-only NEW intent/init/resume, validation-only create and startup activation gate; ACTIVE missing metadata fails. Authority is trusted explicit NEW logical cache + new/consistent vector store, not an empty-scan historical proof. Root/partial/lost-reply/concurrent-init cases require independent review and tests |
| WM-3 HIGH / RA-2/A09: history guarantee exceeds actual evidence | **ACTUAL REQUIREMENT CLARIFICATION ADOPTED; EVIDENCE/FINAL ACCEPTANCE OPEN.** Clarification §8 and §14.1 limit guarantee to trusted intact history/consistent scalar-vector restore. Local AP RF1/commit-to-device=false is not power-loss/partition/restore proof; impossible all-time guarantees outside the approved envelope are no longer CRUD prerequisites. Effective policy, enumeration/retention/clean-restart and released compatibility gates remain |
| WM-4 HIGH / R5/A09: exact query policy / retention | **DEVELOPMENT CONSTRUCTION EVIDENCE CLOSED ONLY, 2026-11-06.** Completed agent `4395eb29` resolution MD/JSON, public policy candidate, focused results, wire/provenance reviewed. Parent technical disposition selects public native `QueryPolicy` + SDK public `.with_policy`, not private mapper/SDK patch/native CRUD. Guide §6.1 binds SDK-declared transport, effective settings/internal base copy-back, explicit txn opt-out, finite deadlines, nonsnapshot validating dedup and visible terminal errors. Actual released-pair execution (group D), final provider/live proof and final acceptance remain OPEN |

R1–R5 remain binding: per-key visibility/diagnostics, original raw acknowledged
previous-access return and zero retries, physical-identity validating dedup/
first-observed states, no synthetic key fallback, no query-policy overclaim.
SQL A03 truthy-name precedence and both tuple row shapes, A06 naive local
datetime.now/ignored supplied insert times, int64 IDs, no implicit TTL,
explicit partial failures, no cross-record snapshot and retained independent
reports are unchanged. Historical failed §11 is not normative; §12 + new
pending §14 are the current control/operator draft.

Fresh review must bind the **new exact guide + regenerated summary hashes**,
not reuse prior group/whole review hashes as a pass. Current pair hashes are
recorded in the local continuation handoff after document checks. Earlier
hashes below are historical and do not bind this revision.
Author §14.7 B1–B3 are paper traces/decision log, not executed tests.

Current draft binding, refreshed 2026-11-06 UTC after narrow WM-4 revision:

| Artifact | SHA-256 (document identity, not approval) |
|---|---|
| Guide | `4f7dfcedfdf572de65df2031d18439641fc34e2fa708ff26e65c605a2028bf75` |
| Derived summary | `64a5de956122c0e38f9104b693165d3b844ff067a41457b8abd5d50f41cf67bb` |
| Actual clarification | `5201080ecf1683049cd8772b3d3eea3eda2e1cff1f5904ba7a845dd7fd813fe9` |
| Routing/status | `7a54e3f587d87f9a3e7e874bc594c652c6158cb4491621db9c3bdebe9e67d119` |

Author document checks: eight JSON examples parse, example physical bins ≤15
bytes and allocator examples carry boot; thirteen matrix methods match the
read-only helper inventory exactly once; summary binds current guide hash,
no development-stack identifiers leaked into shipped docs; whitespace and
local JSON checks pass. A first overly broad markdown-bin check also matched
method names and failed; corrected to inspect physical example bins, not
reported as a model defect or a test-suite failure. These checks prove neither
the operator protocol nor independent/model-evidence acceptance.

**STOP:** parent coordinates OWN NEW disposable operator preflight (separate
owner; no partial operator results read by this WM-4 revision), triages actual
feasibility findings and remaining release/provider policy gates, then dispatches fresh
independent whole-contract review and requests final actual user approval.
All three earlier conditional exploratory group approvals remain historical
inputs, not approval of §14. No model-evidence approval/pass, task .2 completion,
task .3, full plan SUMMARY or implementation-ready verdict is produced.
Plan 01-02 remains **1/3 tasks**, completed plans **1/6**. Only actual
clarification was committed; new contract/summary/ledger remain pending.

### WM-4 completed evidence and parent technical disposition — 2026-11-06

Actual `01-QUERY-POLICY-DECISION.md` is the **parent's technical disposition**,
not actual user approval of the whole model. It accepts the public native
policy extension despite the inspected README/docstring inconsistency, while
keeping all data operations on SDK builders. `QueryPolicy` is a public native
transport type, not an SDK re-export. No private mapper/import/helper or SDK
patch is authorized. Guide §6.1 now replaces earlier pending/no-route guidance;
§14 changes here are query cross-references only, not operator-contract approval.

Completed development compiled-request evidence establishes read-touch -1
(`ffffffff`) through native internal-base get/copy/modify/set; the inherited
setter bug and discarded-copy failure are confirmed. Caller-derived public
Behavior READ/QUERY settings are preserved as in the local candidate's lines
10–39, with actual namespace mode, finite deadline validation and explicit
builder/policy transaction opt-out. `max_retries=0` did not prevent three
partition requests before a terminal timeout: no zero-query-retry claim.

The corrected focused q-shaped scans validate actual bins/bytes/physical
identity, all 4096 partitions and nonsnapshot completion. Never-expire void-time
remains -1; synthetic foreign finite absolute void-time stays unchanged after
threshold-crossing reads/scans, while the default-touch control increases.
Earlier SDK `Exp.void_time()` operation-bearing reads used WritePolicy mapping
with wire 0 and contaminated TTL observations: excluded as no-touch proof.
Corrected explicit public native WritePolicy -1 metadata instrumentation was
wire-verified; it is instrumentation only, not a provider read-path proposal.
Exact development identities, hashes and wire-line bindings remain in local
phase resolution/provenance and the WM-4 handoff, not shipped install guidance.

**Closure scope:** development construction evidence only. Group D must still
execute the actual released pair; final provider/all-entity/live retention and
failure tests, separate operator feasibility proof, final independent review of
the exact revised guide/summary pair and actual final user acceptance remain.
No provider/helper/test edits, runtime rerun, task .2 completion, full-plan
SUMMARY or model-evidence pass is claimed.

## Historical checkpoints — not current query-policy instructions

The following incremental records preserve actual earlier findings/approvals,
including failed or pending query routes. They do not recommend private mapper
construction or override current guide §6.1 / the WM-4 disposition above.

## Latest historical actual checkpoint — conditional report approval

**Recorded 2026-11-05T23:45:08Z UTC**, recording clock, not a supplied
message-event timestamp. Actual user selected **“Approve report model”**:
**APPROVED_CONDITIONAL_EXPLORATORY_ONLY** for report-history group 3,
subject to existing capability/recovery blockers **BEFORE IMPLEMENTATION**.
Not overall-model approval. This supersedes historical group-3 pending entries.

Actual independent report implementer **aff7f7f7**, inspection clock
**2026-11-05T23:40:41Z**, verdict **READY FOR STAKEHOLDER WITH QUALIFICATIONS,
AFTER PARENT TRIAGE**, conditional exploratory group 3 only. Static
source/paper review, not runtime/capability evidence.

| Actual finding | Parent/architect disposition |
|---|---|
| G3-C1 LOW: “recovery” wording could hide all-group report inventory | **FIXED IN SPEC by parent before approval recording**, schema §13.4 path row now says session-purge recovery has no report mutation/deletion; all-group initialization/history recovery still inventories reports. No cleanup API added |
| G3-Q1 HIGH: authoritative allocator/event history and effective durability | **UNRESOLVED RA-2/A09 gate retained before implementation.** Defaults/create-only/atomic add do not prove history or partition safety |
| G3-Q2 HIGH: all-writer exclusion, old-command settlement/continuity and complete inventory | **UNRESOLVED RA-1/RA-2 gate retained.** No reviewed executable facility; accepted quiescence requirement is not proof |
| G3-Q3 MEDIUM: full validation, original correlated outcomes, raw slots, zero retry, TTL/txn opt-out | **Normal implementation obligations retained**, not executed or independently closed as runtime evidence |
| G3-Q4 MEDIUM: symbolic sizing/lifecycle, no-expiry/read-touch and query retention | **Retained exploratory qualifications; R5/A09 UNRESOLVED.** Unknown volumes/budgets stay unknown; no capacity/erasure assertion, new cleanup/index/registry/cap or implicit retention window |

Groups 1/2 conditional approval is committed at `43fe23d`; report source
revision must be committed separately before derived-summary drafting.
All three groups now have actual conditional approvals, not a whole-contract
review. RA-1/RA-2 HIGH and R5/A09 stay UNRESOLVED unless completed evidence is
reviewed by the parent. Separately owned preflight partial artifacts/resources
are not read/adopted here. Shipped model documents name released artifacts only.

Next: derive `docs/aerospike-schema-summary.md` from the current guide as
**DRAFT_BLOCKED**, prepare the thirteen-method/whole-contract handoff, then stop.
Parent obtains completed capability results, fresh independent whole-contract
implementer review and final actual stakeholder acceptance. No independent pass
is inferred from this author's derivation or the group reviews. Task .2 remains
incomplete, .3 untouched, plan 1/3 tasks; no plan SUMMARY.

## Historical derived draft / whole-contract independent-review handoff

**Recorded 2026-11-05T23:47:31Z UTC** (author recording time).
Approved report-model source commit: **`c649d52`**
(`c649d526f6df0bd0f799edd3e52f461cc6205be9`), exactly schema, routing and
review ledger. Ignored planning was not staged. Derived **after** that commit:
[`aerospike-schema-summary.md`](aerospike-schema-summary.md), **DRAFT_BLOCKED**.
Guide SHA-256:
`0684d1889812f383381ed5447b8a4fcda922ee700e1269cc113ac7aa286989c1`.
Derived-summary SHA-256:
`034a0f2b6ed340659d517010a604f3d6c74136d055a058b56ab8060f4ed2548a`.
The summary's section mapping and thirteen-method matrix are source-condensed
from that exact guide. No generator/`derive-summary` execution or independent
author-review pass is claimed. Change the guide → regenerate/re-hash summary;
any independent review must bind the resulting exact pair.

### Unresolved feasibility / requirement-boundary review

These are **handoff flags, not newly approved requirements or silent
relaxations**. The approved model remains binding while blocked.

| Obligation | What is unproved / required parent disposition |
|---|---|
| RA-1 all-writer exclusion + old-command settlement + continuity | No actual enforceable procedure/verification adapter is established. Closing/killing clients, timeouts, sleeps, traffic silence, empty scans or a trusted boolean do not settle delayed commands. Need concrete coverage of old senders/queues/restarts/direct writers and recovery's own commands, across operator failure. Accepted user quiescence prerequisite is not proof. Unsupported facility → **REVISION_CONFLICT**, not a forever-refusing “implemented recovery” |
| Pristine initialization and all-time non-reuse | An empty current scope cannot prove no historical allocations/external vector references. hi=max(survivors), missing metadata=0 or create-only collision checks cannot prove history safety. Need a verifiable first-initialization authority/procedure and supported all-kind/external-ID inventory; missing or untrusted history blocks reopen. No automatic arbitrary-admin-rollback detector is supplied or required by host `CacheStorage` |
| Backup/restore and deletion history | The scalar provider does not own external vector backup/restore. Need an executable operator consistency boundary preserving all three allocator histories, q controls, s/r records and external IDs, plus verified supported restart/delete semantics. Detection/recovery after arbitrary malicious or unannounced admin rollback is not established; do not claim it. If the approved all-time identity wording is interpreted to require such detection without external authority, flag **REVISION_CONFLICT / requirement clarification**, not an invented provider guarantee |
| RA-2 effective policy/durability under AP partitions | Single-record add, CREATE_ONLY and default COMMIT_ALL do not prove one authoritative history across partitions or later restore. Need actual effective namespace/read/commit/server configuration and supported failure envelope; do not infer production partition safety from local multiprocess tests or SDK enum availability. No production partition-safety requirement beyond the approved model's stated prerequisites is newly added here |
| R5/A09 complete query/no-expiry/released compatibility | Query stream draining is not proof that all quiescent rows were enumerated or TTL/read-touch preserved. Need completed authorized transport/server/finite-default evidence for actual supported released artifacts, with limitations. Development-source support is not released compatibility evidence |

**Proposed minimal clarification for parent triage, NOT an adopted change:**
define the supported operational envelope as independently verified
single-authoritative persistent history with explicit initialization authority,
consistent operator restore/inventory and an executable quiesced recovery
procedure; block/refuse on known unsupported configuration, missing/corrupt
history or unavailable recovery proof. Ordinary successful create/CRUD/session/
report paths and closed/zero purge must be usable in that supported envelope,
not replaced by universal refusal. Exceptional recovery must eventually have a
real executable supported path before being called complete. Whether this can
actually be achieved is UNKNOWN. If stronger approved wording instead requires
automatic detection of arbitrary administration rollback, safe available AP
partition writes, or external-vector orchestration unavailable through existing
capabilities, return **REVISION_CONFLICT** with precise requirement/configuration
anchors. Do not silently narrow the approved contract or introduce SC/MRT,
new privileges/common APIs/workers/caps as a presumed solution. Parent must
resolve any actual conflict before implementation and seek stakeholder
acceptance for material requirement changes.

### Exact next independent review assignment

Parent first obtains/reviews the separately owned **completed** capability
result. No partial result/resource was read or adopted by this author; no
RA-1/RA-2/R5/A09 disposition changes on authorization alone. Then dispatch a
fresh implementer who did not author this model/summary. Review the **whole
contract**, not only report §13 or a re-use of prior group verdicts:

1. Read clarification A01–A09, routing, this ledger and the exact guide/summary
   pair above. Treat §12 as normative over failed §11.5–11.7 and historical
   pending statuses. Inspect host `CacheStorage`, SQL/manager/adapter/vector-ID
   consumers and actual supported SDK policy/result/operation sources.
2. Diff every set/key/bin/type/codec, SQL A03 truthiness/name precedence/tuple
   projection, A06 naive client-now/ignored input time, identity and timestamp
   constraint through the summary's **thirteen-method matrix**. Identify any
   omission, misleading condensation or obligation lacking a real mechanism.
3. Independently trace ordinary create/insert/read/report; shared writer
   allocation/exhaustion/collision/reopen; every entry/child/release/mark/purge/
   recovery crash/lost-reply boundary; concurrent purgers/name drop; result
   visibility/raw slots/per-segment TTL/txn opt-out/effective zero retry;
   malformed/duplicate/late-error streams and physical-key corruption.
4. Separate paper/source implementability, executable proof/runbook, observed
   local capability, released compatibility and production operational claims.
   Review complete inventory, pristine authority, external reference restore,
   history/durability/partition envelope, retained-report non-interference,
   no-expiry/query semantics and usable fail-closed behavior. Unknown workload
   metrics remain unknown; do not invent caps or reopen accepted A03/A06.
5. Return actual reviewer identity, UTC inspection time, input digests,
   independent verdict, anchored findings (required property + accepted
   requirement/spec/source), and explicit feasibility/REVISION_CONFLICT flags.
   Existing HIGH gates must be retained unless completed evidence actually
   resolves them. No pass awarded from document hygiene, author traces or
   existing group conditional approvals.

Parent triages every finding; any contract change updates guide first and
regenerates the summary for re-review. Only after the required gates/review
does the parent obtain final actual stakeholder acceptance of the entire
contract. No final acceptance or whole-review identity/time/verdict exists now.
Task 01-02.2 stays in progress; .3 remains untouched; no plan SUMMARY.

### Evidence helper shape limitation — minimal follow-up, no JSON fiction

Read-only `scripts/aerospike_evidence.py` v1 is an **acceptance gate**, not a
pending-model evidence writer: `approval()` accepts only status `"approved"`
(`:145–150`), `check_cases()` only `"passed"` (`:190–220`), and
`validate_model()` requires an independent current-pair approval and
`findings == []` (`:222–250`). It has no explicit conditional-approval,
unresolved-blocker or pending-whole-review representation. It also requires an
actual `derive-summary` command record/transcript and source digests; manual
condensation plus hashing is not an executed generator to invent a record for.
Do **not** relabel the actual conditional group approvals as unconditional,
erase RA findings, create synthetic command/test passes, or fabricate a
whole-review approval.

No `model-evidence.json` is written here. Proposed smallest separately scoped
follow-up: permit explicit pending/conditional statuses, unresolved blockers
and absent final review in an audit-record schema while the **acceptance
gate still fails closed** on any such status/blocker; keep actual approval
quotes/recording times and exact input digests, with honest derivation provenance.
This needs source/test review by the parent, not changes in this docs-only task.
Actual invocation of the existing read-only model gate rejected with
`model evidence rejected (EvidenceError)`, exit 1, with no acceptance record.
This is expected rejection, **not** a model-validation/test pass.

## Latest actual checkpoint — conditional groups 1/2 approval

**Recorded 2026-11-05T23:33:39Z UTC** (recording clock, not a supplied
message-event time). This entry supersedes historical pending-review/approval
statuses below, without overwriting their evidence.

Actual independent revised implementer **1f46acb4**, inspection clock
**2026-11-05T23:27:16Z**, verdict **READY FOR STAKEHOLDER WITH QUALIFICATIONS**
for reopened group-1 controls and group 2 only. This is static source/paper
review, not executed concurrency tests or capability evidence.

| Actual finding | Architect disposition |
|---|---|
| RA-1 HIGH: no verified executable all-writer exclusion AND old-command settlement/continuity facility | **Normal implementation/capability gate retained, UNRESOLVED.** The proof/refusal specification is accepted; no recovery mutation implementation before reviewed actual evidence. Always refusing is safe unavailability, not completed recovery |
| RA-2 HIGH: authoritative durable history, complete scoped enumeration, restart/restore/deletion semantics unverified | **Normal implementation/capability gate retained, UNRESOLVED.** No source enum, create-only operation, empty scan or default policy closes this |
| RA-3 MEDIUM: single-use release, irrevocable no-more-child-sends phase, effective zero retry and original correlated result | **Normal implementation obligation retained.** Unknown release is never replayed or inferred from later counts; append failure discloses known child persistence |
| RA-4 MEDIUM: identity/initialization/generation separation | **Normal implementation obligation retained.** Distinct non-reused numeric high waters, no missing-control defaults/automatic reconstruction/mixed-version migration; generation is only same-record CAS |
| RA-5 MEDIUM: preserve R1–R5 and SQL-facing edge cases | **Normal implementation obligation retained.** A03 truthiness/name precedence/full versus `(name,)` rows, A06 naive now/ignored DTO times, physical identity/raw slots/per-segment TTL/txn policies remain binding; R5 actual query retention proof remains UNRESOLVED A09 |

No new schema-clarity defect was raised by this recheck; no finding is silently
discarded or author-awarded a runtime pass. All HIGH activation blockers remain
**UNRESOLVED**.

**Actual user response: “Approve conditional model”.** Disposition:
**APPROVED_CONDITIONAL_EXPLORATORY_ONLY** for revised group-1 admission controls
plus group-2 session contract, **subject to resolving explicit capability and
recovery blockers BEFORE IMPLEMENTATION**. Accepted backend-only
`recover_session_purge(qids, *, recovery_context)` carries real proof/refusal
obligations; no shared `CacheStorage` API change is authorized. This real
decision permits the third/final entity-group draft, not implementation
readiness, production sizing, final summary/evidence or overall model approval.

**Actual user response: “Allow scoped inspection”.** Separately authorizes
SDK-local editable dependencies/index and a dedicated local development
container, pull disabled, new file-backed data. Inspection is separately
owned; no partial evidence reviewed/adopted and no blocker marked resolved.
Model documents name released artifacts only; development provenance stays local.

Task 01-02.1 remains complete (`eef1144`), .2 incomplete, .3 untouched:
plan 01-02 **1/3 tasks**; only plan 01-01 complete (**1/6 plans**).
Commit only the owned approved model-document revision before group-3 drafting;
never stage ignored local planning. Group 3 requires fresh independent
implementer review and then actual stakeholder approval before final derivation.

## Historical group 3 draft handoff — superseded by latest actual checkpoint

**Recorded 2026-11-05T23:34:56Z UTC**, author recording time.
Approved groups-1/2 revision was committed separately before this draft.
The third/final group candidate is schema §13; routing status is
**DRAFT_AWAITING_INDEPENDENT_IMPLEMENTER_REVIEW**.

- A05 history is independent, append-only and retained after question purge,
  no TTL/query/cleanup API. No copied-history erasure/compliance assertion.
- Granularity is one complete immutable event per append, distinct numeric
  scoped rid/high water, no unbounded collection/registry or speculative SI.
  The explicit PI/packing comparison remains symbolic; unknown population is
  not evidence that tiny-record overhead or allocator contention is acceptable.
- All six report arguments persist. Actual adapter sends the **plain `.answer`
  string**, otherwise `""`, not the Answer wrapper. Direct safe native values
  use §4's typed nodes; no unsupplied Answer type tag is invented. Native
  type/value and elapsed **numeric seconds** are preserved, no arbitrary-object
  or datetime.timedelta interpretation. Generated report time is naive local
  now. No common API/caller/source changes are made.
- §13.4–13.5 specify actual SDK create-only/all-bin atomic insert, guarded raw
  add/get ID allocation, exact key/result correlation, no-expiry/read-touch,
  effective zero retry, error/in-doubt/gap/duplicate/reopen semantics. Error
  correlation is redacted. Acknowledgment is not inferred from a later read.
- H1 normal hit/decode, H2 shared/scoped duplicate/lost reply, H3 purge/history/
  reopen traces and eight failure-mode checks are **author paper reasoning**.
  No (a) schema ambiguity remains identified by the author; SDK policy/codec/
  error plumbing is normal (b) engineering work. Independent review may refute
  this. No code/unit/live/capacity validation occurred.

**Requested independent scope:** reviewer must not have designed group 3.
Trace §13 and its actual host/SDK anchors, particularly plain-string/native-node
encoding, every six-field type, raw ID/results, all-record initialization,
failure phase/correlation, history after purge and non-interference with §12.
Findings require spec and accepted-requirement anchors; architect then triages
fix-in-spec/normal implementation/rejected-no-anchor, recording reasons.
No review identity/time/pass is fabricated for this pending gate.

Author validation caught an initial misreading of the adapter argument as
Answer rather than `.answer`; corrected the draft and examples before handoff.
This was an author source-check correction, not independent reviewer evidence.
Read-only standard-library assertions then passed for both JSON examples,
canonical scoped keys, bin-name lengths, native answer/float values, both actual
adapter `.answer`/numeric-seconds call sites, and byte-for-byte preservation of
§12's binding state-machine text since the approved-doc commit. `git diff
--check` passed; index is empty and only the three report-draft documents remain
modified. No SDK/server tests or capability probes ran.

Only after that review/triage, present actual report decision:
approve exploratory report model / request revisions / ask follow-ups /
explicitly defer review. Disclose unbounded total retained volume, no hidden
cleanup, potential duplicate explicit calls, no exactly-once guarantee, clear
oversize/capacity failures and unresolved PI/hot-key budgets. That future
decision is **not** obtained now and would not clear HIGH capability blockers.

All HIGH activation blockers remain **UNRESOLVED**: RA-1 real exclusion/
settlement/continuity recovery facility, RA-2 durable authoritative histories/
complete enumeration/deletion/restart/restore, R5/A09 real retention/transport/
released compatibility. Parallel inspection authorization is not evidence;
this author neither probes nor reviews its partial results.
Stop before summary/final model-evidence pass/overall review/plan SUMMARY or
persistent code/tests. Plan 01-02 remains **1/3 tasks**; only 01-01 **1/6 plans**.

## Gate ledger

| Stage / entity group | Actual status | Evidence / remaining gate |
|---|---|---|
| Step 0 clarification | RESOLVED_APPROVED_ASSUMPTIONS | Actual response and UTC recording time in clarification §6; A03/A06 SQL choices, other named assumptions accepted exactly |
| `cache-payload-identity` | ORIGINAL APPROVED_EXPLORATORY_ONLY; CONTROLS APPROVED_CONDITIONAL_EXPLORATORY_ONLY | Original approval `3e0bd4b` preserved; latest actual “Approve conditional model” accepts §12 controls subject to capability/recovery blockers before implementation |
| Independent implementer, first group | READY_FOR_STAKEHOLDER_WITH_QUALIFICATIONS | Follow-up review `ec5f6dbc`, recorded inspection time 2026-11-05T18:29:42Z; R1–R4 closed at specification/source-support level, R5 live proof remains A09 |
| Stakeholder, first group | APPROVED_EXPLORATORY_ONLY | “Approve group 1”, recorded 2026-11-05T18:33:44Z; permits session modeling only, not production/deployment/final acceptance |
| `session-associations` | APPROVED_CONDITIONAL_EXPLORATORY_ONLY | §11 association contract plus binding §12 replacement admission/release/purge/recovery; HIGH capability blockers remain unresolved |
| Independent implementer, original second-group candidate | BLOCKED — CONFIRM G2-C1, WITH A MINIMAL-BASELINE QUALIFICATION | Actual `0595eced`, inspection clock 2026-11-05T18:42:31Z; architect triage below |
| Recovery requirement / parent reopening | ACTUAL “Allow quiesced recovery” | Recorded 2026-11-05T23:19:11Z; requirement and scoped reopening only |
| Independent implementer, revised group-1 controls + group 2 | READY_FOR_STAKEHOLDER_WITH_QUALIFICATIONS | Actual 1f46acb4, inspection 2026-11-05T23:27:16Z; RA-1/RA-2 HIGH retained, RA-3–RA-5 warnings triaged above |
| Stakeholder, revised controls / second group | APPROVED_CONDITIONAL_EXPLORATORY_ONLY | Actual “Approve conditional model”, recorded 2026-11-05T23:33:39Z; blockers before implementation; permits report drafting only |
| `reports-history-cleanup` | APPROVED_CONDITIONAL_EXPLORATORY_ONLY | Actual “Approve report model”, recorded 2026-11-05T23:45:08Z; aff7f7f7 ready with qualifications, parent G3-C1 fixed, G3-Q1–Q4 retained |
| Derived summary / final all-method review | DRAFT_BLOCKED / WHOLE_REVIEW_PENDING | Current-pair source mapping and thirteen-method matrix derived after c649d52; no final/model-evidence acceptance |

## First-group draft presented for independent review

Historical entries below retain their original pending-gate language and line
anchors. The current ledger above and final dated entry supersede those statuses,
not the original findings or qualified approval.

Schema: one `gptc_q` record per numeric question ID with immutable safe typed
payload and mutable state/access time; one fixed-size `gptc_meta` high water per
scope. Direct qid access, atomic guarded range reservation, create-only payload
insertion, explicit no-expiry intent. Complete scoped scans for enumeration/
count/tombstone discovery; no indexes or unbounded registry.

Tradeoffs: each touch rewrites the payload record; allocator is a per-scope
write hotspot; range gaps remain permanently; scans have proportional work;
unknown production sizes/rates/retention provide no fit/SLO assurance. Naive
local times intentionally match SQL instead of being portable instants.
Capability/durability/partition verification is an activation blocker, not a
claimed deployed capability. Cross-group session purge/admission fence and
initial-association failure semantics remain open interfaces, not physical
design or approval of future groups.

### Architect walkthrough record (not independent, not executed tests)

1. **W1 create/read:** exact meta/q keys, create-only race, tagged native/optional
   DTO, ignored supplied timestamps, read-before-write prior access. Paper
   question-local trace passes; deployed policy/API support not yet validated.
2. **W2 mutation/shared allocation:** concurrent n-sized reservations disjoint
   within authoritative record history; failures keep high water/gaps and
   per-input outcomes, no overwrite/false success. Paper trace passes; actual
   multi-process and failure execution still required.
3. **W3 mark/get/purge:** one-way tombstone, atomic active read, conditional
   deletion, retry-safe tombstone retention on cleanup failure. Question-local
   trace passes; whole cascade intentionally not closed until session group
   proves its admission fence.

### Self-check findings and disposition

These are author findings, not manufactured independent implementer evidence:

| Finding | Disposition |
|---|---|
| Uniform batch put would write same payload for all keys | Fixed in guide §5: heterogeneous chained create-only segments, per-segment expiry, index/key correlation |
| Read-before-write needs unambiguous old-time result | Fixed in §5: ordered get slots, raw operation_result(6), put is slot 7; no typed default coercion |
| Filtered-out could hide corruption as absence | Initial self-check was incomplete: R1 found missing SDK visibility flags. Revised §5 now binds flags, error strategy and diagnostic classification; independent recheck pending |
| Allocator reset/recovery could reuse purged referenced IDs | Fixed in §§5–6/8: never reset, missing-on-reopen fail closed, quiesced all-group/external-reference recovery |
| AP partition or lost allocator durability defeats unconditional uniqueness | Open activation gate §6, not a fabricated SC/deployment claim |
| Parent check alone cannot prevent late association after purge | Cross-group obligation §8; no pre-decided session storage; final purge blocked pending session design |

## Actual independent findings and architect triage

**Reviewer identity as recorded:** “OpenAI coding subagent, independently
dispatched in this session as the developer who must implement the contract.”
The reviewer states it did not author/participate in this group's design.
The original review and independent follow-up are retained in the local
execution handoff; developer-checkout provenance is not a shipped dependency.
The actual follow-up review ID supplied with the continuation is `ec5f6dbc`.
No human identity or unprovided event timestamp is invented.

**Original reviewer verdict:** blockers to an unqualified walkthrough pass/
stakeholder handoff (R1–R3); warnings R4–R5; source-supported model, not
deployment/capability verification. The following is actual **architect triage
in this revision**, not a replacement verdict attributed to that reviewer.
Required properties are binding; reviewer fix hints were examples, independently
checked against SDK sources before selecting the contract.

| Finding / requirement anchor | Architect classification and actual disposition |
|---|---|
| **R1 HIGH:** default point get/touch omits FILTERED_OUT; F04/A07 require deleted-vs-corrupt-vs-failure classification | **Fix in spec — corrected, pending independent recheck.** §5 Keyed result contract now requires include_missing_keys + fail_on_filtered_out + execute(on_error=ErrorStrategy.IN_STREAM) on all keyed paths, including diagnostic/allocator/mark/purge. Point outcomes table requires keyed row completeness, preserves code/exception/in-doubt, separates explicit absence/filtered/corruption/failure and forbids treating an unexplained empty stream as a miss. Filtered diagnostics are read-only exact-key **unfiltered** inspections, validating full record rather than filtering corruption away; still-active rejection is an error, not silent replay. SDK query_shared visibility/actionability logic and sync query_dispatch single-key path support the chosen flags/strategy. No spec deferral |
| **R2 MEDIUM:** lost touch reply + retry can return this call's own new time; A06/F04 require prior access | **Fix in spec — corrected, pending independent recheck.** §§5–6 require effective max_retries=0 on mixed read/touch WRITE_NON_RETRYABLE/POINT and relevant overrides/modes/reload. Slot 6 of the acknowledged original attempt is the only successful prior access; timeout/lost reply/in-doubt surfaces uncertainty, never replay or read-after-loss reconstruction. SDK write-policy selection maps effective Settings.max_retries; defaults are not sufficient. Future policy assertions/lost-reply validation required; no weakening of old-access requirement |
| **R3 MEDIUM:** count distinct question vs scan deliveries, including conflicting state observations, ambiguous; F05/no snapshot | **Fix in spec — corrected, pending independent recheck.** §5 explicitly deduplicates validated physical question identities **before** state selection for get_ids, count/is_all and purge discovery. First validated observation's state wins for that call; every repeat is validated but never counted twice/reselected. Complete drain/no prefix, quiescent distinct correctness and nonsnapshot concurrent insertion/mark/purge semantics are explicit. No new snapshot/registry/counter is assumed |
| **R4 MEDIUM warning:** scan bin qid could redirect enumeration/purge unless bound to actual key; F03/F05/A07 | **Fix in spec (strengthened from proposed normal implementation) — corrected, pending recheck.** §5 now binds configured namespace, fixed set, exact scope, fmt, qid type/range and state to the returned key's namespace/set/digest reconstructed via DataSet.id from §3. Missing/unusable/synthetic key metadata or mismatches fail closed, including repeat rows. SDK exposes DataSet.id and returned Key metadata; no stored user key/new bin/API is required. Validation primitive choices remain normal implementation work, but the integrity property is now explicit |
| **R5 LOW warning:** reads_query Settings is not query read-touch wire proof; A09 no-expiry | **Fix in spec for overclaim; normal implementation + OPEN A09 capability/live gate for proof.** §6 distinguishes forwarded point/batch read-touch fields from to_query_policy, which does **not** forward that field. Query TTL/read-touch transport/retention semantics remain UNVERIFIED, blocking activation pending actual supported-transport inspection and later authorized finite-default/void-time validation. No invented QueryPolicy setter, source-only retention guarantee or live pass. Explicit never-expire stays mandatory on all writes |

No finding is rejected or silently deferred. R1–R4 contract changes are author
resolutions awaiting independent recheck; R5's query transport/live proof remains
open rather than being “fixed” by an unsupported API. Source inspection found
the needed keyed visibility, policy/retry and key-identity surfaces; no binding
property is known to require an unsupported workaround (`REVISION_CONFLICT`
not established). If recheck or authorized capability work disproves support,
stop with `REVISION_CONFLICT`/blocked activation; never relax the property.

### Revised architect walkthroughs and remaining obligations

- **D1/W1:** acknowledged ordered read/touch returns captured slot 6; explicit
  missing returns None; visible filtered → exact-key diagnostic; corruption or
  transport failure raises. Lost reply raises doubt with zero retries/no replay.
  Revised paper trace only; no executed failure test or independent pass.
- **D2/W2:** existing atomic guarded allocator/create-only range and per-input
  partial-outcome rules retained. Keyed flags now make missing/filtered
  allocator inspection explicit. AP/restore/history loss remains A09, not fixed
  by collisions against surviving questions.
- **D3/W3:** mark/purge outcomes explicit; every scan row's physical identity
  validates before first-observation selection/dedup; count and IDs cannot
  double-count a migration delivery. Guarded delete rechecks after future
  cleanup fence. Question-local revised paper trace; no global snapshot or
  proven cross-group cascade.

The review's cross-group obligations (admission/purge fence, initial association
failure and row-shaped projection, pristine initialization/recovery across all
record kinds/external refs, independent retained reports) remain §8 interfaces,
**not** new entity designs or resolved proofs. A01 unknown workload/size/
contention/scan budgets and A09 actual consistency/limits/no-expiry/deletion/
restart/restore gates are retained. Codec/corruption tests, redacted diagnostics
(not RecordResult repr), acknowledged-only flush and lifecycle-owned close are
implementation obligations, not evidence produced by this revision.

## Independent recheck handoff and acceptance order

Parent independently rechecks the revised first group against the actual
review artifact, clarification, schema guide, this ledger and host/SDK sources.
Any additional reviewer must not be this model author or a participant in its
design. Review spec clarity/
requirements implementability, exact key derivation, operation ordering/return
slots, partial failures, no-expiry policies and cross-group obligations.
Do not ask reviewer to redesign future entity groups.

Record actual findings with guide and requirement anchors. Architect triages
each as fix-in-spec, normal implementation decision, or rejected/no valid anchor;
fix or explicitly disclose every deferred clarity issue before the stakeholder
checkpoint. Original reviewer findings are recorded above; no revised reviewer
pass, timestamp or stakeholder approval is inferred from author triage.

Then present the first-group stakeholder decision:

> Approve the exploratory `cache-payload-identity` group only; request revisions;
> ask follow-ups; or explicitly defer this group's review.

Approval would not certify production sizing, namespace/SDK capabilities,
cross-group purge correctness, final summary or persistent implementation.
Only a recorded real approval/explicit deferral after independent review permits
the next group. Current ledger records **no such response**.

## Actual first-group follow-up and stakeholder decision

This entry supersedes the historical pending-recheck/pending-approval wording
above; it does not relabel the author's earlier paper traces as independent tests.

**Independent follow-up `ec5f6dbc`:** actual inspection time recorded by the
reviewer is `2026-11-05T18:29:42Z`. Verdict:
**READY FOR STAKEHOLDER WITH QUALIFICATIONS — current group only.**
R1–R4 are closed at the specification/source-support level. R5's specification
overclaim is resolved; dataset-query transport/retention proof remains
**UNVERIFIED under A09, blocking activation**. No remaining current-group
implementer-contract blocker was found. No live/runtime/capacity pass is claimed.

**Actual stakeholder response:** “Approve group 1”, delivered in the continuation
instruction against the question:

> Approve exploratory cache-payload-identity model so modeling can proceed to
> session associations? Not approval of production capacity/deployment
> capabilities/final complete model.

**Recorded:** `2026-11-05T18:33:44Z` (UTC recording time; no separate message-event
timestamp supplied). Disposition: **APPROVED_EXPLORATORY_ONLY**.
This is real first-group approval, not inferred from clarification or reviewer
readiness. It permits drafting group 2, not implementation, group 3 design,
schema summary, final model evidence, or task/plan completion.

Binding closures retained: R1 visible missing/filtered keyed outcomes with
IN_STREAM and unfiltered diagnostic classification; R2 effective zero retry and
prior access solely from raw slot 6 of the acknowledged original touch; R3
first-validated distinct physical-question state/count; R4 reconstructed physical
digest/scope validation; R5 query-wire proof still the A09 gate. Immutable
payload/identity/creation time and one-way state remain unchanged.
Session admission/cascade and all-record initialization/recovery are explicit
cross-group obligations, not approved implementations. If group 2 needs parent
coordination changes, flag/reopen the approved control contract before adoption.

## Group 2 candidate and actual conflict handoff

**Recorded 2026-11-05T18:37:34Z** (author recording time).
Schema guide §11 contains only the session-association candidate: all accepted
input dispositions, granularity/alternative/PI/contention worksheets, versioned
key/bin/JSON shapes, independent association identity, no-expiry/error policies,
all SQL truthiness/name precedence paths, full DTO/name-row projection, manager
ID deletion, and initial-association failure correlation. No report schema,
summary or persistent code is supplied.

**Author finding G2-C1 — fix in spec / `REVISION_CONFLICT`, unresolved.**
Requirement anchors: clarification A04/A08 and F06/F09; approved schema §8.3.
Specific gap: §11.5 demonstrates a writer observing active parent before mark,
creating a child after purge's scan/parent deletion, and crashing before
postcheck/compensation. Independent point filters/CAS cannot make parent check
and child write atomic. A successful purge cannot silently retain that child.
Failure visibility alone does not close the schedule. This is not a new
independent reviewer finding or an allegation that group 1's qualified approval
closed its explicit cross-group interface.

**Author disposition:** do not weaken ownership, claim read-check-write atomic,
add an unbounded registry, reset a crashed admission by timeout, or assume MRT.
Leave the candidate blocked; ask the new exceptional recovery requirement in
§11.7. A fixed-size parent admission fence with verifiably quiesced exceptional
recovery would explicitly reopen group 1; it is an option, not adopted control
bins. Online crash-recoverable alternatives may depend on capability inspection
under A09, not source enums. A bounded parent-local CDT is conditional on a new
growth/overflow requirement, not A01's unknown-fan-out approval.

**Walkthrough disposition:** S1 read/shape/duplicate trace conditional on a
future correct admission protocol, not end-to-end passed; S2 manager association-
ID drop/partial retry is paper-source supported; S3 lifecycle/crash **FAILS**
with schema/control gap (a). No group-2 developer checkpoint pass or stakeholder-
ready verdict is asserted. Existing-child interrupted cascade with retained
tombstone is retryable, but that fact does not fix late admission.

**Independent inspection requested, not received:** reviewer must be independent
of this design, trace §11 against the accepted clarification and actual SDK
builder/result/CAS/filter/query logic, and confirm/refute G2-C1 with source and
requirement anchors. Inspect parent-check/child-write/postcheck crash boundaries,
physical-parent-gone orphan, cascade retry/unknown outcomes, duplicate DTOs and
manager association-ID deletion. Do not treat a confirmed blocker as approval.
Any findings require author triage; a correct revision needs renewed walkthrough
and independent review before the actual group-2 model decision. No reviewer
ID/time/verdict is fabricated for this pending inspection.

**Exact next stakeholder requirements question, after independent inspection:**

> After a crashed or in-doubt session addition, may purge fail closed with its
> parent tombstone retained until caller-driven recovery can verifiably stop
> all writers and settle pending database commands, or must recovery work while
> other writers continue? If quiescence is acceptable, may we reopen group 1's
> parent control contract for a fixed-size admission fence and re-review its
> effects, without changing immutable payload/state/timestamps?

This is **not** “approve group 2 so reports may proceed.” No such approval is
requested while S3 fails. Actual resolution/revision/re-review and then actual
group-2 stakeholder approval are required before group 3. Reports are not yet
designed. Plan 01-02 remains 1/3 tasks complete; workgroup 02 remains incomplete.

## Actual recovery answer, independent findings and architect revision triage

**Recorded 2026-11-05T23:19:11Z** (UTC recording time of this continuation,
not a supplied message-event time). Actual user selected **“Allow quiesced
recovery”** after crashed/in-doubt admission: purge may fail closed, retain
tombstone until caller-driven recovery verifiably stops all writers and settles
outstanding commands. User explicitly authorized reopening group-1 fixed-size
parent controls; did **not** approve a revised schema, group 2 or code.
Immutable payload/state/timestamp semantics remain binding.

**Actual independent review received:** `0595eced`, original candidate,
inspection clock **2026-11-05T18:42:31Z**. Verdict **BLOCKED — CONFIRM G2-C1,
WITH A MINIMAL-BASELINE QUALIFICATION**. It independently reconstructed the late
child after parent deletion, even assuming perfect single-record history and
accurate scans. Child CAS/postcheck/repeated scans are not an atomic parent
fence. Reviewer inspected a credible guarded existing-only counter primitive,
but selected no bins/API and approved no revision.

| Actual finding / anchors | Architect classification and disposition in this revision |
|---|---|
| G2-C1 / G2-R1: missing admission fence; A04/F06/F09, §8.3 | **Fix in spec.** Accept. §12.2–12.6 drafts fixed-size afmt/adm, atomic active/capacity entry on parent, mark serialization, single-use settled-child release, closed/zero cleanup then guarded delete. Historical §11.5 remains failure evidence, not current algorithm. Pending independent recheck, no author-awarded closure |
| G2-R2: stuck counter is liveness, not global impossibility; A04/A08 | **Fix in spec / requirements decision.** Accept qualification; actual user allowed exceptional quiescence/reopening. No MRT-only claim. Counter can strand and block purge; ordinary shared-writer progress and exceptional recovery separated. No deadline or permanent-tombstone purge success promised |
| G2-R3: unknown decrement can consume someone else's admission; A04/A08 | **Fix in spec.** §12.4–12.5 requires effective zero retry on entry/release, no child on unknown entry, no release on unsettled child, local irrevocable phase close before one release, no replay of unknown release/read-based ownership inference; owner/restart obligations explicit |
| G2-R4: quiescence needs exclusion AND settlement; A04/A08 | **Fix in spec for contract; actual deployment proof OPEN A09.** §12.7 names proposed backend-only `AerospikeStorage.recover_session_purge(qids, *, recovery_context)` in the existing provider module, exact external inventory/exclusion/settlement/continuity proofs and cleanup/reset/delete order. No facility verified; entrypoint must refuse mutation absent reviewed actual mechanism. Boolean assertion, process kill, timeout/empty scan not proof. New common API/privileges or unsupported prerequisites require new decision/REVISION_CONFLICT |
| G2-R5: MRT SDK surfaces are not online recovery/capability proof; A09 | **Normal implementation/capability gate, explicitly retained.** No MRT/SC/index permission assumed or made mandatory; no convenience retry runner. Authoritative history, durability/deletion/restart/query retention and released compatibility still UNVERIFIED, activation blocked |

All actual findings are accepted and triaged; none rejected without an anchor or
silently marked independently closed. This revises **only reopened parent
controls and session associations**. §12 preserves R1 keyed visibility/unfiltered
diagnostics, R2 original-touch slot 6/zero retry, R3 validated first-observation
scan semantics, R4 physical digest/scope integrity, and R5 query-wire/live gate.
All writes/metadata remain never-expire; update-only guards never resurrect.
A03 name precedence/SQL row projections, A06 naive now/ignored DTO times,
positive numeric IDs and duplicate-instance semantics are unchanged.

### Revised author walkthrough / actual next gate

- §12.8 W/S1: known entry/create/release, duplicate identities, full DTOs versus
  `(name,)` projection, manager association-ID deletion — paper supported.
- W/S2: entry-before/after-mark, paused writer, zero-accounting cleanup,
  concurrent purgers — old schedule excluded by parent serialization, conditional
  on A09 history/complete quiescent enumeration.
- W/S3 + §12.5: pause/crash/lost reply at every entry/create/release/mark/purge/
  recovery boundary; uncertain decrement never replayed; externally excluded and
  settled operator reset only — safety paper argument, external capability
  evidence not supplied. Operational recovery remains blocked absent that proof.
- §12.1/12.8 retains symbolic sizing/PI/contention forms and adds two full-parent
  rewrites per append; fan-out/rates/SLO/hot-key measurements remain UNKNOWN.

These are **architect traces, not independent or executed tests**. Renewed
independent implementer must review reopened group 1 and group 2 as a combined
control/ownership boundary, including external proof enforceability and refusal
when unavailable. Record actual new findings and triage them; then present the
actual stakeholder approval checkpoint. Do not attribute this draft to reviewer
0595eced or reuse group-1 approval as approval of new controls.

No group 3 design, persistent/test code, live tests, schema summary,
model-evidence.json approvals or plan summary. Task 01-02.1 complete `eef1144`;
01-02.2 incomplete (plan 1/3 tasks), only 01-01 complete (1/6 plans).
Drafts remain unstaged/uncommitted. Local planning remains commit_docs:false,
globally ignored and never staged.
