# Aerospike entity-group routing plan

**FINAL status override — recorded 2026-11-06T04:43:56Z UTC:** actual user
**“Approve and implement”** accepts the FULL revised implementation contract
within clarification §8's approved envelope, after the independent whole-model
reviewer's 2026-11-06T01:23:30Z **IMPLEMENTATION_CONTRACT_READY** follow-up.
The exact approved guide/summary pair and actual reviewer quotes are recorded
in [the final decision ledger](aerospike-model-review.md#final-decision--approved-implementation-contract).
This decision supersedes historical pending/draft authoring-status metadata
below and in the byte-preserved guide/summary, not normative model semantics.
Original group approvals/conditional revisions retain their real chronology;
final overall acceptance closes model-design conditions, not future proof.
WM-1–4 close at model-contract/feasibility only. Full provider/operator units,
live integration/fault/TTL/manual evidence, supported released-server validation
and benchmarks remain OPEN in their later gates; none is waived or claimed.

**Latest final-revision override (2026-11-06):** guide §6.1 adopts actual
conditionally authorized, compiled/wire/TTL-proven SDK-only namespace0/set0-or-1
constraint; old native bridge superseded. Completed operator preflight with P2
fix supports §14 model feasibility (P1 STOPPED precreate accepted, P3 absent-root
safe refusal qualified), not final-provider helper tests or independent closure.
All three group approvals remain conditional exploratory. Fresh independent
exact revised guide/summary review and final actual full-contract approval
remain required. Task .2 incomplete/.3 untouched; historical pending-proof
statements below describe earlier checkpoints, not a new all-fault pre-code gate.

**Current continuation, recorded 2026-11-06T00:20:02Z UTC:** actual
“Approve minimal envelope” / “Allow scoped tooling” are requirements authority
in clarification §8, not revised model acceptance. Three groups retain their
conditional exploratory approvals. Cross-group bootstrap/operator contract
revision in guide §14 is **DRAFT_PENDING_FINAL_REVIEW**; whole-model independent
verdict remains BLOCKED. WM-1/WM-2/WM-3 are addressed in a contract draft, not
closed by approval. Operator preflight and separate WM-4 query-policy result
review, fresh independent review of the exact guide/derived-summary pair, then
final actual stakeholder implementation-contract approval remain pending.
Task .2 is incomplete (1/3 tasks; 1/6 plans complete). No code or final evidence.

**Latest report disposition — recorded 2026-11-05T23:45:08Z UTC**
(recording time, no separate message-event timestamp supplied): actual user
**“Approve report model”** approves group 3 **conditionally for exploratory
modeling only**, subject to existing capability/recovery blockers **BEFORE
IMPLEMENTATION**. Reviewer **aff7f7f7**, inspection 2026-11-05T23:40:41Z,
ready with qualifications; parent fixed G3-C1 LOW in schema §13.4. All-group
integrity still inventories reports; session-purge recovery cannot mutate/
delete them. G3-Q1–Q4/RA-1/RA-2/R5/A09 remain binding and unresolved where
capability evidence is required. This supersedes historical pending group-3
statuses. All three groups have actual conditional exploratory approval;
derive a DRAFT_BLOCKED summary only after committing the approved report
sources, then obtain independent whole-contract review and final stakeholder
acceptance. No overall approval or implementation/task completion.

**Latest actual decision — recorded 2026-11-05T23:33:39Z UTC:** “Approve
conditional model” accepts revised group-1 admission controls plus group 2 for
**exploratory modeling only**, subject to resolving explicit capability/recovery
blockers **before implementation**. Independent revised review is ready with
qualifications. Groups 1/2: **APPROVED_CONDITIONAL_EXPLORATORY_ONLY**; this
supersedes their historical pending statuses below, not the binding §12 fence.
All HIGH activation blockers remain **UNRESOLVED**. The backend-only recovery
entrypoint is accepted with proof/refusal, not a shared API change.
Group 3 may now be drafted; no implementation readiness, final summary or
overall approval is implied. “Allow scoped inspection” is authorization, not
evidence. UTC is recording time, no separate message-event time supplied.

**Incremental routing/status — Step 0 `RESOLVED_APPROVED_ASSUMPTIONS`.** The stakeholder approved
the dispositions in [the Clarification Document](aerospike-clarification.md#6-actual-stakeholder-disposition-log-and-resume-contract).
The group order below is proposed review routing, not an approved execution
outcome. Original first-group approval is exploratory only (`3e0bd4b`).
**Historical, superseded entry 2026-11-05T23:19:11Z:** actual **“Allow quiesced recovery”**
authorizes reopening only fixed-size parent controls. Revised controls and
group 2 are **not approved**; no production/deployment/full-model approval.
Independent `0595eced` confirmed G2-C1 with a minimal-baseline qualification
(inspection clock 2026-11-05T18:42:31Z); architect triage is in the review ledger.

An entity in this document is not a promise of one set/record. No key/bin/index,
allocator algorithm, consolidation/split, sharding or materialization choice is
selected by this routing document. Conditionally approved groups 1/2 and the
third-group draft are in [the schema guide](aerospike-schema.md).
Unknown inputs are not cardinality ceilings.

## Group: cache-payload-identity

- **Review domain:** scoped cached question, ordered answers/dependencies,
  optional scalar embedding, numeric question identity and its coordination
  lifecycle. This group establishes host identity and state vocabulary before
  association/report references are reviewed.
- **Access coverage:** `create`, `batch_insert`, `get_data_by_id`, `mark_deleted`,
  `get_ids`, `count`; payload side of `clear_deleted_data`; `flush`/`close`.
- **Routing:** shared sizing/contention §§5.1–5.2; Pack 1 (§5.4) for owned
  question children; Pack 5 (§5.8) for cleanup obligations; cross-cutting ID,
  timestamp and small-independent-entity/primary-index cost guidance for
  coordination metadata. Do not presume that metadata requires a separate set.
- **Confirmed:** numeric positional IDs compose with FAISS; active `0`/deleted
  `-1`; previous-access return; absent/deleted reads; shared scoped writers;
  no TTL, no arbitrary pickle, explicit oversize failure without chunking.
  See clarification F01–F05/F11.
- **Approved unknowns/refinements:** all §4 sizing, rate, skew, contention,
  repetition and growth inputs (A01); supplied/returned times (A06); native/
  optional shapes (A07); retry semantics (A08); capabilities (A09).
- **Status:** original `APPROVED_EXPLORATORY_ONLY`; reopened parent controls
  `APPROVED_CONDITIONAL_EXPLORATORY_ONLY` by the actual latest decision.
  The §12 afmt/adm fence remains binding. Immutable payload/state/timestamps,
  numeric identity and R1–R5 stay binding. Provisional pattern: one immutable
  complete payload per question plus separate fixed-size per-scope high water;
  exact contract in schema guide §§2–5, not production/implementation-ready.
- **Current review gate:** Step 0 resolved; actual independent implementer review
  received with R1–R3 blockers and R4–R5 warnings. Architect triage and first-group
  corrections are recorded in [model review](aerospike-model-review.md): visible
  keyed outcomes, zero-retry prior-access touch, distinct first-observed scan
  identities and physical key validation; query retention transport proof
  remains an A09 gate. Independent follow-up `ec5f6dbc` at
  2026-11-05T18:29:42Z is READY FOR STAKEHOLDER WITH QUALIFICATIONS.
  Actual response “Approve group 1” recorded 2026-11-05T18:33:44Z approves
  exploratory modeling so session associations may proceed, not production
  capacity/deployment capabilities/final complete model.
  Review
  create/read, ordered batch partial failure, shared allocation, update-only
  deletion, touch races and purge scenarios; complete sizing/contention/PI-cost
  worksheets with honest input status. Developer walkthrough, independent
  implementer findings and stakeholder outcome must be recorded before the next
  group starts. Independent source review occurred, not live validation or group
  approval. First-group gates are recorded; renewed combined group-1/2 review
  and conditional actual approval supersede the historical review status.

## Group: session-associations

- **Review domain:** question ↔ session-name associations, independent
  association identity, session-question text, initial association on insert,
  name-drop and parent lifecycle. No authentication/session-account model is
  implied.
- **Access coverage:** initial association in `batch_insert`; `add_session`;
  `list_sessions` by name, question, both, neither; `delete_session` by
  association IDs; session view on `get_data_by_id`; association cascade in
  `clear_deleted_data`.
- **Routing:** shared §§5.1–5.2; Pack 2 (§5.5) for both-direction N:M reads;
  Pack 1 (§5.4) for parent-owned association fan-out; Pack 5 (§5.8) for
  deletion/race/recovery; ID and small-entity PI-cost gates. Traversal Pack 3
  is not applicable.
- **Confirmed:** manager consumes association DTO fields and deletes by
  association IDs, not question IDs; session-name drop does not evict parents
  or vectors; complete listing paths cannot be silently truncated.
  See clarification F06–F09.
- **Approved unknowns/refinements:** degree distribution on both sides, edge
  bytes/churn/skew/growth (A01); duplicate identity (A02); filters/return shape
  (A03); missing/deleted parent admission and purge races (A04); cleanup/retry
  guarantees (A08); capabilities (A09).
- **Status:** `APPROVED_CONDITIONAL_EXPLORATORY_ONLY`; §11 retains association
  shapes/decision worksheets and historical failed independent-write schedules;
  §12 supplies the replacement fixed-size parent admission/release/purge draft.
  Guarded entry serializes with mark; no release until child commands settle;
  zero/closed plus complete cleanup precedes guarded parent deletion. Unknown
  decrement is never replayed. Caller owns ordinary release; operator recovery
  alone may reset abandoned accounting under actual all-writer exclusion **and**
  command settlement. Otherwise retain tombstone and fail visibly.
- **Current review gate:** independent revised review ready with qualifications;
  actual conditional exploratory approval recorded above and in the review
  ledger. RA-1/RA-2 HIGH and R5/A09 remain UNRESOLVED; no actual exclusion/
  settlement/continuity facility, authoritative-history/deletion/query-retention
  evidence has been accepted here. §12.7's backend-only entrypoint and proof/
  refusal are accepted, no boolean-as-proof, new privilege or common-API change.
  A03 SQL truthiness/name precedence/tuple shapes and numeric association-ID
  deletion stay binding. No implementation readiness. Group-3 drafting permitted.

## Group: reports-history-cleanup

- **Review domain:** durable hit report, copied text/value fields and numeric
  parent reference, scope ownership and history retention; cross-group cleanup
  completeness. No new reporting query or timeline API is introduced.
- **Access coverage:** `report_cache`; report effects (if any) of
  `mark_deleted`/`clear_deleted_data`; synchronous acknowledgment/`flush` and
  failure recovery across all selected record kinds.
- **Routing:** shared §§5.1–5.2; Pack 4 (§5.7) to explicitly classify event
  inputs and absence of timeline/unread reads; small-independent-entity/PI-cost
  guidance for append-only events; Pack 5 (§5.8) for lifecycle side effects and
  reconciliation. Pack 4 routing does not prescribe event buckets.
- **Confirmed:** host reports are write-only through `CacheStorage`; adapter
  supplies six values; SQL reference retains reports after parent purge.
  No TTL and visible report errors are already required. See F09/F10.
- **Approved unknowns/refinements:** report size/rate/skew/horizon and operational
  budgets (A01); historical independence/copied-payload erasure (A05); safe
  values/time where applicable (A06/A07); replay/unknown outcomes/recovery
  (A08); capabilities (A09).
- **Status:** `APPROVED_CONDITIONAL_EXPLORATORY_ONLY`, actual response
  “Approve report model”, recorded 2026-11-05T23:45:08Z UTC. Schema §13 contains
  only this third/final group:
  complete immutable copied append, scoped numeric instance identity, all six
  inputs including actual plain-string/safe-native handling and numeric
  elapsed seconds, per-set examples, atomic paths, no-expiry/failure rules and
  symbolic sizing/PI/hot-key comparison. No production caps or retention claim.
- **Author walkthrough/self-check:** H1 ordinary adapter hit/diagnostic decode;
  H2 parallel/scoped duplicate appends and lost replies; H3 parent purge,
  independent retained history and non-pristine reopen. Conditional paper
  traces, not independent or live tests. Existing parent fence remains binding.
- **Review gate:** actual independent report review aff7f7f7 is ready with
  qualifications; G3-C1 parent wording fix accepted, G3-Q1–Q4 retained.
  Actual separate report stakeholder approval is recorded above; it does not
  resolve the existing preimplementation gates. Unknown volume/retention
  means unlimited set growth until capacity failure, not an approved capacity;
  question purge is not copied-history erasure. No report read/cleanup API.

## Common exit rules — first draft only

1. Actual stakeholder disposition of Step 0 is recorded; it approves requirements
   only, not this routing order, physical patterns or any model review.
2. Each group separately clarifies remaining inputs, applies its packs and cost
   gates, drafts only that group's model, runs implementer walkthroughs and
   pauses for the stakeholder. A previous gate cannot approve a later group.
3. Missing production measurements remain unknown under any expressly approved
   exploratory assumption. A capability- or sizing-dependent decision still
   lacking justification stays blocked; no invented threshold repairs it.
4. After all groups pass, derive the schema summary from the authoritative
   guide, audit all thirteen behaviors/scopes/failure paths and obtain the
   final independent implementation-contract acceptance.
5. Persistent implementation is a later task. All three groups have conditional
   exploratory approval, with capability/recovery blockers before implementation.
   A derived draft summary/whole-contract handoff is now permitted; no final
   model evidence or implementation authorization is produced. Plan 01-02
   remains 1/3 tasks complete; only 01-01 is complete (1/6 plans).
