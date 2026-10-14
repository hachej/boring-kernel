# boring-kernel — specification (draft v0.5)

Status: draft v0.5 for review, 2026-10-14. Evidence: boring-factory `docs/research/spike-ledger.md` (double-entry
ledger on Node and a real Durable Object). Principles: **simple, robust, elegant**. Strict only where a mistake is
costly or invisible (data, rights, history); loose elsewhere, because agents repair.

History: v0.2 (receipts, change images, projections, scoped reads, approvals, trust boundary), v0.3 (normalized
images, indexed reads, unique keys, counters and timers as records, `jobs`, one-level child scope, feed paging,
archive chunking). v0.4 tightens the interactions between guarantees: one **commit envelope** for complete recovery
and one hash chain; a fresh **recovery epoch**; **latched seals** and retention lower bounds; backfills never rewrite
records; version-stable **receipt identity**; same-transaction **op composition**; **transition authorization** and
type guards; protected **approval subjects** over immutable blobs; feeds as **replace/delete replication**; a wake
protocol for all pending work and explicit **automation principals**; number **assignment** to artifacts; an honest
**witness model**; a single supervised Node writer; proofs tied to executed guards; agent **preview**; operable
growth and erasure states. v0.5 fixes runtime mechanics: preview rollback and audit, feed messages keyed by source
identity, pre-arm from static bounds with an armed-time cache, batched audited queries, receipt output withheld only
after a grant change, coalesced numbering, guard signatures, materialized fields as kernel-managed projections.

The central claim, and what the law suite tests: **an owner can evolve, crash, restore, retry, share data and revoke
access without silently changing the meaning of its records or its authority.**

## 1. Purpose, scope and trust boundary

A small, public, proven kernel for apps built as **one system of record with views for each need**, evolved by
agents. An app is the kernel plus modules.

**Scope.** Per-tenant systems of record at small and medium scale, including regulated ones: a medical practice,
a company's accounting, a CRM, a personal health journal. One tenant fits one owner: one consistency boundary,
≤ 10 GB of structured data (documents live in blob storage), write throughput as measured (§15). Owners that work
together (a firm and its clients) connect through shared, read-only feeds (§7.3). Out of scope: workloads whose
consistency boundary exceeds one owner (a bank's core ledger, a hospital-wide EHR, global inventory).

The kernel provides the **foundations** a regulated system of record needs (§17): tamper-evident history, legal
numbering, sealed records, retention, read audit, delegation, archive. It does not provide domain certification or
integrations (health-data hosting, prescription software certification, e-invoicing platforms, card readers); those
remain app work, and the kernel must not prevent them. Kernel conformance is not regulatory readiness.

The kernel guarantees that **approved application code operating through the kernel API** cannot corrupt records,
lose history, or bypass rights. Concretely:
- Database handles, storage bindings, registration and migrations are private to trusted bootstrap code. Modules
  receive only an invocation context (`ctx`).
- The registry is frozen after bootstrap. Duplicate and reserved names (`kernel.*`; `rule:`, `timer:`, `run:`
  intents) are rejected. A module owns the record types under its prefix and may write only those; it changes
  other modules' records only by calling their ops (§5.3).
- Modules export op **descriptors**, never directly callable handlers.
- Import checks in `boring-kernel verify` catch mistakes; they are not a sandbox. Hostile executable code needs
  process isolation from storage credentials, which is outside the kernel.

## 2. Nouns

| Noun | Definition |
| --- | --- |
| **Owner** | The unit of isolation and consistency: one SQLite database with exactly one admitted writer (a Durable Object, or one supervised Node process). A person, a company, a clinic. Everything inside one owner is transactional; nothing is transactional across owners. |
| **Generation** | A random recovery epoch (UUID) created at genesis and at every restore, never a restored counter. `(owner, generation, seq)` identifies an event externally; generations are compared for equality, never ordered. |
| **Commit** | The kernel's envelope for one durable transaction: its event range, access-log range, receipt and kernel control changes, hash-chained. The unit of archive, recovery and feed checkpoints. |
| **Record** | A typed, versioned row owned by a module, identified by `(type, id)` within an owner. |
| **Op** | A typed, named, versioned, synchronous function: the **only** way to change records. Exposed as API, agent tool and MCP tool. |
| **Intent** | One top-level op invocation, identified by `intent_id`; it commits at most once. An optional, uninterpreted `correlation_id` groups intents (a sentence that runs three ops). |
| **Receipt** | The stored outcome of a committed intent, written in its transaction. |
| **Event** | An immutable row appended by an op: a change image of one record plus an optional domain payload. |
| **Projection** | Derived, rebuildable SQL data computed from records (generated views, 1:1 and one-to-many tables; materialized fields are kernel-managed 1:1 projections). Never written directly. Local or cross-owner (§7.3). |
| **Feed** | A declared, versioned, read-only stream an owner publishes: replace/delete messages per source record, checkpointed at commit boundaries. |
| **Share** | A record in the source owner granting one consumer owner the right to read one feed. Revocable, expiring. |
| **Query** | A read: a registered query or agent SQL over the actor's scoped views. Not an op. |
| **Rule** | `when <event> [if <predicate>] then <op>`, versioned, run by the kernel after commit under a declared principal. |
| **Effect** | A durable request for an outside action (email, publish), dispatched outside any transaction. |
| **Policy** | Who may run which op, which transitions they may make, and which records they may see. |
| **Module** | A folder owning record types, ops, queries, projections, rules, data and laws. |

## 3. Storage

### 3.1 Platform (portable by design)

The kernel is **not tied to Cloudflare**. Core code imports no platform API; everything platform-specific sits
behind a small set of ports, and an app moves between hosts by changing its adapter, not its modules.

```ts
interface Platform {
  sql: {
    exec(sql: string, ...bindings: unknown[]): Iterable<Row>   // one statement when bindings are given
    transaction<T>(fn: () => T): T                              // synchronous, all-or-nothing
  }
  wake: { ensureBy(at: string): Promise<void> }   // durable wake no later than `at`; never postpones (§10.3)
  peer: { readFeed(owner, feed, after, limit): Promise<FeedPage> }   // cross-owner reads (§7.3)
  archive: { put(owner, chunkId, bytes): Promise<void> }             // immutable, idempotent by chunk id (§6)
  blobs: { put/get by content hash; delete by GC only }              // immutable files (§9)
  hash(bytes): string                                                // synchronous SHA-256 (§6)
}
```
Two adapters ship, as separate entry points, both **production adapters** tested against the same law suite:

| Port | `@hachej/boring-kernel/cloudflare` | `@hachej/boring-kernel/node` |
| --- | --- | --- |
| Owner | one Durable Object | one local SQLite file, one supervised writer process; no automatic multi-host failover in v1 |
| `sql` | `sql.exec`, `transactionSync` | `node:sqlite` or better-sqlite3 (both synchronous); the adapter sets WAL, synchronous mode and an exclusive lock |
| `wake` | DO alarm | durable timer table polled by the host process |
| `peer` | DO RPC | HTTP between hosts (mixed hosting works) |
| `archive`, `blobs` | R2 | any S3-compatible store (e.g. OVH Object Storage) |
| residency | jurisdiction pinning (e.g. `eu`) | choice of host (e.g. an HDS-certified host for health data) |

Rules that keep the core portable and honest:
- Asynchronous databases (D1, remote SQL) cannot host an owner: the kernel needs synchronous transactions. On
  Cloudflare the owner is always a Durable Object.
- The SQL text is identical on both. Kernel code uses no `BEGIN`/`SAVEPOINT` and no `PRAGMA` (adapters may).
- The kernel is designed to the **stricter limits of both** (§15), so an owner can move hosts without redesign.
- Adapters fail on integers outside the safe range instead of rounding. Only the kernel's own bulk-write templates
  are chunked to respect the 100-binding limit; arbitrary SQL is never split.
- **Node writer**: restart or migration stops (or physically fences) the old writer before admitting the new one.
  A lease in the hub or object storage cannot make two database copies safe to write; v1 does not try.
- Conformance covers behavior, not signatures: durability, wake liveness across crash boundaries, restore and
  writer fencing, on each adapter.

### 3.2 Kernel tables
```sql
CREATE TABLE records (
  type TEXT NOT NULL, id TEXT NOT NULL,
  v INTEGER NOT NULL,            -- schema version of data as written (may lag; never rewritten by backfills)
  rev INTEGER NOT NULL,          -- number of change events for this record
  data TEXT NOT NULL,            -- JSON, valid for (type, v)
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL, deleted_at TEXT,
  sealed_at TEXT, sealed_rev INTEGER, sealed_hash TEXT,   -- latched seal (§4)
  retain_until TEXT,                                      -- persisted retention lower bound (§4)
  PRIMARY KEY (type, id)
);
CREATE TABLE events (
  seq INTEGER PRIMARY KEY,       -- allocated by the kernel: last + 1, plain INSERT (§6)
  commit_id INTEGER NOT NULL,
  at TEXT NOT NULL,              -- commit time, not business time
  actor TEXT NOT NULL, on_behalf_of TEXT, purpose TEXT,
  intent_id TEXT NOT NULL, correlation_id TEXT,
  op TEXT NOT NULL, op_version INTEGER NOT NULL, event TEXT NOT NULL,
  type TEXT NOT NULL, id TEXT NOT NULL, rev INTEGER NOT NULL,
  parent_type TEXT, parent_id TEXT,                    -- child records (§8), for timelines
  image TEXT NOT NULL, image_v INTEGER NOT NULL,       -- full record envelope after the change (incl. seal, retention, tombstone)
  payload TEXT, payload_v INTEGER,                     -- optional domain payload, versioned on its own
  cause_seq INTEGER, depth INTEGER NOT NULL,           -- rule causation (§10)
  UNIQUE (type, id, rev)
);
CREATE INDEX events_parent ON events (parent_type, parent_id, seq) WHERE parent_id IS NOT NULL;
CREATE TABLE access_log (        -- disclosures of audited types (§8)
  n INTEGER PRIMARY KEY, commit_id INTEGER NOT NULL, at TEXT NOT NULL,
  actor TEXT NOT NULL, on_behalf_of TEXT, purpose TEXT, break_glass INTEGER NOT NULL,
  action TEXT NOT NULL,          -- 'read' | 'query' | 'aggregate' | 'receipt' | 'preview' | 'blob_grant' | 'feed'
  type TEXT NOT NULL, subjects TEXT NOT NULL, scope TEXT
);
CREATE TABLE receipts (
  intent_id TEXT PRIMARY KEY, commit_id INTEGER NOT NULL,
  actor TEXT NOT NULL, on_behalf_of TEXT, purpose TEXT,
  op TEXT NOT NULL, op_version INTEGER NOT NULL,
  input TEXT,                    -- canonical JSON under the op version's schema; NULL once compacted
  input_hash TEXT NOT NULL,
  output TEXT, output_v INTEGER, -- NULL once compacted
  grant_rev INTEGER NOT NULL,    -- revision of the actor's membership/grants at commit
  subjects TEXT,                 -- audited subjects disclosed by the output (logged again on replay)
  at TEXT NOT NULL
);
CREATE TABLE commits (
  commit_id INTEGER PRIMARY KEY,
  generation TEXT NOT NULL, at TEXT NOT NULL, bundle_v INTEGER NOT NULL,
  event_first INTEGER, event_last INTEGER,
  access_first INTEGER, access_last INTEGER,
  receipt_id TEXT,
  control TEXT NOT NULL,         -- JSON: kernel control changes (rule cursors, restore parent, compactions)
  hash TEXT NOT NULL             -- H(previous hash, canonical bundle): the one tamper-evident chain (§6)
);
CREATE TABLE jobs (              -- scheduler work (§10.3); rebuildable
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,            -- 'timer' | 'rule' | 'effect' | 'backfill' | 'feed' | 'archive' | 'maintenance'
  run_at TEXT NOT NULL,
  status TEXT NOT NULL,          -- 'pending' | 'parked' | 'done'
  attempts INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL            -- JSON: cursor, checkpoint, progress or last error
);
CREATE INDEX jobs_runnable ON jobs (run_at) WHERE status = 'pending';
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
```

**State classes** (what recovery must carry, and what it may rebuild):

| Class | Contents | On restore |
| --- | --- | --- |
| **Authoritative** | `records`, `events`, `receipts`, `access_log`, `commits` (incl. control: rule cursors) | carried by commit bundles |
| **Rebuildable** | projections, remote-projection slices, `jobs`, `meta` versions and `kernel.seq` | rebuilt from authoritative state and code; a slice restored without its rows is never marked ready |
| **Host-local** | archive cursor, adapter caches, parse caches | reset |

All business state is records: numbering series and assignments (`kernel.counter`), timers (`kernel.timer`), shares,
approvals, holds and effects are kernel-owned record types.

### 3.3 Migrations
- **Startup migrations**: kernel table changes, short, versioned, checksummed, one transaction. Unsafe startup DDL
  (anything proportional to data size) is refused rather than retried until it times out.
- **Backfills** are row work only: populating a projection or materialized fields. They never rewrite `records`.
  Resumable, bounded batches run by the scheduler (§10.3), progress in `jobs`, published atomically when complete.
  Each batch reads the **current** source rows and writes their shadow rows in the same transaction, so a live
  update between batches is never overwritten by a stale image. Temporary storage headroom is checked first.
- **DDL is not resumable.** New projection tables and their indexes are created empty, then populated. An index on
  a large existing kernel table needs a measured maintenance plan; `ANALYZE` likewise (§7.1).
- Code refuses to write to storage whose writer version it does not support (fail closed on rollback).
- Index names include a hash of their definition; a changed definition is a new index, the old one is dropped after
  the new one is published.
- State after eviction is reconstructed from `meta`, never from in-memory "initialized" flags.

## 4. Records

```ts
const Entry = defineRecord({
  type: "ledger.entry",
  version: 2,
  schema: z.object({ ... }),                                  // current version, JSON-safe Zod subset
  history: { 1: { schema: EntryV1, to2: { rename: { piece: "pieceRef" } } } },   // frozen
  indexes: [["date"], ["journal", "date"]],
  unique: [["journal", "number"]],                            // business keys, owner-wide
  sealedWhen: (e) => e.status === "validated",                // protected; latched by the kernel
  guard: (before, after, read) => isBalanced(after.lines) ? null : "unbalanced",   // protected, every mutation
  retention: { years: 10, from: "date" },                     // protected; persisted as a lower bound
  audit: { subject: "patientId" },                            // optional: reads are logged by subject (§8)
})
```
- **Normalization**: a read applies the steps from the stored `v` to the current version, then parses. Each step is
  total on its declared source schema; normalizing a current-version record is the identity (K7). A record that
  fails is reported, never silently dropped.
- **Steps are declarative first**: `rename`, `default` (applies to a **missing** field, never to an explicit `null`),
  `remove`. Declarative steps compile to version-dispatched SQL in the generated view, e.g.
  `CASE v WHEN 1 THEN json_extract(data,'$.piece') ELSE json_extract(data,'$.pieceRef') END`, with
  `json_type(...) IS NULL` distinguishing missing from null. No unconditional `COALESCE` across names.
- **Function steps** are allowed for the rest. Their fields are **materialized** in a kernel-managed 1:1 projection
  (`__mat_<type>_v<version>`, keyed by id) with the same backfill and publish machinery as any projection (§7.1),
  and read from there by the generated view; until the backfill is published the query parser rejects them with
  `column_backfilling`, so K11 holds at every instant. `ctx.get` normalizes in TypeScript and is unaffected.
- Historical schemas and steps are frozen. **Every** change to a persistence contract is a new version, including
  optional additions and widening; compatible changes use an identity step.
- Ordinary writes store the current version. Nothing rewrites stored records outside an op.
- **Guards**: an optional protected `guard(before, after, read)` (`before: T | null`, `after: T`) runs on every
  create and update of the type, whichever op writes it. Tombstones are governed by seals, retention and transition
  authorization (§8), so a guard never sees a null `after`. It holds the costly invariants (a posted entry balances, an import key is unique); presentation
  validation does not belong there. Laws attach to guards (§12).
- **Unique keys** are partial unique indexes over the version-dispatched expression (so a rename needs no rewrite),
  `WHERE type = '<type>' AND deleted_at IS NULL`. They hold across the whole type, independently of the caller's
  read scope; a violation is `duplicate`.
- **Seals are latched**: when `sealedWhen` first holds after a write, the kernel stores `sealed_at`, `sealed_rev`
  and `sealed_hash` (hash of the canonical record envelope) in the same transaction. A sealed record is never
  changed or tombstoned again, and no later code (a changed predicate, a migration) can unseal it or rewrite its
  bytes. Corrections are new records (a reversing entry, an amendment linked to the original). Two named reads:
  `ctx.get(..., { as: "sealed" })` returns the stored bytes as signed; the default returns them normalized to
  today's schema.
- **Retention** resolves to a persisted `retain_until` on the record when it is written; it never decreases,
  whatever later code or data changes. Legal holds are separate, audited `kernel.hold` records. The owner's
  maximum is kept in `meta` for `owner.destroy` (§16).
- **Deletion** is a tombstone (`deleted_at`), a change event like any other. Resurrection increments `rev`; an
  identity is never reset to revision 1. Tombstoning is not erasure (§16).
- `at`/`updated_at` are commit times. Business time (occurrence date, timezone, posting date) and source identity
  (device export item, bank line id) are module fields; uniqueness uses declared `unique` keys.

## 5. Ops

```ts
const validate = defineOp({
  name: "ledger.entry.validate",
  version: 1,
  input: z.object({ id: z.string(), expectedRev: z.number().int() }),
  output: z.object({ id: z.string(), rev: z.number().int() }),
  policy: "ledger.validate",                      // §8
  writes: ["ledger.entry"],                       // capabilities, checked by ctx
  calls: ["effects.request"],                     // same-transaction calls (§5.3)
  emits: ["ledger.entry.validated"],              // undeclared emissions are rejected
  run(ctx, input) {                               // synchronous, inside the transaction
    const e = ctx.get(Entry, input.id, { expectedRev: input.expectedRev })
    ctx.require(e.status === "draft", "not_draft")
    return ctx.put(Entry, { ...e, status: "validated" }, { event: "ledger.entry.validated" })
  },                                              // balance is checked by Entry.guard on every mutation
})
```

### 5.1 Invocation (one transaction)
1. Validate the request envelope: `intent_id`, `op`, `op_version`, `expectedGeneration`, delegation and purpose.
   A `generation` mismatch never executes: with a surviving receipt it returns that receipt (step 2), otherwise
   `generation_changed`.
2. Look up the receipt. Found: it matches only on actor, `on_behalf_of`, `purpose`, op, `op_version` and the input
   canonicalized under **that op version's** frozen schema (retired versions stay registered for lookup, without a
   handler). A match returns `committed` with the stored output, logging its audited `subjects` again, as long as
   the actor's membership and grants are unchanged since the receipt (`grant_rev`); if they changed, it returns
   `committed` with `output_withheld`. Neither the action nor the records it touched are re-authorized: the op may
   have legitimately closed the period, reassigned the record or demoted its caller, or used `elevatedReads`. A compacted receipt returns `committed` with
   `output_expired`. A mismatch is `intent_conflict`. Nothing is executed.
3. Otherwise authorize (§8), validate input, run `run(ctx, input)`, validate and serialize output, write events,
   receipt and commit envelope, commit. Successful no-ops also write a receipt.
4. **Abort is sticky**: any failed `require`, guard, kernel call or constraint error marks the invocation aborted,
   even if module code catches it. It rolls back and returns the first error code. An aborted attempt may be
   evaluated again on retry: K4 is "commits at most once", not "evaluates once".
5. `ctx` is valid only during `run`: a token revoked in `finally`. A `run` returning a thenable is an error.
6. Ops never call the network, the LLM or the clock: `ctx.now`, `ctx.actor`, `ctx.intentId` are given.

### 5.2 Records within an op
- `ctx.get` returns the normalized record within the actor's scope (§8) and records its revision in an internal,
  immutable read set. `expectedRev` compares against the revision the caller originally read: editing and review ops
  take it as input, so a stale form is rejected instead of overwriting a newer edit.
- `ctx.find(Type, where, { limit, orderBy })` and `ctx.query(registeredQuery, params)` read by indexed fields in the
  same transaction, under the same scope, read-set and audit rules as `ctx.get`. `where` is a typed equality/range
  descriptor over declared indexes, not SQL.
- `ctx.put` writes **at most once per record per invocation tree** (a second write is rejected immediately),
  authorizes the transition and runs the type's guard (§8, §4), increments `rev` and appends one event with the full
  change image. A later `ctx.get` returns the written state. `ctx.create` fails if the identity exists.
- **Legal numbering**: `ctx.assignNumber(series, ref, field)` reserves the next number of a series (invoices
  `2026-F`, journal `VT`) for that exact target `(ref, field)`. Several assignments from one series in an invocation
  tree are allocated in call order and flushed as **one** write of that series' `kernel.counter` record at the end,
  after checking that each target's final image carries its number; an unused or duplicated assignment aborts. Voided documents keep their number. Series are gapless in commit order (K16);
  business-date rules (no number dated before the previous one) remain module guards.
- **Timers**: `ctx.schedule(at, op, input)` creates a `kernel.timer` record with its `runAs` principal (§10.1); an
  op that schedules declares `kernel.timer` in its `writes`.

### 5.3 Composition
- `ctx.call(opDescriptor, input)` runs a declared (`calls`) same-owner op **inside the current transaction**, with
  no new receipt or intent: the root receipt covers the tree. This is how a publication op requests its effect,
  or an import creates tasks, without writing other modules' types.
- The callee runs with its own `writes` and is authorized on its own policy for the same principal and delegation;
  the caller's `elevatedReads` are not inherited and capabilities are never unioned.
- Sticky abort, budgets and one-write-per-record apply to the whole tree. The call graph is acyclic and bounded;
  rule analysis (§10.1) uses transitive `emits`.

### 5.4 Budgets and large work
Per invocation tree: ≤ 250 record writes, ≤ 1 MB of serialized images, ≤ 5,000 projection rows (defaults,
configurable). Larger business operations are **not** made atomic by chunking. Modules use explicit staging and
publication: stage an unpublished entry, or atomically publish a canonical-contact alias and rewrite references
afterwards.

### 5.5 Numbers
Amounts are integer minor units in `number` (`z.number().int().safe()`): `MAX_SAFE_INTEGER` cents is about 90
trillion, far beyond any owner in scope. Pure functions that sum use kernel `checkedAdd` (fails beyond the safe
range), never bare `+` over unbounded arrays. In SQL, money totals use `SUM` over `INTEGER` (exact, errors on 64-bit
overflow), never `TOTAL` or `AVG`; adapters reject results beyond the safe range. Pure and SQL paths share the
same boundary, cancellation and overflow test vectors. Proofs state the bounds the code checks.

### 5.6 Preview
An op may be run as a **preview**: the same handler, data and permission checks in a transaction that the kernel
wrapper always rolls back after `run` returns and `ctx` is revoked, so module code can never catch the rollback. It
returns the scoped change summary, the revisions it depended on and the approval subject (§8). It
consumes nothing (no number, receipt, timer or effect) and reserves nothing; execution rechecks everything.
Audited subjects read during a preview (including a preview that aborts on a later `require` or guard) are
collected in memory and committed in a separate audit transaction before the summary or error is released; if that
commit fails, the preview fails.

## 6. Events, commits and the archive

- Events are appended only by the kernel during ops, never updated or deleted. `seq` is allocated with plain inserts
  (last + 1); any failure aborts the invocation, so `seq` is gapless and continues across restores, with
  `generation` distinguishing lineages. Never `INSERT OR IGNORE`.
- One event per record change. The **image** is the kernel-generated record envelope after the change (data, `v`,
  `rev`, timestamps, `deleted_at`, seal, `retain_until`). The **payload** is optional domain information.
- **Images are normalized on every read.** An image is stored at `image_v` and never rewritten. Replay,
  point-in-time state, rule predicates and feed `map` receive `image.data` normalized to the current version with
  the frozen steps (§4); payloads likewise from `payload_v`.
- **Commits**: every durable transaction (an op tree, an audited read, a rule step, a scheduler step) writes one
  commit envelope referencing its event range, access-log range, receipt and kernel control changes. The commit
  hash chains `H(previous hash, canonical bundle)` over all of them: one chain covers records, receipts, audits and
  cursors.
- **Replay** applies commit bundles in order: record images, receipts, access-log rows, control changes. It never
  executes ops, policies, rules or effects. Equality is on normalized logical records and authoritative state.
- **Timeline** of a record = its events, plus events of child records whose `parent_type`/`parent_id` name it
  (indexed). Child events do not increment the parent's `rev`.
- **Point-in-time state** at watermark `W` = latest image per `(type, id)` with `seq ≤ W`; it pages by `seq`:
  ```sql
  SELECT * FROM events e WHERE e.seq > :after AND e.seq <= :W
    AND NOT EXISTS (SELECT 1 FROM events n WHERE n.type = e.type AND n.id = e.id AND n.rev = e.rev + 1 AND n.seq <= :W)
  ORDER BY e.seq LIMIT :limit
  ```
- **Archive**: complete commit bundles are shipped through `platform.archive` in immutable chunks named
  `<generation>/<first_commit>-<last_commit>`, encrypted with a per-owner key and a nonce derived from the chunk id,
  so a retry produces identical bytes and acknowledgement is idempotent. The scheduler flushes every 500 commits or
  10 s and immediately before export, handover or planned shutdown. Bundles from genesis are the v1 recovery
  baseline; checkpoints only accelerate. Recovery point from the archive alone = one flush interval.
- **Witness model** (what the chain does and does not prove): a hash chain detects modification relative to a
  trusted earlier head. Heads are witnessed in stores administered separately from the runtime: the runtime's
  credentials can append archive chunks but cannot change retention or delete; the hub records heads under
  different administration. Retention controlled by the same administrator is not protection against that
  administrator. `kernel.status` exposes the committed, archived and witnessed heads separately. Hashes attest to
  recorded bytes, not to the truth of their contents. Qualified timestamps over witnessed heads are an optional
  deployment feature, enabled when a jurisdiction or use case requires them.

## 7. Projections and queries

### 7.1 Projections
- **Generated views**: one `CREATE VIEW "<type>"` per record type, typed columns, version-dispatched (§4),
  joining materialized fields for function steps.
- **One-to-many projections** for volume, e.g. entry → ledger lines:
  ```ts
  defineProjection({ name: "ledger.line", version: 1, from: Entry,
    rows: (entry) => entry.lines.map((l, i) => ({ entryId: entry.id, n: i, ...l })),   // pure, deterministic
    key: ["entryId", "n"], columns: { ... }, indexes: [["account", "date"]] })
  ```
  On every change of the source record the kernel replaces its rows in the same transaction. A new projection
  version is a new empty table with its indexes, populated by a backfill (§3.3) while the kernel maintains both
  versions, then published atomically.
- `ANALYZE` runs after publication and bulk loads (spike: annual trial balance 326 ms → 26 ms), and otherwise as a
  measured maintenance job; it is one statement, not resumable, so its cost is measured per owner size first.
- Projections are disposable: dropping and rebuilding one from records gives the same rows (K11).

### 7.2 Queries
- Queries are a separate execution mode: they share schemas and authorization, never the receipt or event path.
- **Batches**: the queries of one screen run as one `queryBatch` in one transaction, with one snapshot and at most
  one deduplicated audit commit (one access-log row per type and subject set), not one commit per query.
- **Standard registered queries** ship for the common reads: record search (a portable token projection;
  FTS5 optional where capability-tested on the pinned build), timeline, relations (children, references) and exact
  reports. Modules add their own. They are the path for latency-critical reads.
- **Agent SQL** is the flexible escape hatch: parsed by a real parser into an exact fail-closed subset: `SELECT`
  (incl. CTEs, subqueries, compound selects) over allowlisted views and projections only, allowlisted scalar
  functions, no table-valued functions, no direct access to kernel tables. Every view reference is rewritten to the
  actor's scoped subquery **before** joins, aggregates and pagination. Parse caches are keyed by schema and policy
  generation.
- Results are read in bounded pages, synchronously, buffered; nothing is held across `await`.
- Limits: rows, bytes and structure (joins, nesting). There is **no per-statement timeout** the kernel can rely on:
  agent SQL keeps a whole-owner CPU risk, which is accepted and monitored, not denied.

### 7.3 Cross-owner projections (feeds)

The read path across owners: an accounting firm over its client companies, a group of practices, a coach over
clients. Eventually consistent by design; never a cross-owner transaction.

```ts
// source side (client company): what it can expose, and nothing more (protected)
defineFeed({ name: "ledger.summary", version: 1, from: [Entry, Period],
  map: (record) => record.type === "ledger.entry"                     // pure, deterministic; null = not visible
    ? { kind: "entry", id: record.id, date: record.data.date, journal: record.data.journal, status: record.data.status }
    : { kind: "period", id: record.id, closed: record.data.closed } })

// consumer side (firm): rows kept in the firm owner, one slice per source owner
defineRemoteProjection({ name: "firm.clientLedger", version: 1, feed: "ledger.summary@1",
  rows: (item) => [item], key: ["kind", "id"], columns: { ... }, indexes: [["status"]] })
```
- **Share**: the source grants a feed to a consumer owner with `kernel.share.grant` (policy-controlled,
  approval-capable, audited), with an expiry. The feed exposes only `map` output, never raw images.
- **Messages are replacements keyed by source identity**: `sourceKey` is always the source record's `(type, id)`.
  For each source record changed in the page window, the source maps only its **latest** image there and sends
  `replace(sourceKey, sourceRev, item)`, or `delete(sourceKey, sourceRev)` if it is tombstoned or `map` returns
  `null`. Every remote projection table carries hidden `source_owner, source_type, source_id, source_rev` columns,
  indexed; applying a message deletes all rows of that source record, then inserts `rows(item)`. A changed output
  key or a record that stops matching never leaves stale rows, and the source never needs the previous image.
- **Pages** carry source owner, generation, feed and version, grant id and revision, expiry, a checkpoint at a
  **complete commit** and the next scan cursor. Pagination never splits a source commit; empty pages still advance
  across unrelated commits.
- **Transport is pull** through `platform.peer`. The source checks the share and logs the disclosure (`feed`) in its
  access log. A poke after commit is a hint; pull is the truth.
- **Apply**: the consumer applies a page's replacements and advances its checkpoint in **one transaction**.
- **Bootstrap and rebuild**: a snapshot token `(generation, W)` pins point-in-time state at `W` (§6), paged
  resumably and closed by a completion manifest (counts); then tailing after `W`. The shadow slice replaces the old
  one atomically. During a harmless projection rebuild the old slice stays visible; on a source generation change
  or a narrowing of rights it is **hidden** until revalidated.
- **Revocation and expiry**: when observed, the slice is disabled atomically (hidden from every query); physical
  deletion, including shadow copies, follows in bounded batches. Expiry is checked on every read of the slice, not
  only when a job runs. A page fetched under an older grant revision can never reactivate a slice. Bytes already
  delivered to a consumer cannot be recalled from it.
- **Staleness is visible**: every slice exposes its source watermark and last successful sync. Decisions that must
  be correct (post an entry, close a period) run as ops **in the source owner** (a firm actor with `onBehalfOf`),
  never on the copy.
- **Scope on the consumer side** is the consumer's own policy (which firm staff see which clients).
- **Fan-out**: one checkpoint per (source, feed), a `feed` job; bounded pages round-robin. The consumer holds copies,
  so it is bounded by the same 10 GB: feeds are summaries, not full ledgers.

## 8. Authorization

- **Actor binding**: the worker authenticates (the hub) and passes a verified principal to the owner; the owner
  binds it to its own membership. `actor` never comes from op input.
- `authorize` is local and synchronous, evaluated inside the transaction against current owner state. Deny by
  default. v1 ships a role table adapter; a Cedar adapter is optional.
- **Transitions, not just types**: write capabilities (`writes`) are necessary, never sufficient. Every mutation is
  authorized on `(actor, action, before, after)`: may this actor make *this* change (reassign, reparent, change an
  ACL field). Reparenting needs rights on both parents.
- **Resource scope** for reads: each type's policy declares `(actor) → { sql: predicate, check: (record) =>
  boolean }`, generated from one descriptor. `ctx.get`, `ctx.find`, timelines, file references and queries apply it.
- **Child records** (notes, tasks, files) declare `allowedParents` (root types only). The parent must exist in the
  same owner and be visible to the actor; `parent_type`/`parent_id` go on the event. The child's scope is its own
  predicate **and** an `EXISTS` over its parents' scoped views, so reassigning a parent moves its children with it.
- Ops read under the caller's scope. An op that needs wider reads declares `elevatedReads` in protected
  registration, with a reviewed **disclosure contract** (what of the elevated data may appear in its output) and
  negative tests. Registering a query never widens scope.
- Field-level scope is not in v1: sensitive fields go in a separate record type.
- **Delegation**: an invocation carries `actor` and optionally `onBehalfOf` (a secretary for a doctor, a firm for a
  client) and `purpose`; policies decide on both, and events, receipts and the access log record both.
- **Read audit**: audited types declare a **subject** key (e.g. the patient). Record reads, timelines, receipt
  outputs and previews log their subjects; queries that project subjects log them; aggregates and other queries log
  at query level (text, parameters, scope, types). Whether an app allows such queries on audited types is the app's
  policy decision. The audit row **commits before** protected data is released; if it cannot commit, the read
  fails. Issuing a blob download grant is logged as `blob_grant`: it proves access was granted, not that the blob
  was read.
- **Break-glass**: a policy may allow access outside scope only with a stated justification; such reads and writes
  are flagged in the log and raise an alert to the owner's administrators.
- **Approvals**: an op may declare `approval: "<policy>"` and a protected `approvalSubject(input, read)` in its
  registration; callers never choose what is bound. A human with that policy runs `kernel.approval.grant`, which
  stores the subject: owner and generation, op and version, canonical input, record revisions, immutable blob
  hashes, approver, expiry. Execution recomputes the subject, requires an exact match with an unexpired, unconsumed
  approval, and consumes it in the same transaction. For a redaction, the subject covers the source bytes, the
  mask/proposal revision and the output bytes: what the reviewer approved is what is published. The confirmation UI
  stays outside the kernel; a caller-supplied `confirmed: true` means nothing.

## 9. Standard modules

Optional modules following the same contract as app modules: **notes**, **tasks**, **files**, **approvals**,
**holds**, **effects**. Child records point to one root parent among their `allowedParents` and inherit its scope.
- **Files** are immutable and content-addressed: bytes are uploaded and their hash verified before an op finalizes
  the file record (key, size, type, hash). Bytes at a key are never overwritten. Unreferenced uploads are collected
  after a grace period. Sensitive content stays in blobs; events carry references.
- **Publication** is modeled as approved artifact → `publication.requested` (creates the effect, §10.2) →
  `publication.confirmed` (recorded from the effect's completion). Creating an effect is not delivery. Whether a
  renderer really removed hidden text is a module invariant, not a kernel guarantee.

## 10. Rules, effects, scheduler

### 10.1 Rules and automation principals
```ts
defineRule({ id: "ledger.bigBankEntry", version: 1, when: "ledger.entry.validated",
  runAs: "rule:ledger.bigBankEntry",                       // a principal with its own protected grants
  if: (event, read) => bankAmount(event) > 10_000,         // registered pure predicate
  then: { op: "tasks.create", input: (e) => ({ ... }) } })
```
- **Automation has explicit authority.** A rule runs as its declared service principal, whose grants are protected
  policy. A timer runs as its `runAs` (the creator, with delegation), persisted on the `kernel.timer` record and
  **re-authorized at execution**: a reminder created by a former employee does not carry that employee's rights; it
  parks as `principal_revoked`.
- The graph event → op → transitive `emits` is built from declarations at registration, atomically; cycles are
  refused.
- Per `(rule id, version, event)`: the predicate, the derived op (intent `rule:<id>@<version>:<generation>:<seq>`)
  and the cursor advance commit in **one transaction** (the cursor is commit control state, so restore carries it).
  A skip advances the cursor the same way.
- Derived events carry `cause_seq` and `depth = cause.depth + 1`; the maximum depth is enforced from stored values.
- Failure: the derived op rolls back; a separate commit records attempts and the error. After N attempts the event
  parks (a `jobs` row) and is exposed through `kernel.rule.retry`, `.skip`, `.disable`. Only later events with the
  same **order key** wait; the key defaults to the event's `(type, id)`, and a rule may declare
  `orderKey: (event) => string`. Nothing is skipped silently.
- A new rule version starts at the current head by default; replaying history through it is an explicit
  administrative cutover at a stated watermark. Old receipts are never reused for different behavior.

### 10.2 Effects
An op (usually through `ctx.call("effects.request", ...)`) creates an effect record: `kind`, `deliveryId`, payload,
status `pending | attempting | delivered | failed | uncertain | cancelled`. The dispatcher runs outside any
transaction; each attempt gets an attempt token, and completion (`effects.complete`) is revision-checked against
that token, so a late failure from attempt A cannot overwrite success from attempt B. A timeout is `uncertain`, not
`failed`, and goes to reconciliation. The guarantee is **at-least-once delivery plus recipient idempotency where
available** (`deliveryId` as the key), not exactly-once.

### 10.3 Scheduler and wake protocol
One kernel scheduler runs timers, rule drains, effect dispatch, feed pulls, archive flushes, backfills and
maintenance from `jobs`; the owner's single wake (`platform.wake`) is kept at or before `min(run_at)`.
- **Pre-arm**: before any transaction that can create pending work, the adapter awaits `wake.ensureBy(t)`, with
  `t` a **static bound** from the op's declarations, so no dynamic branch of `run` can need an earlier wake: `now` if
  its transitive `emits` match a rule, or it writes `kernel.timer` or calls `effects.request`; otherwise
  `now + archiveInterval` (every write creates archive lag). Only then does the synchronous transaction run. The
  adapter keeps the durably armed time (read from platform storage at startup) and resolves in memory, with no I/O,
  whenever `armedAt <= t`, so ordinary writes rarely touch the alarm.
  Pre-arm and commit go through the same owner admission path, so no other change interleaves. An earlier known
  wake is never postponed. Empty wakes are harmless.
- **Drain**: a wake first ensures a safety wake for the remaining work, then runs bounded steps. On startup every job
  kind is reconciled against authoritative state (cursor behind head, pending timers and effects, archive lag).
- Wakes are at-least-once and platform retries are limited (six for DO alarms), so retries and backoff are kernel
  state. Every arm/commit/re-arm crash boundary is tested with no later user request.

## 11. Kernel laws

Property tests run against **both** adapters with injected failures, duplicate and no-op calls, mixed-version
histories, restores and projection checks. Platform tests on each adapter (real Durable Objects; a real Node host)
cover wakes, concurrent callers, eviction or process restart, writer fencing and recovery.

| ID | Law |
| --- | --- |
| K1 | An invocation tree's record writes, events, projection rows, receipt and commit envelope commit together or not at all, even when module code catches errors. |
| K2 | `seq` is gapless and increasing along a lineage; `(owner, generation, seq)` is never reused for different content. |
| K3 | A record's `rev` equals the number of its change events, which have consecutive revisions. |
| K4 | An `intent_id` commits at most once. A retry returns `committed` (output, withheld or expired) or `intent_conflict`; a stale-generation request without a receipt never executes. |
| K5 | Events, commits and access-log rows are never modified or deleted; receipts are only compacted to markers, never removed. |
| K6 | Every stored record and image normalizes and parses with the current schema. |
| K7 | Each normalization step is total on its source schema; normalization at the current version is the identity. |
| K8 | Queries never write records, events or receipts (only audit commits), and return only rows within the actor's scope over allowlisted views. |
| K9 | The declared rule and call graphs are acyclic; undeclared emissions are rejected; stored depth is bounded. |
| K10 | Replaying commit bundles into empty storage restores all authoritative state; rebuildable state rebuilt from it is equal. |
| K11 | Generated views and projections (incl. materialized fields) equal the normalized records: values, filters, ordering, aggregates. |
| K12 | `ctx.get`, `ctx.find`, timelines, queries and file references never expose a record outside the actor's scope; elevated outputs stay within their reviewed disclosure contract (negative tests). |
| K13 | Each `(rule, version, event)` is decided at most once, together with its cursor advance. |
| K14 | The commit hash chain verifies from genesis or any witnessed head to the current head. |
| K15 | A sealed record is latched: never changed, tombstoned, unsealed or byte-rewritten afterwards. |
| K16 | Every assigned number is carried by exactly one committed record; each series is gapless in commit order. |
| K17 | Every disclosure of an audited type is preceded by its committed audit row (per subject, or per query for aggregates). |
| K18 | A remote slice equals `rows(map(...))` of the source's latest images at a complete source commit, keyed by source identity; never half a commit, never rows from a superseded image. |
| K19 | A feed page is served only under an unexpired, unrevoked share revision, and contains only `map` output. |
| K20 | After revocation or expiry is observed, no consumer query returns rows from that source; older pages cannot reactivate it. |
| K21 | Declared unique keys hold over all non-deleted records of their type. |
| K22 | Every reader of an event image sees it normalized to the current version. |
| K23 | A record's `retain_until` never decreases; nothing under retention or hold is destroyed. |
| K24 | Every mutation passes transition authorization on `(before, after)` and its type's guard. |
| K25 | An approval-gated op commits only with an approval whose recomputed subject matches exactly, consumed in the same transaction. |
| K26 | Every pending job has a wake armed at or before its `run_at`, across every crash boundary. |
| K27 | A preview commits nothing but its audit, and its audit commits before its result is released. |

Evidence by kind of law: **ordering laws** (K4 receipts, K13 rule delivery, K18–K20 feed apply and revocation, K26
wake, K2 recovery epochs) are TLA+ models checked by TLC, each with mutants that must fail, as in boring-app's
`formal/`; **pure functions** (normalization steps, sequence/revision allocation, DAG check, module guards) are
proven with LemmaScript (Dafny backend first, pinned toolchain, regenerated artifacts checked); the rest is
property-tested on both adapters. The kernel ships a `VERIFY.json` mapping each law to its evidence or to an
explicit deferral (statuses evidence / partial / driven / deferred); anything not proven is labelled
**property-tested, not proven**, and a deferral is never a pass.

## 12. Modules

```
modules/<name>/
  schema.ts       record types (defineRecord), incl. guards
  ops.ts          ops (defineOp)
  queries.ts      registered queries and projections (optional)
  feeds.ts        feeds (optional, protected)
  rules.ts        rules (optional)
  read.ts         read-only contract other modules may import (optional)
  LAWS.ts         the module's laws, as LemmaScript annotations on pure functions (human-owned)
  test/           model-based properties (fast-check)
  data/           starter packs, templates, prompts
  journeys/       feature journeys, human-owned (§14.3)
```
Screens live in the app's page project, grouped by module, each with its manifest (§14.3); see
`docs/boring-app.md` for the Boring app template.

`boring-kernel verify` enforces:
1. The layout above; `LAWS.ts` is mandatory (may be empty only with an approved waiver).
2. Modules import no storage, adapter or registration API; writes go through ops (mistake check, not a sandbox).
3. A module imports another module only through its op descriptors or its `read.ts`.
4. Every op has a policy, declared `writes`, `calls` and `emits`, and at least one test.
5. **Proofs bind to what executes**: every claimed proof verifies the pinned guard or transition the kernel actually
   runs, and the verified declaration set is pinned (a passing verifier over the wrong set fails). Textual call-site
   checks are lint only. Each critical law has a seeded mutation (ignore the predicate, change its input, overwrite
   its result) that the guards or independent transition tests reject.
6. No unapproved `assume`, `extern`, axiom, strengthened precondition or skipped obligation.
7. The generated client (§14.2) is up to date with op and query descriptors.
8. Every screen has a manifest; it uses only declared queries and ops; every journey step names existing ones.

## 13. Evolution gate

Run on every change, before merge, using the **base branch's** gate (candidate code, protected gate).
1. **Schema compatibility**, per record type, classified on four axes: old data readable by new code, new data
   readable by old code, writer compatibility, query contract. Every changed persistence contract gets a new version
   (compatible changes use an identity step); narrowing, refinement changes and opaque Zod (transforms, custom
   refinements) need explicit review: JSON Schema is not a semantic digest.
2. **Normalization (K7), replay (K10) and parity (K11)** over a corpus of real, anonymised commits and records.
3. **Laws**, seeded mutations and **module properties** pass.
4. **Boundaries** (§12) hold.
5. **Journeys** (§14.3) pass on both drivers and both adapters; the **role matrix** passes for every screen.
6. **Accessibility and smoke** checks pass for every screen. Visual review is advisory.

The **trusted gate boundary** requires human approval on any change, whatever else the change contains:
`LAWS.ts`, `journeys/`, gate configuration, verifier and its dependencies, the replay corpus, waivers, policies and
role mappings, protected registration (`approval`, `approvalSubject`, `elevatedReads` and disclosure contracts,
guards, `sealedWhen`, `retention`, feeds, `calls`, rule principals), and test deletions. Replacing the gate itself is
a separately approved change.

## 14. Clients, agents and features end to end

Strictness follows cost, layer by layer: **proven** (pure laws, guards) → **property-tested** (kernel and module
properties) → **contract-checked** (typed client, screen manifests, role matrix) → **journey-tested** (API and
browser) → **reviewed** (visual polish, advisory). The kernel stays small; the gate spans the whole app.

### 14.1 Agent surface
- Every op exports its JSON Schema (from Zod), description and version; an app exposes it unchanged as an agent tool
  and an MCP tool. Discovery also shows, under current policy: required revisions, budgets, approval requirements
  and typed errors. Sensitivity and approval come from protected registration, not from tool descriptions.
- Agents can **preview** an op (§5.6) before executing it, to see conflicts, dependencies and the approval subject
  without consuming anything.
- Agents read through the standard and module registered queries first, agent SQL second, and write only through
  ops, with the same actor binding, scope and approvals as humans.

### 14.2 Client contract (`@hachej/boring-kernel/client`, headless)
Framework-agnostic, no components. Generated from op and query descriptors, so a screen calling an op with the wrong
input does not compile.
- **Ops**: `client.op("ledger.entry.validate", input)` creates the `intent_id` once and reuses it on every retry
  (network loss, reload while pending), and always sends `op_version` and `expectedGeneration` (learned from every
  response). An unknown outcome is resolved by retrying the same intent.
- **Edits**: `client.edit(record)` carries the `rev` it loaded as `expectedRev`; a stale save surfaces as
  `stale_record` with the newer version, never a silent overwrite.
- **Queries and live updates**: `client.subscribe(query, params)` returns results with the owner's `seq` watermark.
  After each commit that advances `seq` (never after audit-only or control-only commits, which would loop) the owner
  notifies subscribers (WebSocket on both platforms) with `seq` and the types changed;
  the client re-runs affected queries through the normal scoped path, so notifications never carry data outside
  scope. Remote projections also expose their source watermark and last sync.
- **Screens** run their subscriptions as one `queryBatch` (§7.2) on load and after each notification.
- **Errors and outcomes** are typed and shared by every app: `committed` (with `output`, `output_withheld` or
  `output_expired`), `not_found`, `forbidden`, `stale_record`, `intent_conflict`, `generation_changed`, `duplicate`,
  `sealed`, `approval_required`, `column_backfilling`, `budget_exceeded`, `write_limited`, module `require` and
  guard codes.
- **Approvals**: `approval_required` carries the approval subject; the client resumes the same intent once granted.
- **Delegation and purpose** (`onBehalfOf`, `purpose`, break-glass justification) are explicit call options.

React bindings (`@hachej/boring-kernel/react`) are thin hooks on TanStack Query: `queryFn` runs the scoped query,
subscriptions invalidate affected keys, and mutations call `client.op` with the intent created once per `mutate()`,
so TanStack retries reuse it. UI components are not in the kernel: the default app stack (React, Vite, shadcn,
react-hook-form with the Zod resolver) belongs to the app template, and an optional `@hachej/boring-ui` may generate
forms from schemas, tables from views and record timelines.

### 14.3 Features end to end
A feature is a vertical slice: schema, ops, guards, laws, queries, screens and journeys, in one module change.
- **Journeys** (`journeys/*.ts`, human-owned) state the feature as steps over ops, queries and screens, with
  actors and expected outcomes, including denials:
  ```ts
  journey("validate an entry", ({ as }) => {
    as("accountant").open("ledger.entry", draft).do("ledger.entry.validate").expect({ status: "validated" })
    as("assistant").open("ledger.entry", draft).expectAction("ledger.entry.validate").hidden()
    as("accountant").do("ledger.entry.update", { id: draft }).expectError("sealed")
  })
  ```
  The same journey runs on two drivers: **API** (kernel client, fast, every adapter) and **browser** (real screens,
  Playwright). Humans approve journeys and laws (the *what*); agents build the rest (the *how*).
- **Screen manifest**: each screen declares the queries it subscribes to and the ops it can trigger. Verify rejects
  screens that read or write anything else (e.g. calling the network directly).
- **Role matrix**: the gate renders every screen as every role on seeded data and checks that no out-of-scope record
  appears and no forbidden op is offered. It is the screen-level counterpart of K12.
- **Seeds**: journeys and the role matrix run on module starter data (`data/`) plus generated records, on both
  adapters. Restore journeys (crash, restore, retry, revoke) are part of every regulated module set.

## 15. Operational limits

- Design limits (the stricter of both platforms): one owner is single-threaded, 30 s CPU per request, 10 GB
  storage, 2 MB per row/string, 100 bindings per statement.
- Throughput: the spike measured ~1–2k simple writes/s; that is a measurement, not a guarantee. Supported sizes are
  published only after benchmarking sustained latency with commits, hashing, audit, feeds and archive enabled.
- Exports read bounded pages synchronously; nothing is held across `await` (Durable Object cursors have no snapshot
  across awaits). History exports page commits up to a watermark captured at an op boundary; state exports use
  point-in-time state (§6). A completed export has a manifest: generation, watermark, schema versions, counts, chunk
  checksums. A partial stream is never a backup.
- The query parser and rewrite are the read-only guarantee on every platform (Durable Objects have no
  `query_only`, so the kernel never relies on it).

## 16. Lifecycle

- **Status**: `kernel.status` reports schema and projection readiness, storage use and reserve, committed /
  archived / witnessed heads, archive lag, oldest pending and parked work, feed expiries and the last successful
  restore drill. These are actionable facts, not logs.
- **Growth** is a state machine: `normal` → `write-limited` (early warning passed: new business writes refused with
  `write_limited`) → `recovery-only`. A reserve is kept for completing already-accepted work, audit commits,
  revocations and lifecycle control; only kernel-owned maintenance routes are admitted into it (no module flag
  grants it). Tombstones are not assumed to reclaim space. If a required audit cannot commit, the protected read
  fails closed. Splitting an owner is a redesign of its consistency boundary, not an operational remedy.
- **Receipts** are compacted after a documented window (default 30 days): input and output are dropped, the marker
  (`intent_id`, identity fields, `input_hash`, `commit_id`) stays, so an old retry returns `output_expired` and
  never re-executes. Business deduplication across sources uses declared unique keys (§4).
- **Recovery** (privileged, not an op): pause admission and the scheduler; restore a verified prefix of commit
  bundles (from the host's backups or the archive; it must work without the lost database); create a fresh
  generation whose first commit records its parent (generation, commit, hash); rebuild rebuildable state;
  **reconcile authority before reopening** (memberships, shares, holds, numbering against external evidence);
  reconcile external consumers and effects (an epoch cannot unsend an email); resume. Requests carrying the old
  generation never execute without a surviving receipt.
- **Erasure**: `owner.destroy` is a privileged lifecycle operation, an explicit exception to K5/K10. It is refused
  while anything is under retention or hold. Order: first persist, **outside the owner** (the hub), an idempotent
  destruction job and a deny-restore marker; then delete owner storage. The external job owns blob deletion, archive
  key destruction (crypto-shredding the archive) and completion tracking; bootstrap and restore check the marker
  before serving data. Host backup expiry is documented (30 days for Cloudflare PITR). Per-record erasure is not in
  v1.
- **Handover**: closing a practice or changing accountant is an export of the owner (state at a watermark, full
  commit history, access log, blob manifest, witnessed heads) in a documented format, readable without the kernel.
  Domain exports (accounting FEC, health-data standards) are module work built on it.

## 17. Fit for regulated systems of record

What the kernel gives, and what stays app work. "App" items are often most of a commercial product's effort.

| Need | Kernel | App / module |
| --- | --- | --- |
| Validated entries cannot change; corrections by reversal | latched seals (K15), history (K5) | accounting rules, reversal ops |
| Chronological, gapless invoice and journal numbering | `assignNumber` (K16) | series naming, date rules per legislation |
| Period closing, balanced journals | guards, transactional cross-record checks, laws (K24) | the checks themselves, proofs |
| Legal exports (FEC, e-invoicing formats) | point-in-time state, archive | format, certified platform connection |
| Amend-not-overwrite medical record, author and time | latched seals, events with actor/delegation | clinical record model |
| Who consulted a patient file | subject audit committed before disclosure (K17), break-glass | review screens, query policy |
| Retention (e.g. 10 years accounting, 20 years medical) | persisted lower bounds, holds, archive (K23) | per-type durations |
| Proof that history was not altered | commit chain, separately administered witnesses (K14); qualified timestamps optional | certification evidence |
| What was approved is what is published | approval subjects over immutable blobs (K25) | the renderer's own correctness |
| Certified hosting, residency | portable adapters, jurisdiction pinning | choice of host, certification |
| Professional identity (health professional cards, SSO) | verified actor, delegation | the hub, identity providers |
| Patient / contact search by name | standard search query (token projection; FTS5 optional) | tokenizer, ranking, phonetic matching |
| Firm view across many client companies | feeds, shares, remote projections (§7.3) | which summaries to share, firm dashboards |
| Restore after a disaster without changing meaning | commit bundles, fresh generation, authority reconciliation (K10, K2, K4) | restore drills |
| A feature is correct end to end | client contract, journeys, role matrix (§14) | journeys, screens |
| Billing integrations, card readers, prescription software certification | — | entirely app |

## 18. Non-goals for v1

Cross-owner transactions (sagas in apps), cross-owner live queries (use remote projections, §7.3), tenants larger
than one owner, automatic multi-host failover of Node owners, per-record erasure, field-level authorization,
exactly-once external effects, a message broker, a general saga or workflow engine, a general rule expression
language, a state-machine library, mandatory Cedar, UI components in the kernel (optional `boring-ui`), auth and
identity (the hub), file storage itself (behind `platform.blobs`), vector search, offline-first clients,
collaborative editing (CRDT inside a record later).

## 19. Decisions and open questions

Decided:
- LemmaScript with Dafny first; fallback is fast-check, labelled as such; losing a proof needs a waiver.
- Policies: role table in v1, Cedar optional (Cedar/WASM on workerd is untested).
- Local projections (§7.1) and cross-owner projections (§7.3) are both core v1.
- Package `@hachej/boring-kernel` with `/node`, `/cloudflare`, `/client` and `/react` entry points;
  `boring-kernel verify` is a dev CLI.
- Receipts: compacted to markers after 30 days, never deleted; old retries never re-execute (§16).
- Money: safe integers in minor units with `checkedAdd`; SQL totals with integer `SUM`; shared test vectors (§5.5).
  `bigint` was considered and rejected for v1: the bounds are checked on both paths and cover every owner in scope.
- New rule versions start at head; replaying history is an explicit administrative cutover (§10.1).
- Approvals: generic `kernel.approval` with a protected `approvalSubject` over immutable blobs (§8, §9).
- First regulated target: **accounting** (closing, multi-currency, import and restore journeys), with firm feeds;
  medical second. Fitness, redaction and CRM stay as small adversarial acceptance modules.
- Platforms: both adapters supported from v1; the law suite and journeys run on both.
- Schemas: **Zod 4** across the kernel and apps, as boring-app and boring-ui already use; agent tools receive the
  op's JSON Schema (`z.toJSONSchema`), which pi-durable validates before the kernel validates again.
- Agent runtime: pi-durable (through `@hachej/boring-agent`); ops are the app's operations, exposed as its tools
  (`docs/boring-app.md`).
- Witnessing: separately administered witnesses (runtime can append, not delete or change retention; the hub
  records heads). Qualified timestamps are an optional deployment feature, decided per jurisdiction and use case.
- Agent SQL over audited types: allowed or forbidden by app policy; the kernel guarantees the audit commits first.
- Search: standard search query on a portable token projection; FTS5 optional where capability-tested.
- Node writer: one supervised local writer per owner; no distributed lease or automatic failover in v1. Failover
  later requires real storage-level fencing.

Open: none at this time.
