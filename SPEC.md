# boring-kernel — specification (draft v0.3)

Status: draft v0.3 for review, 2026-10-14. Evidence: boring-factory `docs/research/spike-ledger.md` (double-entry ledger on
Node and a real Durable Object). Principles: **simple, robust, elegant**. Strict only where a mistake is costly or
invisible (data, rights, history); loose elsewhere, because agents repair.

v0.2 answers the review of PR #1 at `c1483d9`. Main changes: intent = one op invocation with a stored receipt;
events carry kernel-generated change images (replay never runs ops); typed storage becomes rebuildable projections;
resource-scoped reads; approvals enforced at the op boundary; rule delivery committed with its cursor; an honest
trust boundary; lifecycle (recovery, growth, erasure) made explicit.

v0.3 answers the second review: events are normalized whenever read; ops get indexed reads and declared unique
keys; counters and timers are records, so replay restores them; a `jobs` table replaces queues in `meta`; child
scope is one level deep; receipts expire after a retry window; rules park per record; feed bootstrap paging and
archive chunking are specified; decisions taken on the open questions.

## 1. Purpose, scope and trust boundary

A small, public, proven kernel for apps built as **one system of record with views for each need**, evolved by
agents. An app is the kernel plus modules.

**Scope.** Per-tenant systems of record at small and medium scale, including regulated ones: a medical practice,
a company's accounting, a CRM, a personal health journal. One tenant fits one owner: ≤ ~1–2k writes/s, ≤ 10 GB of
structured data (documents live in blob storage), one consistency boundary. Owners that work together (a firm
and its clients) connect through shared, read-only feeds (§7.3). Out of scope: workloads whose
consistency boundary exceeds one owner (a bank's core ledger, a hospital-wide EHR, global inventory).

The kernel provides the **foundations** a regulated system of record needs (§17): tamper-evident history, legal
numbering, sealed records, retention, read audit, delegation, archive. It does not provide domain certification
or integrations (health-data hosting, prescription software certification, e-invoicing platforms, card readers);
those remain app work, and the kernel must not prevent them.

The kernel guarantees that **approved application code operating through the kernel API** cannot corrupt records,
lose history, or bypass rights. Concretely:
- Database handles, storage bindings, registration and migrations are private to trusted bootstrap code. Modules
  receive only an invocation context (`ctx`).
- The registry is frozen after bootstrap. Duplicate and reserved names (`kernel.*`, `rule:*` intents) are rejected.
  A module owns the record types under its prefix and may write only those.
- Modules export op **descriptors**, never directly callable handlers.
- Import checks in `boring-kernel verify` catch mistakes; they are not a sandbox. Hostile executable code needs
  process isolation from storage credentials, which is outside the kernel.

## 2. Nouns

| Noun | Definition |
| --- | --- |
| **Owner** | The unit of isolation and consistency: one SQLite database with a single writer (a Durable Object, or a Node process holding the owner's lease). A person, a company, a clinic. Everything inside one owner is transactional; nothing is transactional across owners. |
| **Generation** | A counter in `meta`, bumped by recovery (§16). `(generation, seq)` identifies an event externally. |
| **Record** | A typed, versioned row owned by a module, identified by `(type, id)` within an owner. |
| **Op** | A typed, named, synchronous function: the **only** way to change records. Exposed as API, agent tool and MCP tool. |
| **Intent** | One top-level op invocation, identified by `intent_id`. Retrying it within the receipt window never executes twice. An optional, uninterpreted `correlation_id` groups intents (a sentence that runs three ops). |
| **Receipt** | The stored outcome of an intent, written in the op's transaction. |
| **Event** | An immutable row appended by an op: a change image of one record plus an optional domain payload. |
| **Projection** | Derived, rebuildable SQL data computed from records (generated views, one-to-many tables). Never written directly. Local (same owner) or cross-owner (§7.3). |
| **Feed** | A declared, versioned, read-only stream an owner publishes: a pure mapping of selected change images, ordered by `(generation, seq)`. |
| **Share** | A record in the source owner granting one consumer owner the right to read one feed. Revocable. |
| **Query** | A read: a registered query or agent SQL over the actor's scoped views. Not an op. |
| **Rule** | `when <event> [if <predicate>] then <op>`, versioned, run by the kernel after commit. |
| **Effect** | A durable request for an outside action (email, publish), dispatched outside any transaction. |
| **Policy** | Who may run which op, and which records an actor may see. |
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
  wake: { armAt(at: string): void }          // one durable wake-up per owner (§10.3)
  peer: { readFeed(owner, feed, after, limit): Promise<FeedPage> }   // cross-owner reads (§7.3)
  archive: { append(owner, chunk): Promise<void> }                   // append-only, encrypted (§6)
  blobs: { put/get/delete by key }                                   // files (§9)
  hash(bytes): string                         // synchronous SHA-256 (§6)
}
```
Two adapters ship, as separate entry points, both **production adapters** tested against the same law suite:

| Port | `@hachej/boring-kernel/cloudflare` | `@hachej/boring-kernel/node` |
| --- | --- | --- |
| Owner | one Durable Object | one SQLite file, one writer process holding a lease |
| `sql` | `sql.exec`, `transactionSync` | `node:sqlite`; the adapter sets WAL and synchronous mode |
| `wake` | DO alarm | durable timer table polled by the host process |
| `peer` | DO RPC | HTTP between hosts (mixed hosting works) |
| `archive`, `blobs` | R2 (object lock) | any S3-compatible store (e.g. OVH Object Storage) |
| residency | jurisdiction pinning (e.g. `eu`) | choice of host (e.g. an HDS-certified host for health data) |

Rules that keep the core portable:
- The SQL text is identical on both. Kernel code uses no `BEGIN`/`SAVEPOINT` and no `PRAGMA` (adapters may).
- The kernel is designed to the **stricter limits of both** (§15), so an owner can move hosts without redesign.
- Adapters fail on integers outside the safe range instead of rounding, and split statements to respect the
  100-binding limit.
- A third adapter (e.g. a different SQLite host) needs only these ports and must pass the law suite (§11).

### 3.2 Kernel tables
```sql
CREATE TABLE records (
  type TEXT NOT NULL, id TEXT NOT NULL,
  v INTEGER NOT NULL,            -- schema version of data (physical; may lag the current version)
  rev INTEGER NOT NULL,          -- number of change events for this record
  data TEXT NOT NULL,            -- JSON, valid for (type, v)
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL, deleted_at TEXT,
  PRIMARY KEY (type, id)
);
CREATE TABLE events (
  seq INTEGER PRIMARY KEY,       -- allocated by the kernel: last + 1, plain INSERT (§6)
  generation INTEGER NOT NULL,
  at TEXT NOT NULL,              -- commit time, not business time
  actor TEXT NOT NULL, intent_id TEXT NOT NULL, correlation_id TEXT,
  op TEXT NOT NULL, event TEXT NOT NULL,
  type TEXT NOT NULL, id TEXT NOT NULL, rev INTEGER NOT NULL,
  parent_type TEXT, parent_id TEXT,                    -- set for child records (§8), for timelines
  image TEXT NOT NULL, image_v INTEGER NOT NULL,       -- full record envelope after the change, incl. tombstone
  payload TEXT, payload_v INTEGER,                     -- optional domain payload, versioned on its own
  cause_seq INTEGER, depth INTEGER NOT NULL,           -- rule causation (§10)
  on_behalf_of TEXT, purpose TEXT,                     -- delegation (§8)
  hash TEXT NOT NULL,            -- H(previous hash, canonical event): tamper-evident chain (§6)
  UNIQUE (type, id, rev)
);
CREATE INDEX events_parent ON events (parent_type, parent_id, seq) WHERE parent_id IS NOT NULL;
CREATE TABLE access_log (        -- reads of types marked auditReads (§8); append-only, hash-chained
  n INTEGER PRIMARY KEY, generation INTEGER NOT NULL, at TEXT NOT NULL,
  actor TEXT NOT NULL, on_behalf_of TEXT, purpose TEXT, break_glass INTEGER NOT NULL,
  action TEXT NOT NULL, type TEXT NOT NULL, ids TEXT NOT NULL, hash TEXT NOT NULL
);
CREATE TABLE receipts (
  intent_id TEXT PRIMARY KEY,
  actor TEXT NOT NULL, op TEXT NOT NULL, op_version INTEGER NOT NULL,
  input TEXT NOT NULL,           -- canonical JSON of the validated input
  output TEXT NOT NULL, output_v INTEGER NOT NULL,
  generation INTEGER NOT NULL, at TEXT NOT NULL
);
CREATE INDEX receipts_at ON receipts (at);                -- expiry (§16)
CREATE TABLE jobs (              -- scheduler work (§10.3); derived from records and cursors, rebuildable
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
`meta` holds scalars only: `kernel.schema`, `kernel.generation`, `kernel.seq`, head hashes, archive cursor,
`retain_until`, supported reader/writer ranges, module and projection versions, migration checksums. Anything with
many items (timers, rule cursors and parked events, backfills, feed checkpoints) is a row in `jobs`.

Business state lives **only** in `records` (and therefore in events): numbering series (`kernel.counter`), timers
(`kernel.timer`), shares, approvals and effects are kernel-owned record types. `jobs` and projections are derived:
replay rebuilds them (K10).

### 3.3 Migrations
- **Startup migrations**: kernel table changes, short, versioned, checksummed, one transaction.
- **Backfills**: anything proportional to data size (rewriting records, building a projection or index). Resumable,
  run in bounded batches by the scheduler (§10.3), progress persisted in `jobs`, published atomically when complete.
  Temporary storage headroom is checked before starting.
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
  sealedWhen: (e) => e.status === "validated",                // optional; enforced by the kernel
  retention: { years: 10, from: "date" },                     // optional legal retention
  auditReads: false,                                          // true: every read is logged (§8)
  unique: [["journal", "number"]],                            // declared business keys
})
```
- **Sealed records**: once `sealedWhen` holds on the stored record, the kernel rejects any further `put` or
  tombstone. Corrections are new records (a reversing entry, an amendment note linked to the original). This is
  the accounting "validated entries are intangible" rule and the medical "amend, never overwrite" rule, enforced
  once, in the kernel, instead of in each op.
- **Retention**: a record under retention cannot be destroyed before its date, including by `owner.destroy`
  (§16); a legal hold extends it. The kernel maintains the owner's `retain_until` (the maximum over all writes,
  never decreasing) in `meta`, so the check needs no scan.
- **Unique keys**: each `unique` entry becomes a partial unique index on
  `records(json_extract(data, ...)) WHERE type = '<type>' AND deleted_at IS NULL`. A unique field cannot be renamed
  by a declarative step; that change is a new version with a backfill and an index rebuild. Source identities (bank
  line id, device export item) are declared this way, so a re-import fails with `duplicate` instead of duplicating.
- **Normalization**: a read applies the steps from the stored `v` to the current version, then parses. Each step is
  total on its declared source schema; normalizing a current-version record is the identity (K7). A record that
  fails is reported, never silently dropped.
- **Steps are declarative first**: `rename`, `default` (applies to a **missing** field, never to an explicit `null`),
  `remove`. Declarative steps compile to version-dispatched SQL in the generated view, e.g.
  `CASE v WHEN 1 THEN json_extract(data,'$.piece') ELSE json_extract(data,'$.pieceRef') END`, with
  `json_type(...) IS NULL` distinguishing missing from null. No unconditional `COALESCE` across names.
- **Function steps** are allowed for the rest. Fields they produce are not queryable until a backfill has rewritten
  every record of the type to the new version (§3.3). Until then the query parser rejects references to those
  columns with `column_backfilling`, and projections depending on the new version build in a shadow table and cut
  over when the backfill completes, so K11 holds at every instant. `ctx.get` normalizes in TypeScript and is
  unaffected. Types with ≤ 5,000 records are backfilled inside the startup migration. There is no lazy
  query-time rewrite.
- Historical schemas and steps are frozen. Every change to persisted meaning is a new version.
- Writes always store the current version. A backfill rewrite is physical: no event, `rev` unchanged.
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
  emits: ["ledger.entry.validated"],              // undeclared emissions are rejected
  run(ctx, input) {                               // synchronous, inside the transaction
    const e = ctx.get(Entry, input.id, { expectedRev: input.expectedRev })
    ctx.require(e.status === "draft", "not_draft")
    ctx.require(isBalanced(e.lines), "unbalanced")      // pure function, proven in LAWS.ts
    return ctx.put(Entry, { ...e, status: "validated" }, { event: "ledger.entry.validated" })
  },
})
```

### 5.1 Invocation (one transaction)
1. Look up the receipt for `intent_id`. Found, with the same actor, op, `op_version` and canonical input, and the
   actor still a member of the owner → return the stored output as stored (`output_v`); otherwise
   `intent_conflict`. Nothing is executed. The receipt is not re-authorized against current state: the op may have
   legitimately changed it (closed the period, consumed the approval, demoted its caller), and the output was
   already the caller's.
2. Authorize (§8), validate input, run `run(ctx, input)`, validate and serialize output, write the receipt, commit.
   Every step happens inside one synchronous transaction; successful no-ops also write a receipt.
3. **Abort is sticky**: any failed `require`, failed kernel call or constraint error marks the invocation aborted,
   even if module code catches it. An aborted invocation rolls back and returns the first error code.
4. `ctx` is valid only during `run`: a token revoked in `finally`. A `run` returning a thenable is an error.
5. Ops never call the network, the LLM or the clock: `ctx.now`, `ctx.actor`, `ctx.intentId` are given.

### 5.2 Records within an op
- `ctx.get` returns the normalized record within the actor's scope (§8) and records its revision in an internal,
  immutable read set. `expectedRev` compares against the revision the caller originally read: editing and review ops
  take it as input, so a stale form is rejected instead of overwriting a newer edit.
- `ctx.find(Type, where, { limit, orderBy })` and `ctx.query(registeredQuery, params)` read by indexed fields in the
  same transaction, under the same scope, read-set and audit rules as `ctx.get`. `where` is a typed equality/range
  descriptor over declared indexes, not SQL. This is how an op checks "no draft entry left in period 2026-01".
- `ctx.put` writes **at most once per record per invocation** (a second write is rejected immediately), increments
  `rev` by one and appends one event with the full change image, executed immediately. A later `ctx.get` in the
  same op returns the written state. `ctx.create` fails if the identity exists.
- Cross-record checks (balanced journal, period closed) are reads in the same transaction; single-owner serial
  execution makes them exact.
- **Legal numbering**: `ctx.nextNumber(series)` returns the next number of a named series (invoices `2026-F`,
  journal `VT`), backed by a `kernel.counter` record written once at the end of the invocation however many
  numbers it allocated. The series is gapless and chronological (K16), independent of `seq`, and restored by replay.
- **Timers**: `ctx.schedule(at, op, input)` creates a `kernel.timer` record in the same transaction; the scheduler
  runs the op at or after `at` with intent `timer:<id>` (reminders, deadlines, recurring invoices).

### 5.3 Budgets and large work
Per invocation: ≤ 250 record writes, ≤ 1 MB of serialized images, ≤ 5,000 projection rows (defaults, configurable).
Larger business operations are **not** made atomic by chunking. Modules use explicit staging and publication: stage
an unpublished entry, or atomically publish a canonical-contact alias and rewrite references afterwards.

### 5.4 Numbers
Amounts are integer minor units in `number` (`z.number().int().safe()`): `MAX_SAFE_INTEGER` cents is about 90
trillion, far beyond any owner in scope. Pure functions that sum use kernel `checkedAdd` (fails beyond the safe
range), never bare `+` over unbounded arrays. Proofs state the same bounds the code checks.

## 6. Events and replay

- Appended only by the kernel during ops, never updated or deleted. `seq` is allocated from `meta.kernel.seq` with
  plain inserts; any failure aborts the invocation, so `seq` is gapless within a generation. Never `INSERT OR IGNORE`.
- One event per record change. The **image** is the kernel-generated record envelope after the change (data, `v`,
  `rev`, timestamps, `deleted_at`). The **payload** is optional domain information for rules and humans.
- **Images are normalized on every read.** An image is stored at the version current when it was written
  (`image_v`) and never rewritten (it is hash-chained). Replay, point-in-time state, rule predicates and feed `map`
  all receive `image.data` normalized from `image_v` to the current version with the frozen steps (§4); payloads
  likewise from `payload_v`.
- **Replay** applies images in `seq` order. It never executes ops, policies, rules or effects. Equality is on
  normalized logical records; physical `v` may differ.
- **Timeline** of a record = its events, plus events of child records whose `parent_type`/`parent_id` name it
  (indexed). Child events do not increment the parent's `rev`.
- **Point-in-time state** at watermark `W` = latest image per `(type, id)` with `seq ≤ W`: "what did the record
  say when the doctor signed it" is a query, not a reconstruction. It pages by `seq` with existing indexes:
  ```sql
  SELECT * FROM events e WHERE e.seq > :after AND e.seq <= :W
    AND NOT EXISTS (SELECT 1 FROM events n WHERE n.type = e.type AND n.id = e.id AND n.rev = e.rev + 1 AND n.seq <= :W)
  ORDER BY e.seq LIMIT :limit
  ```
- **Tamper evidence**: each event stores `hash = H(previous hash, canonical event)`, computed synchronously in the
  transaction through `platform.hash`. The head hash is anchored
  outside the owner (the archive, §16) at least on every archive flush, so the operator cannot silently rewrite
  history either. `boring-kernel audit` verifies a chain from any anchor.
- **Archive**: events, the access log and receipts are shipped through `platform.archive` to append-only external
  storage, encrypted with a per-owner key, in immutable chunks `<generation>/<from_seq>-<to_seq>`. The scheduler
  flushes every 500 events or 10 s (whichever first) and immediately before export, handover or planned shutdown;
  the archive cursor lives in `meta`. Each chunk header carries the head hash and the rule cursors at its last
  `seq`. Because events carry full change images and all business state is records, the archive alone rebuilds
  the owner (K10): it is the backup, the long-term retention store and the source for restore beyond any host's
  own backups (Cloudflare PITR covers 30 days). Recovery point from the archive alone = one flush interval.

## 7. Projections and queries

### 7.1 Projections
- **Generated views**: one `CREATE VIEW "<type>"` per record type, typed columns, version-dispatched (§4).
- **One-to-many projections** for volume, e.g. entry → ledger lines:
  ```ts
  defineProjection({ name: "ledger.line", version: 1, from: Entry,
    rows: (entry) => entry.lines.map((l, i) => ({ entryId: entry.id, n: i, ...l })),   // pure, deterministic
    key: ["entryId", "n"], columns: { ... }, indexes: [["account", "date"]] })
  ```
  On every change of the source record the kernel replaces its rows in the same transaction. A new projection
  version is built by a backfill into a new table; during the build the kernel maintains both generations; it is
  published atomically. `ANALYZE` runs after publication and bulk loads, then periodically as a maintenance job
  (spike: annual trial balance 326 ms → 26 ms after `ANALYZE`).
- Projections are disposable: dropping and rebuilding one from records gives the same rows (K11).

### 7.2 Queries
- Queries are a separate execution mode: they share schemas and authorization, never the receipt or event path.
- **Registered queries** (module-defined, parameterized) are the path for latency-critical reads.
- **Agent SQL**: parsed by a real parser into an exact fail-closed subset: `SELECT` (incl. CTEs, subqueries,
  compound selects) over allowlisted views and projections only, allowlisted scalar functions, no table-valued
  functions, no direct access to `records`/`events`/`receipts`/`meta`. Every view reference is rewritten to the
  actor's scoped subquery **before** joins, aggregates and pagination. Parse caches are keyed by schema and policy
  generation.
- **Audited types in agent SQL**: when the result projects record ids of an `auditReads` view, those ids are
  logged. An aggregate or a query that does not project ids is logged at query level (text, parameters, scope,
  types touched). Whether such queries are allowed on audited types is the app's policy decision, not the
  kernel's: the kernel guarantees only that they are logged (K17). The kernel registers no SQL functions:
  Durable Objects do not support application-defined functions, so a per-row audit UDF is not portable.
- Limits: rows, bytes and structure (joins, nesting). There is **no per-statement timeout** the kernel can rely on (none on Durable Objects):
  agent SQL keeps a whole-owner CPU risk, which is accepted and monitored, not denied.

### 7.3 Cross-owner projections (feeds)

The read path across owners: an accounting firm over its client companies, a group of practices, a coach over
clients. Eventually consistent by design; never a cross-owner transaction.

```ts
// source side (client company): what it can expose, and nothing more
defineFeed({ name: "ledger.summary", version: 1, from: [Entry, Period],
  map: (image) => image.type === "ledger.entry"                        // pure, deterministic, versioned
    ? { kind: "entry", id: image.id, date: image.data.date, journal: image.data.journal, status: image.data.status }
    : { kind: "period", id: image.id, closed: image.data.closed } })

// consumer side (firm): rows kept in the firm owner, one slice per source owner
defineRemoteProjection({ name: "firm.clientLedger", version: 1, feed: "ledger.summary@1",
  rows: (item) => [item], key: ["kind", "id"], columns: { ... }, indexes: [["status"]] })
```
- **Share**: the source owner grants a feed to a consumer owner with `kernel.share.grant` (policy-controlled,
  approval-capable, audited). The feed only ever exposes what `map` returns, never raw images, so a share is
  minimal by construction. Revocation is a source-side op.
- **Transport is pull**: the consumer's scheduler calls `readFeed(feed, after: {generation, seq}, limit)` on the
  source through `platform.peer` (§3.1). The
  source checks the share, logs the read in its access log as the consumer owner, maps images, returns a bounded
  page plus its head. After commit the source may poke consumers (an effect) to pull sooner; the poke is a hint,
  pull is the truth.
- **Apply**: the consumer applies a page and advances its checkpoint for that source in **one transaction**, so
  delivery is exactly-once into the projection. Rows are keyed by `(source owner, key)`.
- **Bootstrap and rebuild**: first sync, a feed version change, or a source generation change (restore) rebuilds
  that source's slice into a shadow slice. The consumer captures the source head `W`, pages point-in-time state at
  `W` with the keyset query of §6 (resumable from `after`, no server-side cursor), then tails events after `W`.
  When the shadow slice reaches `W` it replaces the old slice in one transaction; until then the old one is served.
- **Revocation**: the next read returns `share_revoked`; the consumer purges that source's rows in one transaction.
  A consumer that never reads again keeps stale rows: shares therefore carry an expiry the consumer enforces too.
- **Staleness is visible**: every slice exposes its source watermark and last successful sync; queries can filter
  or display on it. Consumer ops may read remote projections for display and routing, but a decision that must be
  correct (post an entry, close a period) runs as an op **in the source owner** (an actor of the firm with
  `onBehalfOf`), never on the copy.
- **Scope on the consumer side** is the consumer's own policy: which firm staff see which clients' rows. A remote
  projection is a record-scoped view like any other (§8).
- **Fan-out**: one checkpoint per (source, feed), a `feed` job; the scheduler round-robins sources with bounded pages, so 500
  clients cost 500 small pulls, not one large one. The consumer holds copies, so it is bounded by the same 10 GB:
  feeds should be summaries, not full ledgers.

## 8. Authorization

- **Actor binding**: the worker authenticates (the hub) and passes a verified principal to the owner; the owner
  binds it to its own membership. `actor` never comes from op input.
- `authorize(actor, action, resource)` is local and synchronous, evaluated inside the transaction against current
  owner state. Deny by default. v1 ships a role table adapter; a Cedar adapter is optional.
- **Resource scope**: each record type's policy declares a scope, `(actor) → { sql: predicate, check: (record) =>
  boolean }`, both generated from one descriptor. `ctx.get`, timelines, file references and queries all apply it.
- **Child records** (notes, tasks, files) declare `allowedParents: [Deal, Contact, ...]`, root types only: a child
  never has a child parent (a file attached to a note points to the note's root and links the note). `ctx.put`
  checks that the parent exists and is visible to the actor, and records `parent_type`/`parent_id` on the event.
  The child's scope is its own predicate **and** an `EXISTS` over the scoped views of its declared parents, so
  reassigning a parent moves its children with it.
- Ops read under the caller's scope. An op that needs wider reads declares `elevatedReads` in protected
  registration; its output schema is then part of the reviewed contract. Registering a query never widens scope.
- Field-level scope is not in v1: sensitive fields go in a separate record type.
- **Delegation**: an invocation carries `actor` and optionally `onBehalfOf` (a secretary for a doctor, an
  accounting firm for a client) and `purpose`; policies decide on both, and events and the access log record both.
- **Read audit**: for types with `auditReads`, every `ctx.get`, query row and timeline read appends to
  `access_log` (actor, delegation, purpose, ids), in the same transaction as the read. Medical systems must show
  who consulted which patient file.
- **Break-glass**: a policy may allow access outside scope only with a stated justification; such reads and writes
  are flagged in the log and raise an alert to the owner's administrators.
- **Approvals**: an op may declare `approval: "<policy>"` in protected registration. A human with that policy runs
  `kernel.approval.grant`, which stores `(op, canonical input, revisions of the records it binds, approver,
  expiry)`. The approved op checks a matching, unexpired, unconsumed approval with unchanged revisions, and consumes
  it in the same transaction. The confirmation UI stays outside the kernel; a caller-supplied `confirmed: true`
  means nothing.

## 9. Standard modules

Optional modules following the same contract as app modules: **notes**, **tasks**, **files** (key, size, type,
hash of blobs stored through `platform.blobs`), **approvals**, **effects**. Child records point to one root
parent among their `allowedParents` and inherit its scope (§8). Sensitive content stays in blobs; events carry references.

## 10. Rules, effects, scheduler

### 10.1 Rules
```ts
defineRule({ id: "ledger.bigBankEntry", version: 1, when: "ledger.entry.validated",
  if: (event, read) => bankAmount(event) > 10_000,         // registered pure predicate
  then: { op: "tasks.create", input: (e) => ({ ... }) } })
```
- The graph event → op → `emits` is built from declarations at registration, atomically; cycles are refused.
- Per `(rule id, version, event)`: the predicate, the derived op (intent `rule:<id>@<version>:<generation>:<seq>`,
  reserved prefix) and the cursor advance commit in **one transaction**. A skip advances the cursor the same way.
- Derived events carry `cause_seq` and `depth = cause.depth + 1`; a maximum depth is enforced from the stored
  value, so it survives eviction.
- Failure: the derived op rolls back; a separate transaction records attempts and the error. After N attempts the
  event is parked (a `jobs` row, `status = 'parked'`) and exposed through `kernel.rule.retry`, `.skip`, `.disable`.
  With the default `order: "per_record"`, the cursor moves on and only later events of the **same** `(type, id)`
  wait behind the parked one; `order: "global"` parks the whole rule. Nothing is skipped silently.
- A new rule version starts at the current head by default; `start: "backlog"` (explicit) replays history through
  it. The previous version drains what it had already accepted. Old receipts are never reused for different
  behavior.

### 10.2 Effects
An op creates an effect record (`kind`, `deliveryId`, payload, status). The dispatcher runs outside any transaction,
calls the platform service with `deliveryId` as the idempotency key, and records the result through
`effects.complete`. Pending and failed effects stay visible for reconciliation. The guarantee is **at-least-once
delivery plus recipient idempotency where available**, not exactly-once.

### 10.3 Scheduler
An owner has one durable wake-up (`platform.wake`), armed at `min(run_at)` of pending `jobs`. One kernel scheduler
runs timers, rule drains, effect dispatch, feed pulls, archive flushes, backfills and maintenance from `jobs`. The adapter arms the wake-up before committing any invocation that may emit
events (empty drains are harmless), and every startup re-arms if any cursor is behind the head. Wake-ups are
at-least-once and platform retries are limited (six for DO alarms), so retries and backoff are kernel state, not platform state. Each
run is bounded in work.

## 11. Kernel laws

Property tests run against **both** adapters with injected failures, duplicate and no-op calls, mixed-version
histories and projection checks. Platform tests on each adapter (real Durable Objects; a real Node host) cover wake-ups, concurrent callers, eviction or
process restart, lease loss and recovery.

| ID | Law |
| --- | --- |
| K1 | An invocation's record writes, events, projection rows and receipt commit together or not at all, even when module code catches errors. |
| K2 | Within a generation, `seq` is gapless and increasing. |
| K3 | A record's `rev` equals the number of its change events, which have consecutive revisions. |
| K4 | Within the receipt window, an `intent_id` executes at most once; a retry returns the stored output; a different actor or input is `intent_conflict`. |
| K5 | Events are never modified or deleted; receipts only expire after the window. |
| K6 | Every stored record normalizes and parses with the current schema. |
| K7 | Each normalization step is total on its source schema; normalization at the current version is the identity. |
| K8 | Queries never write records, events or receipts (only the access log), and return only rows within the actor's scope over allowlisted views. |
| K9 | The declared rule graph is acyclic; undeclared emissions are rejected; stored depth is bounded. |
| K10 | Applying change images in `seq` order to empty storage yields normalized-equal records, including counters, timers, shares and approvals; jobs and projections rebuilt from them are equal. |
| K11 | Generated views and projections equal the normalized records: values, filters, ordering, aggregates. |
| K12 | No op output, query, timeline or file reference exposes a record outside the actor's scope. |
| K13 | Each `(rule, version, event)` is decided at most once, together with its cursor advance. |
| K14 | The event and access-log hash chains verify from genesis (or any anchor) to the head. |
| K15 | A sealed record is never changed or tombstoned again. |
| K16 | Each numbering series is gapless and increasing in commit order. |
| K17 | Every read of an `auditReads` type that returns data has an access-log row (per id, or per query for aggregates). |
| K18 | A remote projection slice equals `rows(map(...))` over the source's state at the slice's checkpoint. |
| K19 | A feed page is served only under an unexpired, unrevoked share, and contains only `map` output. |
| K20 | After a revocation is observed, the consumer holds no rows from that source. |
| K21 | Declared unique keys hold over non-deleted records of their type. |
| K22 | Every reader of an event image sees it normalized to the current version. |

Pure algorithms (normalization steps, sequence/revision allocation, DAG check) and module laws are proven with
LemmaScript (Dafny backend first, pinned toolchain, regenerated artifacts checked). Anything not proven is labelled
**property-tested, not proven**.

## 12. Modules

```
modules/<name>/
  schema.ts       record types (defineRecord)
  ops.ts          ops (defineOp)
  queries.ts      registered queries and projections (optional)
  rules.ts        rules (optional)
  read.ts         read-only contract other modules may import (optional)
  LAWS.ts         the module's laws, as LemmaScript annotations on pure functions (human-owned)
  test/           model-based properties (fast-check) and journeys
  data/           starter packs, templates, prompts
  views/          UI (optional, convention only, outside the kernel)
```
`boring-kernel verify` enforces:
1. The layout above; `LAWS.ts` is mandatory (may be empty only with an approved waiver).
2. Modules import no storage, adapter or registration API; writes go through ops (mistake check, not a sandbox).
3. A module imports another module only through its op descriptors or its `read.ts`.
4. Every op has a policy, declared `writes` and `emits`, and at least one test.
5. Every law verifies; each law names the ops that must call its function, and verify checks they do.
6. No unapproved `assume`, `extern`, axiom, strengthened precondition or skipped obligation.

## 13. Evolution gate

Run on every change, before merge, using the **base branch's** gate (candidate code, protected gate).
1. **Schema compatibility**, per record type, classified on four axes: old data readable by new code, new data
   readable by old code, writer compatibility, query contract. Classes: optional addition, required addition with a
   deterministic default, widening, narrowing, rename, removal, refinement change. Anything but optional addition
   and widening needs a new version and a step; narrowing and refinement changes need explicit review. Opaque Zod
   (transforms, custom refinements) always needs review: JSON Schema is not a semantic digest.
2. **Normalization (K7), replay (K10) and parity (K11)** over a corpus of real, anonymised events and records.
3. **Laws** and **module properties** pass.
4. **Boundaries** (§12) hold.

The **trusted gate boundary** requires human approval on any change, whatever else the change contains:
`LAWS.ts`, gate configuration, verifier and its dependencies, the replay corpus, waivers, policies and role
mappings, protected registration metadata (`approval`, `elevatedReads`), and test deletions. Replacing the gate
itself is a separately approved change.

## 14. Agent surface

- Every op exports its JSON Schema (from Zod) and description; an app exposes it unchanged as an agent tool and an
  MCP tool. Sensitivity and approval requirements come from protected registration, not from tool descriptions.
- Agents read through queries and write only through ops, with the same actor binding, scope and approvals as
  humans.

## 15. Operational limits (from the spike)

- Design limits (the stricter of both platforms): one owner is single-threaded (~1–2k writes/s), 30 s CPU per
  request, 10 GB storage, 2 MB per row/string, 100 bindings per statement.
- Exports read bounded pages synchronously; nothing is held across `await` (Durable Object cursors have no snapshot across
  awaits). History exports keyset-page events up to a watermark `W` captured at an op boundary; state exports use
  point-in-time state at `W` (§6). A completed export has a manifest: generation, `W`, schema versions, counts,
  chunk checksums. A partial stream is never a backup.
- The query parser and rewrite are the read-only guarantee on every platform (Durable Objects have no
  `query_only`, so the kernel never relies on it).

## 16. Lifecycle

- **Growth**: storage is monitored per owner. Above a threshold (default 80%) the owner becomes write-limited
  (only ops marked `reclaim` run) and an alert is raised. Splitting an owner is a redesign of its consistency
  boundary, not an operational remedy.
- **Receipts** expire after a documented retry window (default 30 days) through a bounded maintenance job.
  Business deduplication beyond the window uses declared unique keys (§4), not receipts.
- **Recovery** (privileged, not an op): pause writes and the scheduler, export (incl. receipts and blob manifest),
  restore (from the host's backups or, beyond them, the archive), bump `generation`, rebuild `jobs` and projections, re-import receipts created after the restore
  point (still within the window) as tombstones so their retries return `intent_lost_in_restore`, reconcile external consumers and effects, resume.
  A new generation prevents identity collisions; it does not undo sent emails or restore external blobs.
- **Erasure**: `owner.destroy` is a privileged lifecycle operation, an explicit exception to K5/K10: it deletes the
  owner's storage, schedules blob deletion, destroys the archive key (crypto-shredding the archive), records the
  erasure in the hub so restore never resurrects it, and documents the host's backup expiry (30 days for Cloudflare PITR). It is refused while any
  record is under retention or legal hold. Per-record erasure is not in v1.
- **Handover**: closing a practice or changing accountant is an export of the owner (state at `W`, full history,
  access log, blob manifest, hash anchors) in a documented format, readable without the kernel. Domain exports
  (accounting FEC, health-data standards) are module work built on it.

## 17. Fit for regulated systems of record

What the kernel gives, and what stays app work. "App" items are often most of a commercial product's effort.

| Need | Kernel | App / module |
| --- | --- | --- |
| Validated entries cannot change; corrections by reversal | sealed records (K15), history (K5) | accounting rules, reversal ops |
| Chronological, gapless invoice and journal numbering | `nextNumber` (K16) | series naming per legislation |
| Period closing, balanced journals | transactional cross-record checks, laws | the checks themselves, proofs |
| Legal exports (FEC, e-invoicing formats) | point-in-time state, archive | format, certified platform connection |
| Amend-not-overwrite medical record, author and time | sealed records, events with actor/delegation | clinical record model |
| Who consulted a patient file | read audit (K17), break-glass | review screens |
| Retention (e.g. 10 years accounting, 20 years medical) | retention, archive, refusal to destroy | per-type durations |
| Proof that history was not altered | hash chain, object-locked archive, daily qualified timestamp (K14) | certification evidence |
| Certified hosting, residency | portable adapters, jurisdiction pinning | choice of host, certification |
| Professional identity (health professional cards, SSO) | verified actor, delegation | the hub, identity providers |
| Patient / contact search by name | token projection (normalized, accent-stripped prefixes, B-tree index) | tokenizer, ranking, phonetic matching |
| Firm view across many client companies | feeds, shares, remote projections (§7.3) | which summaries to share, firm dashboards |
| Billing integrations, card readers, prescription software certification | — | entirely app |

## 18. Non-goals for v1

Cross-owner transactions (sagas in apps), cross-owner live queries (use remote projections, §7.3), tenants
larger than one owner, per-record erasure, field-level authorization, exactly-once external effects, a general rule
expression language, a state-machine library, UI components, auth and identity (the hub), file storage itself (behind `platform.blobs`),
vector search, offline-first clients, collaborative editing (CRDT inside a record later).

## 19. Decisions and open questions

Decided:
- LemmaScript with Dafny first; fallback is fast-check, labelled as such; losing a proof needs a waiver.
- Policies: role table in v1, Cedar optional (Cedar/WASM on workerd is untested).
- Local projections (§7.1) and cross-owner projections (§7.3) are both core v1: the accounting firm view is a
  first-class use case, not an app add-on.
- Package `@hachej/boring-kernel` with `/node` and `/cloudflare` entry points; `boring-kernel verify` is a dev CLI.
- Receipts expire after 30 days (§16); restore tombstones follow the same window.
- Money: safe integers in minor units with `checkedAdd` (§5.4); no `bigint` in v1.
- New rule versions start at head; `backlog` is explicit (§10.1).
- Approvals: generic `kernel.approval`. The blob is bound through the revision of its staged file record, which
  carries the blob's SHA-256; no blob logic in approvals.
- First regulated target: **accounting** on Cloudflare (`eu`), with firm feeds. Medical second, on the Node adapter
  at an HDS host.
- Anchoring: object-locked archive (compliance mode) plus, daily, one RFC 3161 timestamp from a qualified TSA over a
  Merkle root of all owners' head hashes, computed by the hub.
- Agent SQL aggregates over audited types: allowed or forbidden by app policy; the kernel only logs them.
- Search: a portable token projection, not FTS5; the kernel depends on no SQLite extension.
- Node lease: held in the hub, with a monotonically increasing epoch. The adapter opens the file with an exclusive
  lock (no second local writer); the holder stops accepting requests a safety margin before lease expiry and the
  next holder waits for expiry; every archive chunk is written conditionally under the epoch, so a stale holder
  fails closed at its next flush.

Open:
1. Rule `per_record` ordering: is `(type, id)` of the triggering event the right key, or should rules declare a
   key (e.g. the correlation or a parent)?
2. Startup backfill threshold (5,000 records): measure on a real Durable Object against the CPU limit.
