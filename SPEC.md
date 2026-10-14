# boring-kernel — specification (draft v0.1)

Status: draft for review, 2026-10-14. Evidence: boring-factory `docs/research/spike-ledger.md` (double-entry ledger on
Node and a real Durable Object). Principles: **simple, robust, elegant**. Strict only where a mistake is costly or
invisible (data, rights, history); loose elsewhere, because agents repair.

## 1. Purpose

A small, public, proven kernel for apps built as **one system of record with views for each need**, evolved by
agents. An app is the kernel plus modules. The kernel guarantees that no module, human or agent can corrupt
records, lose history, or bypass rights.

## 2. Nouns

| Noun | Definition |
| --- | --- |
| **Owner** | The unit of isolation and consistency: one storage instance (one Durable Object). A person, a company, a clinic. Everything inside one owner is transactional; nothing is transactional across owners. |
| **Record** | A typed, versioned row owned by a module, identified by `(type, id)` within an owner. |
| **Op** | A typed, named function: the **only** way to change records. Exposed as API, agent tool and MCP tool. |
| **Event** | An immutable fact appended by an op. The history of the owner. |
| **View** | A read model: a generated SQL view over records, or a query. |
| **Rule** | `when <event> [if <condition>] then <op>`, stored as data, run by the kernel. |
| **Policy** | Who may run which op or read which view. |
| **Intent** | One request (a tap, a sentence, a rule firing) carrying an `intent_id`; it may run several ops. |
| **Module** | A folder owning record types, ops, views, data and laws. |

## 3. Storage

### 3.1 Adapter
```ts
interface Sql {
  exec(sql: string, ...bindings: unknown[]): Iterable<Row>   // one statement when bindings are given
  transaction<T>(fn: () => T): T                              // synchronous, all-or-nothing
}
```
Two adapters ship: `nodeSqlite(db)` (node:sqlite) and `durableObject(ctx.storage)` (`sql.exec`,
`transactionSync`). The SQL text is identical. No SQL `BEGIN`/`SAVEPOINT`, no `PRAGMA` in kernel code (not
available on Durable Objects).

### 3.2 Kernel tables
```sql
CREATE TABLE IF NOT EXISTS records (
  type TEXT NOT NULL, id TEXT NOT NULL,
  v INTEGER NOT NULL,            -- schema version of data
  rev INTEGER NOT NULL,          -- number of events applied to this record
  data TEXT NOT NULL,            -- JSON, valid for (type, v)
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL, deleted_at TEXT,
  PRIMARY KEY (type, id)
);
CREATE TABLE IF NOT EXISTS events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL, actor TEXT NOT NULL, intent_id TEXT NOT NULL,
  op TEXT NOT NULL,              -- op that wrote it (ledger.entry.reverse)
  event TEXT NOT NULL,           -- event name rules match on (ledger.entry.reversed, ledger.entry.created)
  type TEXT NOT NULL, id TEXT NOT NULL,
  rev INTEGER NOT NULL, v INTEGER NOT NULL, data TEXT NOT NULL,   -- event payload
  UNIQUE (intent_id, op, type, id)
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
```
`meta` holds `kernel.schema` (kernel table version), rule cursors and module versions. Kernel table changes are
versioned SQL migrations run at startup, inside one transaction.

### 3.3 Typed storage (escape hatch)
A record type may declare `storage: "typed"`: the kernel also maintains a typed table (real columns, indexes)
written by the same op transaction, and generated views read from it. Use for volume (> ~10k rows queried together).
Ops, events and laws do not change.

## 4. Records

```ts
const Entry = defineRecord({
  type: "ledger.entry",
  version: 2,
  schema: z.object({ ... }),                    // current version
  upcast: { 1: (d) => ({ ...d, pieceRef: d.piece }) },   // v1 → v2, pure
  storage: "json" | "typed",
  indexes: [["date"], ["journal", "date"]],
})
```
- Upcasters are **declarative first**: `{ rename: { piece: "pieceRef" }, default: { currency: "EUR" } }` covers most
  changes and is visible to the SQL views generator (a view reads `COALESCE(new, old, default)` across stored
  versions). A function upcaster is allowed for the rest; the view for that field then reads the typed table or is
  refreshed by a lazy rewrite before the new version is relied on in queries.
- Reads parse `data` with the record's schema after applying upcasters from the stored `v`. A record that fails to
  parse is reported, never silently dropped.
- Writes always store the current version (lazy migration).
- Indexes are `CREATE INDEX IF NOT EXISTS` on `json_extract` expressions (or typed columns), created at startup.
- Deletion is soft (`deleted_at`) and emits an event.

## 5. Ops

```ts
const validate = defineOp({
  name: "ledger.entry.validate",
  input: z.object({ id: z.string() }),
  output: z.object({ id: z.string(), rev: z.number() }),
  policy: "ledger.validate",                      // §8
  run(ctx, input) {                               // synchronous, inside the transaction
    const e = ctx.get(Entry, input.id)
    ctx.require(e.status === "draft", "not_draft")
    ctx.require(isBalanced(e.lines), "unbalanced")      // pure function, proven in LAWS.ts
    return ctx.put(Entry, { ...e, status: "validated" }, { event: "ledger.entry.validated" })
  },
})
```
Semantics (each is a kernel law, §11):
- **One op call = one transaction.** Either every record write and every event lands, or nothing does.
- Input and output are validated with Zod; a failed `require` aborts with a typed error code, writing nothing.
- `ctx.put` checks the record's `rev` (optimistic, the value read in this op) and increments it by one per event.
- **Idempotency:** an op replayed with the same `intent_id` and target returns the stored result and writes nothing.
- Ops never call the network, the LLM or the clock directly: `ctx.now`, `ctx.actor`, `ctx.intentId` are given.
  Side effects outside the owner go through rules or a platform service, never inside the transaction.
- Batching: an op writing many records must stay under a size budget (default 250 record writes); larger work is
  split into several ops or run by a rule.

## 6. Events

- Appended only by ops, never updated or deleted. `seq` is strictly increasing and gapless per owner.
- Event payloads are versioned like records and upcast on read.
- **Timeline** of a record = its events ordered by `seq`. Kernel-provided ops add notes and tasks to any record's
  timeline (§9).
- Events are the outbox: rules and projections read them after a cursor stored in `meta`.

## 7. Views and queries

- For every record type the kernel generates `CREATE VIEW "<type>"` with typed columns from the schema (or reads
  the typed table).
- `kernel.query` is a read-only op: input SQL is parsed (real parser, not regex); only `SELECT` over allowlisted
  views; no functions outside an allowlist; row and time limits. This is the agent's read path.
- Cross-owner reads are not a kernel concern in v1: apps use fan-out ops or projections fed by events.

## 8. Policies

- Every op and every view declares a policy name. The kernel calls an injected `authorize(actor, action, resource)`
  before running an op or a query; deny by default.
- v1 ships a simple role table adapter; a Cedar adapter (WASM) is optional. Policies are data, owned by humans.

## 9. Kernel features every record gets

- **Timeline** (events), **notes** (`kernel.note.add`), **tasks** (`kernel.task.*` attached to any record),
  **files** (references to blobs stored outside, e.g. R2; the kernel stores the key, size, type, hash).
- Each is a kernel module following the same contract as app modules.

## 10. Rules

```ts
defineRule({ name: "ledger.bigBankEntry", when: "ledger.entry.validated",
  if: (event, read) => bankAmount(event) > 10_000, then: { op: "kernel.task.create", input: (e) => ({ ... }) } })
```
- Run after commit, from the events cursor, never inside the triggering transaction.
- The derived op uses `intent_id = <rule name>:<event seq>`, so re-running a rule is idempotent.
- At registration the kernel builds the graph event → op → events and **refuses cycles**; it also enforces a
  maximum chain depth at run time.
- On a Durable Object, rules are driven by an alarm; each run is bounded in work (CPU limit).

## 11. Kernel laws (proven or property-tested in the kernel repo)

| ID | Law | How |
| --- | --- | --- |
| K1 | An op's writes and events commit together or not at all. | property test with injected failures |
| K2 | `seq` is gapless and increasing. | property test |
| K3 | A record's `rev` equals the count of its events. | property test |
| K4 | An op replayed with the same `intent_id` writes nothing new. | property test |
| K5 | Events are never modified or deleted. | API (no op can), property test |
| K6 | Every stored record parses with its schema after upcasting. | property test, replay |
| K7 | Upcasters are total and idempotent (`upcast(upcast(x)) = upcast(x)` at the target version). | LemmaScript where pure, fast-check otherwise |
| K8 | `kernel.query` never writes and never reads outside allowlisted views. | fuzzing, SQLite bytecode check |
| K9 | The rule graph is acyclic. | check at registration |
| K10 | Replaying all events into empty storage rebuilds the records. | property test |

## 12. Modules

```
modules/<name>/
  schema.ts   record types (defineRecord)
  ops.ts      ops (defineOp); the only code that writes
  views/      components and default screens (optional, UI is outside the kernel)
  data/       starter packs, templates, prompts
  rules.ts    rules (optional)
  LAWS.ts     the module's laws, as LemmaScript annotations on pure functions (human-owned)
  test/       model-based properties (fast-check) and journeys
```
`boring-kernel verify` enforces:
1. The layout above; `LAWS.ts` is mandatory (may be empty only with an explicit waiver).
2. Only `ops.ts` imports the kernel write API; nothing else writes.
3. A module imports another module only through its ops.
4. Every op has a policy and at least one test.
5. Every law in `LAWS.ts` verifies (LemmaScript), and every module property passes.

## 13. Evolution gate

Run on every change to a module, before merge:
1. **Schema diff** (Zod → JSON Schema, old vs new) classified as: additive (allowed), changed or renamed (requires a
   new version and an upcaster), removed (requires a deprecation marker first).
2. **Upcaster properties** (K7) and **event replay**: a corpus of real, anonymised events and records must parse and
   upcast with the new code.
3. **Laws** (LemmaScript) and **module properties** (fast-check, model-based) pass.
4. **Boundaries** (§12) hold.
`LAWS.ts`, the gate configuration and the replay corpus are human-owned; agents may not edit them in the same change
as the code they constrain.

## 14. Agent surface

- Every op exports its JSON Schema (from Zod) and description, so an app exposes it unchanged as an agent tool and
  an MCP tool.
- Agents read through `kernel.query` and write only through ops; writes the app marks as sensitive require a
  confirmation step outside the kernel.

## 15. Operational limits (from the spike)

- Run `ANALYZE` after bulk loads; the planner needs it to pick covering indexes.
- Keep transactions small (≤ 250 record writes); split long work across requests or alarms (30 s CPU limit).
- Stream large exports through a cursor, never one aggregated string.
- Durable Objects have no `query_only`: the query parser is the read-only guarantee.
- One owner is single-threaded (~1–2k writes/s) and bounded (10 GB): split very large owners.

## 16. Non-goals for v1

Cross-owner transactions (use sagas in apps), cross-owner queries (projections), UI components, auth and identity
(the hub), file storage itself (R2), full-text and vector search, collaborative editing (CRDT inside a record later).

## 17. Open questions

1. LemmaScript is a tech preview: which laws it can prove today, and the fallback (fast-check) for the rest.
2. Policy adapter default: simple roles in v1, Cedar optional; or Cedar from day one?
3. Projections and cross-owner reads: a kernel helper in v1.1, or always app code?
4. Package scope and name: `@hachej/boring-kernel`?
