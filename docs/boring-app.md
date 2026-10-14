# boring-kernel in boring-app

How the kernel fits the Boring app template (hachej/boring-app, read at `647c29c`, 2026-10-13) and the agent runtime
it uses (pi-durable, through `@hachej/boring-agent` of hachej/boring-ui 0.2.x). Status: proposal, 2026-10-14. The
kernel spec (`SPEC.md`) stays generic; this file is the integration contract.

## 1. What the template is today

- One Hono app (`server/src/app.ts`) with two runtimes: **vm** (Node, a SQLite file through better-sqlite3, Drizzle)
  and **cloudflare** (a Worker, D1, Drizzle).
- **One agent** on pi-durable (pinned 1.1.0 with pi-ai and chord): files in `agents/<name>/`, its tools are the app's
  operations (`tools/<name>/tool.json`, server handlers in `server/src/tools/`), served to the agent, the hub
  (`/agent/tools/<name>/call`) and the page. It runs in the Node process (`agent.sqlite`) or in a Durable Object per
  person (`ASSISTANT`).
- **Exposure** is declared in `boring.json` (agents, tools, views, pages); nothing undeclared is served (APP-B10).
- **Laws** in `docs/INVARIANTS.md` (APP-B1…B10, APP-F1…F8) with evidence in `scripts/verify/VERIFY.json`
  (evidence / partial / driven / deferred); boundaries as rules in `scripts/verify/ARCHITECTURE.json`; ordering laws
  as TLA+ models in `formal/` with mutants that must fail.
- **Page**: `<AppShell layout="screen">` from boring-ui (vendored), screens as nav items (`web/src/pages/`), views
  (`web/src/views/`), the agent as a layer; the web calls its server only through `web/src/lib/api.ts`.
- Zod 4 everywhere (server, boring-ui). No valibot, no Flue: the Zod migration planned in earlier drafts is done.

## 2. The split

**boring-kernel = the system of record**, a library. Per app it replaces:

| Today in the template | With the kernel |
| --- | --- |
| Product tables in Drizzle (`server/src/db/schema.ts`, e.g. `memories`) and their routes' SQL | `modules/<name>/`: record types, ops, guards, queries, laws |
| `idempotency` table and middleware (APP-B2) | kernel receipts (K4) |
| `revision` columns and conditional writes (APP-B3) | `rev` and `expectedRev` (K3, §5.2) |
| Server tool handlers that write (`server/src/tools/remember.ts`) | ops; their `tool.json` is generated from the op's Zod schema |
| D1 for business data on Cloudflare | an **Owner** Durable Object per owner (D1 has no synchronous transactions, so the kernel cannot run on it) |
| Ad-hoc re-reads after agent activity (`web/src/lib/agent-activity.ts`) | kernel subscriptions (`SPEC.md` §14.2) |

**Stays the template's**: Hono routes and auth (hub OIDC, Access, dev), the agent and its hosts, `boring.json`
exposure, the shell and screens, the verify CLI, INVARIANTS/VERIFY/ARCHITECTURE, `formal/`, deploy on both runtimes.
Drizzle remains for the shell's own non-business tables, if any; business data lives only in the kernel.

## 3. Contracts where they meet

| Concern | Rule |
| --- | --- |
| **Ops are the app's operations** | Each op exposed in `boring.json` is a server tool whose `tool.json` is generated from the op descriptor (`z.toJSONSchema`; verify fails if stale). Its handler invokes the op. pi-durable validates arguments against the schema first (APP-B4); the kernel validates again under the op version's schema, and is the authority. |
| **Intents** | Agent tool call: `run:<conversation>:<toolCallId>`. Hub or page call with `Idempotency-Key`: the key, namespaced by the verified actor. Retries reuse it; receipts answer (APP-B2 becomes K4). |
| **Actor** | `identify()` gives `{email, sub, role}` (hub role: owner, editor, user). The app binds it to the owner's membership; kernel policies map hub roles to kernel roles. Never from a body (APP-B1). |
| **Owner** | Today one team's data per app: one owner per app. An app with several tenants (a firm and its clients) has one owner per tenant and connects them with feeds (`SPEC.md` §7.3). |
| **Runtime placement** | vm: the kernel's Node adapter in the server process, one SQLite file per owner (better-sqlite3 or `node:sqlite`, both synchronous). cloudflare: one Owner Durable Object per owner (binding `OWNER`), next to the agent's `ASSISTANT`; the Worker forwards kernel calls to the owner's object. |
| **Approvals** | Ops with `approval` return `approval_required` with the subject; the page or the agent's ask-user flow shows it; a person grants it through `kernel.approval.grant`. |
| **Schedules, effects** | Kernel timers and effects, on the owner's wake (`SPEC.md` §10). |
| **Live data** | The page subscribes through the kernel client (`queryBatch` per screen); notifications follow commits, including the agent's. |

## 4. Layout of an app

```
modules/<name>/        schema.ts, ops.ts, queries.ts, feeds.ts, rules.ts, read.ts, LAWS.ts, journeys/, data/, test/
agents/ tools/         unchanged; tools/<op>/tool.json generated for exposed ops
server/src/            Hono app; kernel bootstrap per runtime (node-app.ts, worker.ts + Owner DO)
web/src/pages/<name>/  screens (nav items); web/src/views/<name>/ views; each declared in boring.json
test/e2e/              Playwright; runs module journeys with the browser driver
```
Screen manifests are the `boring.json` entries of pages and views, extended with the queries and ops they use;
`scripts/capabilities.mjs` and ARCHITECTURE rules check them.

## 5. One gate

`node scripts/verify.mjs check` stays the only gate.
- **Laws**: kernel laws (K1…K27) ship with their own `VERIFY.json` (same statuses); module laws are recorded as
  `APP-<module><n>` in `docs/INVARIANTS.md` with evidence in `scripts/verify/VERIFY.json`. A law is never "passed"
  without evidence; a deferral counts apart.
- **Ordering laws** (receipts, wake protocol, rule delivery, feed apply, recovery epoch) get TLA+ models with
  mutants, in the template's `formal/` style; pure functions get LemmaScript proofs; the rest is property-tested.
- **Boundaries**: ARCHITECTURE rules add "business data only through the kernel", "no Drizzle table for a kernel
  type", "the web reaches data only through the kernel client".
- **Journeys** run on the API driver and in Playwright on the app's runtime; the role matrix over every screen.
- **Human-owned**: laws, journeys, policies and protected registration join `CODEOWNERS` and the Factory's path policy.

## 6. Platforms

Both runtimes stay supported; an app picks one at `init --runtime`.

| | vm | cloudflare |
| --- | --- | --- |
| Kernel owner | SQLite file per owner, the server process as its single writer | Owner Durable Object per owner |
| Agent | pi-durable in the process | pi-durable in a DO per person |
| Archive, blobs | S3-compatible (OVH) | R2 |
| Wake | timer table polled by the process | DO alarm |

## 7. Adoption steps

1. **Kernel spike on the Node adapter** inside a branch of boring-app: the `memories` example becomes a kernel
   module; `remember` an op; APP-B2/B3 evidence moves to kernel laws; journeys green.
2. **Owner Durable Object** on the cloudflare runtime against the same journeys (verified in `wrangler dev`).
3. Kernel client and subscriptions behind `web/src/lib/api.ts`; screen manifests in `boring.json`.
4. First real module set: accounting (with firm feeds), then the clinic.

## 8. Open points for the maintainer

- Drizzle: keep it for non-business shell tables, or drop it once the kernel holds all data?
- The Owner DO on Cloudflare adds a second Durable Object class next to `ASSISTANT`: acceptable, or should the
  agent and the owner share an object for single-person apps?
