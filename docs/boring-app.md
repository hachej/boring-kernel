# boring-kernel in boring-app

How the kernel fits the Boring app template (hachej/boring-app) and its agent runtime (`@boring/agent`, from
hachej/boring-ui-v3). Status: proposal, 2026-10-14. The kernel spec (`SPEC.md`) stays generic; this file is the
integration contract.

## 1. The split

**boring-kernel = the system of record**, a library like `@boring/agent`. It replaces what each app writes by hand
today:

| Today in boring-app | With the kernel |
| --- | --- |
| `server/app/domain.mjs` (hand-written SQL, validation, `fail`) | `modules/<name>/`: schema, ops, queries, rules, laws |
| `server/migrations/0002…` (product tables) | record types and projections; kernel migrations and backfills |
| Product part of `server/shell/db.mjs` | the kernel's Node adapter (one SQLite file per owner) |
| `experience/src/shared/api.ts` (hand-written fetch) | `@hachej/boring-kernel/client` + `/react` (TanStack Query) |
| Direct SQL from jobs (`work.mjs` `saveSummary`) | jobs' `collect` calls ops |

**boring-app = the template around it**, unchanged in role: the shell (HTTP, Cloudflare Access auth, workspace,
settings, run log), the agent runtime (agents, jobs, conversations), the page (React, Vite, shadcn), the verify CLI
and feature map, evals, skills, CI and deploy.

`0001_shell.sql` and the agent store (`agent.sqlite`: threads, runs, usage, agent receipts) stay outside the kernel:
they are operational data, not business records.

## 2. Contracts where they meet

| Concern | Rule |
| --- | --- |
| **Mutating agent tools** (AGENT-2, AGENT-4) | Every kernel op is offered as an agent tool from its descriptor (`SPEC.md` §14.1). The tool handler invokes the op with intent `run:<runId>:<toolCallId>`, so the agent receipt and the kernel receipt name the same effect; observed and produced revisions come from the kernel's read set and events. |
| **Read tools** | Registered queries are offered as read-only agent tools; agent SQL (`SPEC.md` §7.2) is an optional tool per app policy. |
| **Authority** (AGENT-8, AGENT-9) | The Host is implemented on the kernel: `resolveActor` binds the Access identity to the owner's membership; `mayRequest` and `allowedTools` ask `authorize`. Roles exist once, in kernel policies. |
| **Approvals** (AGENT-5) | Business approvals are `kernel.approval` records bound to op input and record revisions. A run that needs one waits on that record; the person grants it from the page. The library's own question/approval rows, when they land, reference the kernel approval rather than duplicate it. |
| **Idempotency** (AGENT-11) | A wire request's `idempotencyKey` and a page's `client.op` intent are the same discipline: the key is created once per user action and reused on retry. |
| **Schedules** (CAPABILITIES: not possible) | Kernel timers and jobs (`SPEC.md` §5.2, §10.3) through the Node adapter's wake-up port. A scheduled agent run is an op that starts a job. |
| **Outbound messages** (CAPABILITIES: not possible) | Kernel effects (`SPEC.md` §10.2); the dispatcher calls a platform service. |
| **Delivery of results to the page** | Kernel subscriptions (`SPEC.md` §14.2) for records; the wire's run/thread event streams for agent progress. |

## 3. Layout of an app

```
modules/<name>/          schema.ts, ops.ts, queries.ts, rules.ts, read.ts, LAWS.ts, journeys/, data/, test/
agents/ jobs/ conversations/        unchanged (agent runtime)
server/shell/            unchanged, plus kernel bootstrap (platform, owners, Host on policies)
server/app/              thin: registers modules, agent tools from ops, product routes if any
experience/src/app/<module>/        screens, each with its manifest
test/e2e/                Playwright; runs module journeys with the browser driver
docs/verify/<area>.md    feature map; one area per module (or per screen group)
```
Screens live in the page project (`experience/`), grouped by module, because the page is its own Vite build.

## 4. One gate

`node scripts/verify.mjs check` stays the only gate. It gains `boring-kernel verify` (laws, boundaries, generated
client, screen manifests), journeys on the API driver, and the role matrix. Playwright journeys become the browser
driver of module journeys. `AGENTS.md` "Never" adds: laws, journeys, policies and protected registration without
the product owner's agreement (the kernel's trusted gate boundary).

## 5. Platforms: both supported

The kernel ships both adapters as production adapters (`SPEC.md` §3.1). An app picks one per deployment:

| | Node (VM, today's boring-app deploy) | Cloudflare (Workers + Durable Objects) |
| --- | --- | --- |
| Owner | one SQLite file per owner, one supervised writer process | one Durable Object per owner |
| Server shell | `server/shell` on Node HTTP (current) | a Worker entry with the same routes |
| Agent runtime | `@boring/agent` on Flue, Node (current) | to be verified: Flue on Workers |
| Archive, blobs | S3-compatible (e.g. OVH Object Storage) | R2 |
| Fits | certified hosting (health data), existing apps | serverless, many small owners, `eu` jurisdiction |

Modules, journeys and the page are identical on both; the law suite and journeys run on both adapters in CI. The
agent runtime on Workers is the open item: until it is verified, Cloudflare deployments may host the kernel and
call agents over the wire.

## 6. Zod migration plan

One schema library everywhere: **Zod 4** (JSON Schema export built in, used by the MCP SDK and shadcn forms, best
known by agents). Today valibot appears only inside `@boring/agent`, as a shim converting JSON Schema tool inputs to
valibot for Flue (`packages/agent/src/runtime/flue.ts`, `toValibot`); agent contracts themselves are JSON Schema
(`tool.json`) and hand-written `validate`.

1. **Check Flue's tool input type** (boring-ui-v3). If `defineTool` accepts Standard Schema, replace `toValibot`
   with a JSON Schema → Zod conversion (`z.fromJSONSchema` or a 15-line equivalent of the current shim). If it
   requires valibot, keep the shim internal and private to the Flue adapter; nothing else depends on it. Either
   way, no valibot type appears in the library's public API.
2. **Agent output contracts**: allow `contract.ts` exporting a Zod schema next to `tool.json`; the runtime derives
   the JSON Schema with `z.toJSONSchema` and validates with the same schema. `tool.json` stays supported.
3. **boring-app**: drop `valibot` from `package.json`; product validation moves into module schemas (Zod) as
   `domain.mjs` is replaced by modules.
4. **Gate**: a lint rule rejects `valibot` imports outside the Flue adapter.
5. **Order**: (1) and (2) in boring-ui-v3, sync to boring-app with `scripts/sync-boring.mjs`; (3) and (4) in the
   template when the first module lands. Apps follow by the usual template port.

## 7. Adoption steps

1. Kernel spike on the Node adapter: notes module replacing `domain.mjs` in a branch of boring-app; journeys green.
2. Host on kernel policies; ops as agent tools; job `collect` through ops.
3. Client and React hooks replacing `api.ts`; subscriptions for live updates.
4. Cloudflare adapter against the same journeys.
5. First real module set: accounting (with firm feeds), then the clinic.
