# iag-dms-depot-node

Distributor **depot edge node** for the IAG platform. It runs at a distributor
depot and captures documents (delivery notes, invoices, proof-of-delivery, stock
counts, GRNs), **buffers them locally so capture never blocks on connectivity**,
and **syncs them upstream to the central DMS** through the API gateway when the
link is available.

Headless Go/Gin JSON API — no bundled UI.

## How it works

```
 field capture ──POST /v1/documents──▶ [ offline buffer (Postgres) ]
                                              │
                              sync engine (background, backoff)
                                              │  service-account token (aud=iag.dms)
                                              ▼
                        central DMS  ◀──  POST {DMS_UPSTREAM_URL}{DMS_INGEST_PATH}
```

- **Offline buffer** — every captured document is persisted with status
  `buffered`. The buffer is durable (Postgres); capture works with or without a
  network link.
- **Sync engine** — a background worker claims due documents (`FOR UPDATE SKIP
  LOCKED`), POSTs each upstream, and marks it `synced` (storing the id the
  central DMS assigned) or `failed` with exponential backoff. It also tracks
  online/offline state and per-depot last-sync outcome.
- **Idempotency** — capture is idempotent on `(depotId, docType, reference)`, and
  each upstream POST carries an `Idempotency-Key` header so replays don't double
  up centrally.
- **Events** — emits `depot.document.buffered`, `depot.document.synced` and
  `depot.node.heartbeat` on `iag.operations` (via a transactional outbox when a
  DB and Kafka are configured).

## Auth

- **Inbound** — every `/v1/*` request needs a Bearer token with
  `aud=iag.dms-depot-node`. RBAC: `dms_depot.view`, `dms_depot.capture`,
  `dms_depot.manage` (registered with iag-authentication at boot).
- **Outbound** — the sync engine mints a `client_credentials` service token with
  `aud=iag.dms` to call the central DMS through the gateway.

## API

| Method | Path | Permission | Purpose |
|--------|------|-----------|---------|
| GET | `/` | public | service descriptor |
| GET | `/health`, `/healthz`, `/ready` | public | probes |
| GET | `/v1/node` | `dms_depot.view` | depot identity, online state, buffer stats |
| GET | `/v1/documents` | `dms_depot.view` | list (filter `?status=`, `?limit=`) |
| POST | `/v1/documents` | `dms_depot.capture` | buffer a document |
| GET | `/v1/documents/:id` | `dms_depot.view` | fetch one |
| POST | `/v1/documents/:id/retry` | `dms_depot.manage` | requeue a failed document |
| GET | `/v1/sync/status` | `dms_depot.view` | buffer depth + last sync outcome |
| POST | `/v1/sync/run` | `dms_depot.manage` | force an immediate drain sweep |

## Run locally

```bash
cd edge/dms-depot-node
go mod tidy
STORE_MODE=memory go run .        # in-memory buffer, no DB, no upstream sync
```

With Postgres and upstream sync:

```bash
export DATABASE_URL=postgres://user:pass@localhost:5432/iag_platform?sslmode=disable
export DMS_UPSTREAM_URL=http://localhost:8080/api/v1/dms
export SERVICE_CLIENT_SECRET=... # client_credentials secret for aud=iag.dms
go run .
```

`GET http://localhost:4020/` returns the service descriptor.

## Upstream ingest contract

The node POSTs each buffered document to `DMS_UPSTREAM_URL + DMS_INGEST_PATH`
(default `/v1/depot/documents`) as:

```json
{ "depotId": "...", "docType": "...", "reference": "...", "outletId": "...",
  "payload": { }, "capturedBy": "...", "capturedAt": "RFC3339" }
```

A 2xx is treated as accepted; the node reads an `id` / `upstreamId` / `data.id`
from the response to record `upstreamId`. **The matching ingest endpoint on the
central DMS is the counterpart to build** — until it exists, point
`DMS_INGEST_PATH` at whatever endpoint accepts the document, or leave
`DMS_UPSTREAM_URL` unset to run pure-buffer mode.

## Config

See [`config/.env.example`](config/.env.example). Key vars: `DEPOT_ID`,
`DATABASE_URL`, `AUDIENCE`, `JWKS_URL`, `SERVICE_CLIENT_SECRET`,
`DMS_UPSTREAM_URL`, `DMS_UPSTREAM_AUDIENCE`, `DMS_INGEST_PATH`, `SYNC_ENABLED`,
`SYNC_INTERVAL`, `EVENT_BUS_ENABLED`, `KAFKA_BROKERS`.

Registry: [`subrepos.json`](../../subrepos.json) · Dev port: **4020**
