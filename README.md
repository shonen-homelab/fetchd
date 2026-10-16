# fetchd

A small authenticated HTTP fetch-proxy for personal use. The rakuyomu novel
reader and other projects call it to get raw page HTML. **No content parsing —
fetchd returns upstream bytes as-is.**

## API

All `/fetch` endpoints require auth: `Authorization: Bearer <key>` or
`X-API-Key: <key>` (keys from `FETCHD_API_KEYS`). `GET /healthz` is open.

### `GET /fetch?url=<encoded>`

```sh
curl -H "X-API-Key: $KEY" \
  "http://localhost:8080/fetch?url=https%3A%2F%2Fexample.com%2Fnovel%2F1"
```

### `POST /fetch`

```sh
curl -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/novel/1","method":"GET",
       "headers":{"Accept-Language":"ja-JP"},"timeout_ms":10000}' \
  http://localhost:8080/fetch
```

- `method`: `GET` (default) or `HEAD`.
- `headers`: optional map, forwarded except hop-by-hop/`Host`/`Content-Length`.
- `timeout_ms`: optional per-request attempt timeout.

### Response

- Upstream body streamed as-is, upstream status passed through.
- Forwarded headers: `Content-Type`, `Content-Length`, `Last-Modified`,
  `ETag`, `Location` (when not following). `Content-Encoding: gzip` is
  decompressed and dropped — fetchd always returns identity bytes.
- Extra headers: `X-Fetchd-Final-URL`, `X-Fetchd-Attempts`,
  `X-Fetchd-Upstream-Status`.

### Errors (fetchd itself)

JSON `{"error":code,"message"}`:

| Status | Code | Meaning |
|---|---|---|
| 401 | `unauthorized` | missing/invalid API key |
| 400 | `bad_request` | bad URL, non-http(s) scheme, bad method |
| 403 | `blocked_target` | SSRF guard refused the target |
| 413/502 | `response_too_large` | body over `FETCHD_MAX_BODY_BYTES` (413 known length, truncated stream otherwise) |
| 429 | `queue_full` | queue full, has `Retry-After` |
| 503 | `queue_timeout` | waited too long for capacity |
| 504 | `upstream_timeout` | upstream timed out (after retries) |
| 502 | `upstream_error` | unreachable after retries; last response returned when upstream answered |

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `FETCHD_LISTEN` | `:8080` | listen address |
| `FETCHD_API_KEYS` | (required) | comma-separated keys; refuse to start empty unless `FETCHD_ALLOW_NO_AUTH=true` |
| `FETCHD_ALLOW_NO_AUTH` | `false` | allow unauthenticated fetch (testing only) |
| `FETCHD_ALLOW_PRIVATE` | `false` | disable SSRF guard |
| `FETCHD_MAX_CONCURRENCY` | `8` | global in-flight fetches |
| `FETCHD_MAX_QUEUE` | `64` | waiters beyond in-flight; excess → 429 |
| `FETCHD_QUEUE_TIMEOUT` | `30s` | max wait for capacity → 503 |
| `FETCHD_PER_HOST_CONCURRENCY` | `2` | in-flight per host |
| `FETCHD_PER_HOST_INTERVAL` | `500ms` | min gap between starts per host |
| `FETCHD_TIMEOUT` | `30s` | per-attempt upstream timeout |
| `FETCHD_MAX_BODY_BYTES` | `10485760` (10MiB) | response cap |
| `FETCHD_MAX_REDIRECTS` | `10` | follows; `0` = don't follow (302 passes through) |
| `FETCHD_USER_AGENT` | browser-like | sent upstream |
| `FETCHD_MAX_RETRIES` | `4` | retries on 429/502/503/504 + conn errors |
| `FETCHD_RETRY_BASE` | `500ms` | backoff base (full jitter, ×2 per attempt) |
| `FETCHD_RETRY_MAX_DELAY` | `30s` | backoff cap |
| `FETCHD_MAX_RETRY_WAIT` | `60s` | total wait budget; upstream `Retry-After` honored within it |

Queue fairness: waiters already queued for a host beat newcomers for that
host (no starvation); there is no strict FIFO across hosts.

## Compose

```yaml
services:
  fetchd:
    image: ghcr.io/shonen-homelab/fetchd:latest
    ports: ["8080:8080"]
    environment:
      FETCHD_API_KEYS: ${FETCHD_API_KEYS:?set me}
      # FETCHD_MAX_CONCURRENCY: "8"
    healthcheck:
      test: ["/fetchd", "healthcheck"]
      interval: 30s
      timeout: 5s
      retries: 3
    restart: unless-stopped
```

## Cloudflare Access note

Cloudflare Access sits **in front of** fetchd (inbound user auth at the edge);
fetchd's own `Authorization: Bearer` / `X-API-Key` check is service-to-service
auth behind that. There is no in-app CF Access (JWT) validation — out of scope
by design. If you expose fetchd through a CF Access-protected hostname, the
caller must satisfy both: CF Access first, then fetchd's API key.

## SSRF guard

Every dial resolves the host and refuses loopback, private, link-local, CGNAT,
ULA, unspecified, multicast, and documentation ranges — checked at dial time,
so redirects and DNS rebinding are covered. `FETCHD_ALLOW_PRIVATE=true`
disables it (never expose that to the internet).

## Memory

- Stdlib only, streaming bodies (no full buffering except the 1MiB POST
  envelope cap and 256KiB retry drain).
- `GOMEMLIMIT` friendly: no custom allocator use; set `GOMEMLIMIT` env to ~90%
  of the container limit if you cap memory, e.g. `GOMEMLIMIT: 171MiB` for a
  192MiB limit. Idle RSS ≈ a few MiB (measured § below).

## Development

```sh
go test -race ./...   # needs a C compiler for -race (e.g. zig cc)
go vet ./... && gofmt -l .
```
