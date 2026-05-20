# Changelog: gRPC Migration (since `last-prod-v2.0.3-rc.1`)

## Overview

The entire L1 communication has been migrated from **WebSocket + HTTP JSON-RPC** to **gRPC**.
The indexer is no longer required. All `iotax_*` calls (getCoins, getDynamicFields, etc.)
now run through gRPC `StateService`, `LedgerService`, and `TransactionExecutionService`.

---

## 1. New gRPC Client Package (`clients/iota-go/iotagrpc/`)

Entirely new package — did not exist before.

| File | Description |
|---|---|
| `grpc_client.go` (1306 lines) | Main client: wraps `LedgerService`, `StateService`, `TransactionExecutionService`. Drop-in replacement for all indexer-backed calls. |
| `stream_client.go` (203 lines) | Streaming client for event subscriptions via gRPC server streams. |
| `iota/grpc/v1/*/` | Generated Protobuf stubs: `ledger_service`, `state_service`, `transaction_execution_service`, `filter`, `coin`, `epoch`, `event`, `types`, `transaction`, `object`, `signatures`, `bcs`, `dynamic_field`, `checkpoint`, `command`, `move_package_service` |

**New methods in `grpc_client.go`:**
- `GetCoins` / `GetAllCoins` → replaces `iotax_getCoins`
- `GetObject` / `GetObjectBCS` → replaces `iota_getObject`
- `ListOwnedObjectsPage` / `ListOwnedObjects` → replaces `iotax_getOwnedObjects`
- `GetDynamicField` / `GetDynamicFields` → replaces `iotax_getDynamicFields`
- `GetIotaSystemState` → replaces `iotax_getLatestIotaSystemState`
- `ExecuteTransaction` → replaces `iota_executeTransactionBlock`
- `SimulateTransaction` → replaces `iota_dryRunTransaction`
- `Health` → new health check endpoint

---

## 2. ISCMove Client: gRPC Overrides (`clients/iscmove/iscmoveclient/`)

### New Files

**`client_grpc.go`** (282 lines)
- Methods on `Client` shadow the identically-named methods on the embedded `*iotaclient.Client`, routing all calls directly to gRPC. No JSON-RPC fallback.
- Overridden methods: `GetObject`, `GetOwnedObjects`, `GetCoins`, `GetAllCoins`, `GetCoinObjsForTargetAmount`, `GetCoinMetadata`, `GetReferenceGasPrice`, `GetAnchorFromObjectID`, `GetPastAnchorFromObjectID`, `SimulateTransaction`, `ExecuteTransaction`.

**`event_listener.go`** (205 lines)
- `EventListener` interface with gRPC-only implementation (`GRpcClientWrapper`) using `StreamCheckpoints`.
- `selectEventClient()`: validates `grpc://` scheme and creates the wrapper.
- Channels: `SubscribeEvents() (<-chan iscmove.RequestEvent)` and `SubscribeAnchorUpdates() (<-chan *iscmove.AnchorWithRef)`.

**`client_assets_bag_grpc.go`** (100 lines)
- gRPC variant of `GetAssetsBagWithBalances`.

### Modified Files

**`client.go`**: `WithGRPCClient(grpcClient)` method added; `grpcClient` field added to `Client` struct.

**`client_anchor.go`**: Removed `ReceiveRequestsAndTransition` and other indexer-specific methods (~59 lines removed).

**`feed.go`**: Major refactor:
- `wsClient *Client` → `eventClient EventListener` (transport abstraction)
- `NewChainFeed` now takes `socketURL string` + `httpClient *Client` instead of `wsURL` + `httpURL`
- `subscribeToNewRequests`: WebSocket reconnect loop removed, replaced by a single `eventClient.SubscribeEvents(ctx)` call
- `consumeRequestEvents`: now receives `<-chan iscmove.RequestEvent` instead of `<-chan *iotajsonrpc.IotaEvent` with manual BCS unmarshalling
- Client-side filtering by `anchorID` (gRPC cannot filter by event field value)

---

## 3. NodeConnection (`packages/nodeconn/`)

### `nodeconn.go`
- `wsURL string` + `httpURL string` → **`grpcURL string`** (single endpoint)
- `New(...)`: now creates `iotagrpc.NewClient(grpcAddr)` and calls `httpClient.WithGRPCClient(grpcClient)`
- `WaitUntilInitiallySynced`: `GetLatestIotaSystemState()` → `httpClient.Health(ctx)`
- `AttachChain`: passes `grpcURL` instead of `wsURL`+`httpURL`

### `chain.go`
- `newNCChain`: signature `wsURL, httpURL string` → `grpcURL string`
- `postTxLoop` simplified:
  - `DryRunTransaction()` → `SimulateTransaction()` (no more manual nil/IsFailed checks)
  - `ExecuteTransactionBlock(ExecuteTransactionBlockRequest{...})` → `ExecuteTransaction(ctx, txBytes, signatures)`
  - Anchor ObjectID is now derived directly from `chainID.AsObjectID()` instead of being parsed from TX effects

---

## 4. L1 Parameters Fetcher (`packages/parameters/fetcher.go`)

- `NewL1ParamsFetcher(grpcClient, log)` — takes only a gRPC client, no JSON-RPC fallback
- `FetchLatestGRPC` — fetches system state via gRPC (epoch info, coin metadata, total supply)
- Improved logging on fetch (Epoch, ProtocolVersion, ReferenceGasPrice, etc.)

---

## 5. Configuration

### `config.json` / `config_defaults.json`
```diff
- "httpURL": "https://api.iota-rebased-alphanet.iota.cafe",
- "websocketURL": "wss://api.iota-rebased-alphanet.iota.cafe",
+ "grpcURL": "grpc://grpc.alphanet.iota.cafe",
```

### `components/nodeconn/params.go`
```diff
- WebsocketURL string `default:"ws://localhost:9000"`
- HttpURL      string `default:"http://localhost:9000"`
+ GrpcURL      string `default:"grpc://localhost:50051"`
```

### `components/nodeconn/component.go`
- Now passes `ParamsL1.GrpcURL` instead of `ParamsL1.WebsocketURL` + `ParamsL1.HttpURL`

### `tools/cluster/` (Templates + Config)
- `L1HttpHost` + `L1WsHost` → `L1GrpcURL`
- URL construction: `grpc://` prefix instead of `https://` / `wss://`

---

## 6. Solo / Tests

- `packages/solo/solo.go` + `solofun.go`: minor adjustments for gRPC compatibility
- `packages/testutil/l1starter/remote_node.go`: new file (25 lines) for remote node support in test setup
- `packages/testutil/l1starter/local_node.go`: `GrpcURL()` method added
- `clients/iscmove/iscmoveclient/iscmoveclienttest/setup.go`: cleanup (16 lines removed)
- `packages/origin/origin.go`: minor additions

---

## 7. Dependencies (`go.mod` / `go.sum`)

- New gRPC/Protobuf dependencies added:
  - `google.golang.org/grpc`
  - `google.golang.org/protobuf`
  - `fieldmaskpb` and other Protobuf runtime packages
- 34 lines changed in `go.mod`, 37 new entries in `go.sum`

---

## Summary of Breaking Changes

| What | Before | After |
|---|---|---|
| Config keys | `httpURL` + `websocketURL` | `grpcURL` |
| Default port | 9000 | 50051 |
| `nodeconn.New()` signature | `wsURL, httpURL string` | `grpcURL string` |
| `NewChainFeed()` signature | `wsURL, httpURL string` | `grpcURL string, httpClient *Client` |
| Health check | `GetLatestIotaSystemState()` | `Health()` |
| TX execution | `ExecuteTransactionBlock(request)` | `ExecuteTransaction(ctx, bytes, sigs)` |
| Dry-run | `DryRunTransaction()` + manual effect check | `SimulateTransaction()` |

---

## Risk Analysis: Production Deployment (EVM)

### 🟡 Medium Risk

#### 3. Checkpoint stream: no cursor persistence on reconnect
**File:** [clients/iota-go/iotagrpc/stream_client.go](clients/iota-go/iotagrpc/stream_client.go)

The `StreamClient` has reconnect logic with exponential backoff (5s → 30s). On reconnect,
**no checkpoint cursor** is passed — the stream restarts from the current checkpoint.

- Events (ISC Requests) that arrived during an outage of up to 30s are **not replayed**
- The old WebSocket code had the same behavior (no resume), so this is not a regression — but worth noting explicitly
- On a node restart or brief network interruption in prod, missed requests must be recovered via other means (existing mempool mechanisms apply here)

**Recommendation:** Cache the last processed checkpoint sequence number and pass it as `StartCheckpoint` on reconnect.

---

#### 4. Client-side anchor filtering for events
**File:** [clients/iscmove/iscmoveclient/feed.go:94](clients/iscmove/iscmoveclient/feed.go#L94)

```go
// gRPC cannot filter by event field value, so we filter client-side.
```

All ISC `RequestEvent`s for the entire package are streamed. Filtering by `anchorID` (= ChainID)
happens only after receipt.

- With many active chains on the same L1 node: **significantly more traffic and CPU** than necessary
- Does not scale well when many Wasp nodes share the same gRPC node
- **Not a concern in practice:** Wasp effectively supports only one chain per node. Multi-chain support was an earlier design goal but is no longer an active use case.

---

### 🟢 No Risk / Already Covered

| Topic | Status |
|---|---|
| Stream reconnect logic | Present, exponential backoff 5s→30s |
| gRPC connection keepalive | Configured (30s/10s) |
| gRPC URL validation | Startup error on wrong format |
| Dry-run before TX | Preserved via `SimulateTransaction()` |
| JSON-RPC / WebSocket removed | All L1 communication is exclusively via gRPC |
| TX execution error handling | `GetError()` from gRPC response is correctly checked |

---

### Recommended Actions Before Production Deployment

1. **Smoke test after deploy** — post a TX and verify the digest is logged, confirm anchor updates are received, test stream reconnect behavior
2. **Checkpoint resume** — nice-to-have, not a blocker for the initial deployment
3. **TLS support** *(nice-to-have)* — gRPC is used internally (plaintext is fine), but `grpcs://` support can be added later if needed: scheme-based credential selection in `iotagrpc.NewClient`, `stream_client.go`, `event_listener.go`, and the URL validation in `nodeconn.go`

---

## Future TODOs

#### Migrate wasp-cli to gRPC
`tools/wasp-cli` still uses JSON-RPC (`iotaclient.NewHTTP`) for L1 queries and
`parameters.FetchLatestGRPC` via a URL derived from the HTTP API address (a workaround).
Once the CLI gets a dedicated `grpcURL` config key, it should use `iotagrpc.NewClient`
directly — same as `nodeconn` does — and drop the HTTP dependency for L1 entirely.

---

#### Replace manual BCS parsing with `bcs.Unmarshal` from the iota-rust-sdk Go binding
Several places in the gRPC event pipeline manually parse raw BCS bytes (e.g. in `event_listener.go`).
Once the iota-rust-sdk Go binding exposes `bcs.Unmarshal` for the relevant types, these manual
parsing steps should be replaced to reduce maintenance burden and improve correctness guarantees.
