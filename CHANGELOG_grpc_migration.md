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

## 2. ISCMove Client: Three-way Split (`clients/iscmove/iscmoveclient/`)

The monolithic `Client` struct has been split into three clearly separated types:

### `GRPCClient` (`grpc_client.go`)

Pure gRPC client — **no `*iotaclient.Client` embedding**, no JSON-RPC fallback.
Used exclusively by the wasp node via `nodeconn`.

- Implements `NodeL1Client` interface (verified by `var _ NodeL1Client = &GRPCClient{}`)
- Methods: `GetObject`, `GetOwnedObjects`, `GetCoins`, `GetAllCoins`, `GetCoinObjsForTargetAmount`, `GetCoinMetadata`, `GetReferenceGasPrice`, `GetAnchorFromObjectID`, `GetPastAnchorFromObjectID`, `GetAssetsBagWithBalances`, `GetCoin`, `GetRequestFromObjectID`, `GetRequestsSorted`, `GetRequests`, `SimulateTransaction`, `ExecuteTransaction`, `Health`

### `CLIClient` (`client.go`)

HTTP/JSON-RPC only — embeds `*iotaclient.Client`, **no gRPC field**.
Used by wasp-cli and apilib.

- `GetAssetsBagWithBalances` and `GetPastAnchorFromObjectID` are panic stubs (not supported without gRPC)
- Constructor: `NewCLIClient(client, faucetURL)` / `NewHTTPClient(apiURL, faucetURL, waitParams)`

### `SoloClient` (`client_solo.go`)

Extends `CLIClient` with a `*iotagrpc.Client` for operations that require gRPC.
Used by solo and integration tests.

- Embeds `*CLIClient` + `grpcClient *iotagrpc.Client`
- Overrides: `GetAssetsBagWithBalances`, `GetPastAnchorFromObjectID` (gRPC-backed)
- Overrides `GetRequestFromObjectID`, `GetRequestsSorted`, `GetRequests`, `ReceiveRequestsAndTransition` to pass `*SoloClient` as `iscFetcher` (required due to Go's lack of virtual dispatch on embedded methods)
- Constructor: `NewSoloClient(httpClient, grpcClient)`

### Shared Helpers (`client_shared.go`)

Package-level helpers accepting the `iscFetcher` interface eliminate code duplication between `GRPCClient` and `SoloClient`:
`getAnchorFromObjectID`, `getCoin`, `getRequestFromObjectID`, `parseRequestAndFetchAssetsBag`, `pullRequests`, `getRequestsSorted`, `getRequests`

The shared `receiveRequestsAndTransitionWith(ctx, fetcher, ptbSigner, req)` helper is used by both `CLIClient.ReceiveRequestsAndTransition` and `SoloClient.ReceiveRequestsAndTransition`.

### Interface (`client_interface.go`)

`NodeL1Client` interface — the contract between `nodeconn` and the L1 client layer:
`Health`, `GetObject`, `GetAnchorFromObjectID`, `GetRequestFromObjectID`, `GetRequestsSorted`, `ExecuteTransaction`, `SimulateTransaction`

### Other New/Modified Files

**`event_listener.go`**
- `EventListener` interface with gRPC-only implementation (`GRpcClientWrapper`) using `StreamCheckpoints`.
- `selectEventClient()`: validates `grpc://` scheme and creates the wrapper.
- Channels: `SubscribeEvents() (<-chan iscmove.RequestEvent)` and `SubscribeAnchorUpdates() (<-chan *iscmove.AnchorWithRef)`.
- Uses `NodeL1Client` instead of `*Client` for the `httpClient` field.

**`feed.go`**
- `wsClient *Client` → `eventClient EventListener` (transport abstraction)
- `NewChainFeed` now takes `grpcURL string` + `httpClient NodeL1Client`
- `subscribeToNewRequests`: WebSocket reconnect loop removed, replaced by a single `eventClient.SubscribeEvents(ctx)` call
- `consumeRequestEvents`: now receives `<-chan iscmove.RequestEvent` instead of `<-chan *iotajsonrpc.IotaEvent` with manual BCS unmarshalling
- Client-side filtering by `anchorID` (gRPC cannot filter by event field value)

---

## 3. NodeConnection (`packages/nodeconn/`)

### `nodeconn.go`
- `wsURL string` + `httpURL string` → **`grpcURL string`** (single endpoint)
- `New(...)`: now creates `iotagrpc.NewClient(grpcAddr)` and `iscmoveclient.NewGRPCClient(grpcClient)` — pure gRPC, no HTTP client
- `l1Client *iscmoveclient.GRPCClient` (was `httpClient *iscmoveclient.Client`)
- `WaitUntilInitiallySynced`: `GetLatestIotaSystemState()` → `l1Client.Health(ctx)`
- `AttachChain`: passes `grpcURL` + `l1Client` (typed as `NodeL1Client`) instead of `wsURL`+`httpURL`

### `chain.go`
- `newNCChain`: signature `wsURL, httpURL string` → `grpcURL string`
- `httpClient` → `l1Client` (renamed to reflect that it's a pure gRPC client)
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

- `packages/solo/solofun.go`: `ISCMoveClient()` now returns `*iscmoveclient.SoloClient` (was `*Client`); constructs `CLIClient` + optional `GRPCClient` and combines them via `NewSoloClient`
- `clients/iscmove/iscmoveclient/iscmoveclienttest/setup.go`: `NewHTTPClient()` now returns `*iscmoveclient.SoloClient`; picks up gRPC URL from `l1starter.Instance().GrpcURL()`
- `packages/testutil/l1starter/remote_node.go`: new file for remote node support in test setup
- `packages/testutil/l1starter/local_node.go`: `GrpcURL()` method added
- `clients/l2client.go`: compile-time assertion updated to `var _ L2Client = &iscmoveclient.CLIClient{}`
- `clients/l1client.go`: `L2()` method uses `NewCLIClient` (was `NewClient`)
- All test files updated: `*iscmoveclient.Client` → `*iscmoveclient.SoloClient`

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
| `NewChainFeed()` signature | `wsURL, httpURL string` | `grpcURL string, httpClient NodeL1Client` |
| Health check | `GetLatestIotaSystemState()` | `Health()` |
| TX execution | `ExecuteTransactionBlock(request)` | `ExecuteTransaction(ctx, bytes, sigs)` |
| Dry-run | `DryRunTransaction()` + manual effect check | `SimulateTransaction()` |
| ISCMove client type (node) | `*Client` (HTTP+gRPC) | `*GRPCClient` (pure gRPC) |
| ISCMove client type (cli/apilib) | `*Client` | `*CLIClient` (HTTP only) |
| ISCMove client type (solo/tests) | `*Client` | `*SoloClient` (HTTP + gRPC) |
| `NewClient(client, faucetURL)` | — | `NewCLIClient` / `NewSoloClient` |

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
| WebSocket removed | All wasp node L1 communication is exclusively via gRPC |
| wasp-cli JSON-RPC | Intentional — CLI uses HTTP JSON-RPC, no gRPC dependency |
| TX execution error handling | `GetError()` from gRPC response is correctly checked |

---

### Recommended Actions Before Production Deployment

1. **Smoke test after deploy** — post a TX and verify the digest is logged, confirm anchor updates are received, test stream reconnect behavior
2. **Checkpoint resume** — nice-to-have, not a blocker for the initial deployment
3. **TLS support** *(nice-to-have)* — gRPC is used internally (plaintext is fine), but `grpcs://` support can be added later if needed: scheme-based credential selection in `iotagrpc.NewClient`, `stream_client.go`, `event_listener.go`, and the URL validation in `nodeconn.go`

---

## Future TODOs

#### wasp-cli: intentionally JSON-RPC only
`tools/wasp-cli` uses JSON-RPC (`iotaclient.NewHTTP`) exclusively for all L1 queries.
`parameters.FetchLatestHTTP` was added to `packages/parameters/fetcher.go` so that
`chain deploy` can fetch epoch/gas/supply info without a gRPC client.

This is a deliberate design decision: gRPC is for the wasp node and solo/tests only.
wasp-cli stays on JSON-RPC to keep the CLI dependency-light and configuration simple.

---

#### Revert `ExecuteTransaction` checkpoint polling once node-side bug is fixed

`ExecuteTransaction` in `clients/iota-go/iotagrpc/grpc_client.go` currently uses a
client-side polling loop (50ms interval via `waitForCheckpointInclusion`) instead of the
server-side `CheckpointInclusionTimeoutMs` field.

Background: with `CheckpointInclusionTimeoutMs` set, the gRPC server blocks for the full
timeout duration regardless of when the transaction is actually included in a checkpoint
(i.e. it does not return early on inclusion). This caused ~30s latency per transaction even
though checkpoints are produced at ~20/s. The L1 node team has been informed.

Once the server-side behavior is fixed (early return upon checkpoint inclusion), the polling
loop can be replaced with a simple `CheckpointInclusionTimeoutMs` of a few hundred
milliseconds.

---

#### Replace manual BCS parsing with `bcs.Unmarshal` from the iota-rust-sdk Go binding
Several places in the gRPC event pipeline manually parse raw BCS bytes (e.g. in `event_listener.go`).
Once the iota-rust-sdk Go binding exposes `bcs.Unmarshal` for the relevant types, these manual
parsing steps should be replaced to reduce maintenance burden and improve correctness guarantees.
