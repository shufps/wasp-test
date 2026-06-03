# Changelog: gRPC Migration (since `last-prod-v2.0.3-rc.1`)

## Overview

The **wasp node's** L1 communication has been migrated from **WebSocket + HTTP JSON-RPC** to
**gRPC**, so the node no longer needs a co-located Indexer. All of the node's `iotax_*`-style
reads (getCoins, getDynamicFields, etc.) now run through gRPC `StateService`, `LedgerService`,
and `TransactionExecutionService`.

This is **node-only** by design — wasp-cli, solo and the test/integration tooling stay on
JSON-RPC and point at a separate Indexer endpoint. See "Transport architecture" below for the
rationale and the per-consumer split.

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

## Transport architecture: gRPC (node) vs JSON-RPC (cli / tests)

JSON-RPC is **not** being removed — it remains available via the **Indexer**. Moving the node
to gRPC is an *operational* decision, not a protocol deprecation: on main/testnet each Wasp
node runs next to its own IOTA node (≈6 committee + N access nodes). Co-locating an Indexer
(which requires a Postgres) with every one of those L1 nodes is too much overhead, so the
wasp **node** talks to its lean, indexer-less IOTA node over **gRPC** instead.

Everything else intentionally stays on JSON-RPC, pointed at a **separate** L1 endpoint:

| Consumer | Transport | Config key | Endpoint |
|---|---|---|---|
| wasp node (daemon) | gRPC | `l1.grpcURL` (`grpc://host:50051`) | its co-located IOTA node (no indexer) |
| wasp-cli | JSON-RPC | `l1.apiAddress` | a (public) Indexer exposing JSON-RPC |
| solo / integration tests | JSON-RPC (+ gRPC for a few node-path ops) | l1starter config | an Indexer docker image (or current localnet) |

The node's `l1.grpcURL` and the CLI's `l1.apiAddress` are **independent** config values and
normally point at different hosts — the CLI never talks to the committee's per-node IOTA nodes.
`parameters.FetchLatestHTTP` exists so `wasp-cli chain deploy` can fetch epoch/gas/supply over
JSON-RPC without a gRPC client.

**This is the target architecture, not a migration gap.** JSON-RPC operations that have no gRPC
equivalent — `SignAndExecuteTransaction`, `Publish`/`PublishContract`, `GetAllBalances`/`GetBalance`,
and the iscmove request-posting methods (`StartNewChain`, `CreateAndSendRequest*`,
`ReceiveRequestsAndTransition`) — are intentionally **not** being ported to gRPC; they stay on the
JSON-RPC/Indexer path used by cli and tests. Per maintainer guidance, transitions are kept to the
minimum needed to keep main stable: only the wasp node was moved to gRPC.

---

## 8. BCS Struct Migration (`clients/iota-go/iotagrpc/`)

All manual BCS parsing (hand-written byte readers) replaced with proper Go structs + `bcs.Unmarshal` / `bcs.UnmarshalStream` from `github.com/iotaledger/bcs-go`.

### New files

| File | Description |
|---|---|
| `bcs_types.go` | BCS structs for object decoding: `versionedObject`, `grpcObject`, `grpcObjectData`, `grpcMoveStruct`, `coinContents`; helpers `ownerAddress`, `moveObjectTypeToCoinType` |
| `bcs_system_state.go` | BCS structs for system state decoding: `sysValidatorMetadataV1`, `sysStakingPoolV1`, `sysValidatorV1`, `sysValidatorSetV1/V2`, `sysStateHeadV1/V2` |

### Deleted from `grpc_client.go`

- `bcsReader` type and all methods (`readU8`, `readU64`, `readU32`, `readULEB128`, `readBytes`, `readString`, `skip`, `readMoveObjectType`, `readTypeTag`, `readStructTag`, `skipStructTag`, ~10 more)
- All `skipValidator*` and `skipStakingPool*` helpers
- ~400 lines total removed

### Changed functions

| Function | Before | After |
|---|---|---|
| `parseObjectBCS` | ~50 lines, manual byte reading via `bcsReader` | ~15 lines, `bcs.UnmarshalStream[versionedObject]` |
| `parseCoinFromObjectBCS` | ~60 lines, manual byte reading | ~30 lines, `bcs.UnmarshalStream[versionedObject]` |
| `decodeSystemStateBCS` | ~300 lines, manual byte reading + 6 `skip*` helpers | ~25 lines, `bcs.NewDecoder` + struct decode |

### Notes

- `bcs.UnmarshalStream` (not `bcs.Unmarshal`) is used for object decoding because the gRPC wire format includes a trailing byte after the last struct field that would trigger a strict "excess bytes" check. Streaming decode reads only what the struct consumes.
- `grpcMoveStruct` intentionally omits `HasPublicTransfer` — it is not present in `iota-sdk-types::MoveStruct` (only in the internal `iota-types::MoveObject` used on the JSON-RPC path).
- Fixed-size `Bag` / `Table` / `StorageFund` fields in system state are decoded as `[40]byte` / `[16]byte` blobs (their internal structure is not needed).

---

## 9. Review Fixes (post-review hardening)

Scope is deliberately limited to defects **introduced by / required by the gRPC
migration**. Pre-existing bugs in non-gRPC code that were noticed during review
are listed under "Out of scope" below and left for separate PRs.

### 🔴 Correctness / crash

| Fix | File | What |
|---|---|---|
| `PickupCoins` nil-target panic | `clients/iota-go/iotagrpc/grpc_client.go` | `GetCoinObjsForTargetAmount` (new gRPC code) passed `nil` as the `*big.Int` target to `PickupCoins`, which dereferences it (`big.Int.Add(nil, …)`) → guaranteed nil-pointer panic on every call. Now passes `new(big.Int).SetUint64(targetAmount)`, matching the JSON-RPC implementation. Regression test added in `grpc_client_pickup_test.go`. |
| `node_test.go` build failure | `packages/chain/node_test.go` | The migration changed `parameters.NewL1ParamsFetcher` to take `*iotagrpc.Client`, but the test still passed `*iotaclient.Client` → `packages/chain` (and everything depending on its test helpers) failed to compile. The test now builds a real `iotagrpc.Client` against the local node's `GrpcURL()`. |
| Local node gRPC unreachable | `packages/testutil/l1starter/local_node.go`, `l1starter.go` | `GrpcURL()` returned the mapped REST port (9000); gRPC calls hit the JSON-RPC server → HTTP 404 → gRPC-go reports `Unimplemented` (this is the `TestReceiveRequestAndTransition` failure). Root cause: with gRPC enabled but no explicit address, the node binds gRPC to `127.0.0.1:<random>` (`iota-swarm-config/node_config_builder.rs` fallback) — a loopback bind that Docker cannot port-map. Fix: pass `--with-grpc=0.0.0.0:50051` (iota PR #11041), expose/map `50051`, and point `GrpcURL()` at the mapped port. Also renames the container `Cmd` from the removed `iota` subcommand to `iota-localnet` (required for the container to boot on the current image — prerequisite for the gRPC wiring). Verified: `TestReceiveRequestAndTransition` and `TestStartNewChain` pass against `iota-tools:devnet` (iota ≥ 1.24.0-beta). Requires an image that includes `--with-grpc`. |

### 🟠 Robustness / resource (all in gRPC-introduced code)

| Fix | File | What |
|---|---|---|
| `CLIClient.GetAssetsBagWithBalances` implemented (JSON-RPC) | `clients/iscmove/iscmoveclient/client.go` | The migration's client split left this a panic stub, which broke `wasp-cli inspect assetsbag` and the `L2Client` contract. Re-implemented over JSON-RPC (`iotax_getDynamicFields` + `iota_getObject`) — the pre-migration logic — so the CLI/L2Client path works against a JSON-RPC Indexer. Verified live (the JSON-RPC path runs against the local node in `TestNodeBasic`). It intentionally stays JSON-RPC (see "Transport architecture"); the node uses the separate gRPC `GRPCClient` variant. `SoloClient`'s nil-grpc guards are left as panics (test-only programmer-error assertions). |
| Ignored `iotagrpc.NewClient` errors | `packages/solo/solofun.go`, `clients/iscmove/iscmoveclient/iscmoveclienttest/setup.go` | New gRPC client construction discarded the error (`_ =`); now handled (`require.NoError` / `panic` with context). |
| Unguarded `grpc://` prefix slicing | `packages/solo/solo.go`, `solofun.go`, `tools/cluster/cluster.go`, `iscmoveclienttest/setup.go` | New gRPC-URL parsing used `url[len("grpc://"):]`, which panics on a malformed URL. Replaced with `strings.HasPrefix` guard + `strings.TrimPrefix`. |
| DEBUG-to-stderr from library code | `clients/iscmove/iscmoveclient/grpc_client.go` | Removed the `os.Getenv("DEBUG")` stderr block added with the new gRPC client. |

### ◻️ Out of scope — pre-existing issues noticed but NOT changed here

Found during review but unrelated to the gRPC migration (present identically in
the pre-PR baseline / upstream). Left for separate PRs to keep this diff focused:

- **`PublishTX` deadlock** (`nodeconn.go`) — unbuffered `publishTxQueue` send isn't ctx-guarded; blocks forever if `postTxLoop` already exited. Identical in upstream.
- **`time.Unix` ms-as-seconds** (`parameters/fetcher.go:69`) — `shouldFetch` treats a ms timestamp as seconds → `L1Params` effectively cached forever after first fetch. Pre-existing; fix is `time.UnixMilli`.
- **`ClusterStart` unreachable code** (`l1starter.go`) — dead statements after `panic` (`go vet`). Pre-existing.
- **`container.Start()` unchecked error** (`local_node.go`) — masks the real startup failure as a misleading `port "9000" not found` panic. Pre-existing.
- **`GetEpochInfo` swallows `decodeSystemStateBCS` error** — the PR author intentionally ignores it ("best-effort; missing fields stay 0"). A `WarnLog` would help observability but second-guesses a documented choice; defer.
- **Hardcoded `CheckpointInclusionTimeoutMs = 5000`** (`grpc_client.go`) — a timeout does not prove the TX failed; treating it as failure + retry can double-publish. The real fix is publish/retry idempotency (consensus-adjacent), not a config knob; defer.
- **`feed.go` bare channel sends** (`consumeRequestEvents` / `subscribeToAnchorUpdates`) — `requests <- …` / `anchorCh <- …` are not ctx-guarded, so the goroutine can leak if the consumer stops on shutdown. **Byte-identical in develop and the pre-PR base** (the migration carried the pattern over from the WebSocket version); upstream has never fixed it. Pre-existing; left as-is.

### ⏸️ Deferred follow-ups (gRPC-related, but bigger than this PR)

1. **Checkpoint cursor persistence** — `StreamClient` does not resume from the last processed checkpoint on reconnect; events during an outage are not replayed (same behavior as the old WebSocket code). Feature work.
2. **`ConsensusL1InfoProposal` panics** — the goroutine panics on any L1/gRPC error during consensus. Confirmed pre-existing (identical in upstream). The proper fix changes the `cons_gr.NodeConnL1Info` channel contract — architectural, separate PR.

### ✅ Intentional behavior change (analyzed — safe, NOT a follow-up)

- **Anchor updates now deliver the latest anchor, not the per-checkpoint version.** The old WebSocket path fetched the exact per-transaction anchor (`TryGetPastObject` at the tx's version); `event_listener.SubscribeAnchorUpdates` now fetches the latest anchor (`GetAnchorFromObjectID`), so rapid successive mutations collapse to the newest confirmed tip and intermediate state indices aren't delivered individually. **Analyzed as safe**: the chain only ever acts on the latest confirmed L1 tip (cmt_log `VarLocalView`/`VarConsInsts` keep only the newest confirmed anchor and discard anything below), and the content of any skipped intermediate L2 blocks is back-filled by the state manager (`ChainFetchStateDiff` walks `PreviousL1Commitment` to the common ancestor + P2P `GetBlock`). No liveness/safety impact — collapsing to the tip is the desired BFT outcome. Rationale documented inline in `event_listener.go`.

## 10. Cluster-test enablement (`TestClusterMultiNodeCommittee` green on Alphanet)

Running the cluster tests surfaced one real migration gap (TLS) plus
pre-existing base-version gaps in the test harness. With the fixes below,
`TestClusterMultiNodeCommittee` passes **12/12 subtests against Alphanet**
(~167s), exercising the full node path over gRPC: initial sync, checkpoint
streams, consensus, EVM JSON-RPC, off-ledger requests.

### 🔴 Migration gap: gRPC client had no TLS support

`iotagrpc.NewClient` and the checkpoint `StreamClient` always dialed with
`insecure.NewCredentials()`. Public gRPC endpoints (e.g.
`grpc://grpc.alphanet.iota.cafe`) are TLS-terminated (TLS 1.3, ALPN h2), so a
plaintext dial got an HTTP error back from the load balancer, surfacing as
`rpc error: code = Unimplemented … 404 … text/plain` on every call
(`GetEpochInfo` during `WaitUntilInitiallySynced`, all streams).

Fix: `transportCredentials(address)` in `grpc_client.go`, used by both dial
sites — loopback hosts (`localhost`, `127.0.0.0/8`, `::1`) keep plaintext (the
local test node), anything else dials TLS with system root CAs.

**Documented trade-off:** a deployment pointing the node at a *plaintext* gRPC
endpoint on a non-loopback address (LAN IP, docker service name) would now
attempt TLS and fail to connect. No such configuration exists in this repo
(`config_defaults.json` → `grpc://localhost:50051`; `config.json` /
`test/config.json` → `grpc://grpc.alphanet.iota.cafe`). Code-level callers can
override via explicit `grpc.DialOption`s; if plaintext-over-LAN deployments
are required, a config knob (or a `grpc://`/`grpcs://` scheme split) is a
follow-up.

### 🟠 Cluster wiring: node gRPC URL was derived from the JSON-RPC API URL

`cluster.NewConfig` built each node's `l1.grpcURL` as
`"grpc://" + apiURL.Host`, and the cluster tests did not set
`L1EndpointConfig.GrpcURL` at all. The gRPC endpoint is a **different host**
(`grpc.alphanet.iota.cafe` vs `api.alphanet.iota.cafe`), not the API host on
another port, so wasp nodes spoke gRPC at an HTTP server. Fixed:
`NewConfig` now uses `l1Config.GrpcURL()` verbatim (`tools/cluster/config.go`),
and the tests pass `GrpcURL: iotaconn.AlphanetGrpcEndpointURL`
(`tools/cluster/tests/cluster.go`).

### ◻️ Base-version gaps in the cluster test harness (test-only)

Verified via git: the gRPC PR never touched these files; both bugs exist in
the `v2.0.3` fork base, where production code had already moved to the
one-chain-per-node model but the cluster harness had not. develop fixed both
later; the fixes below replicate develop's approach without rebasing:

1. **`"too many active chain records"` (500) from the second subtest on** —
   `createTestWrapper` deployed a fresh chain per subtest, but the registry
   guard allows a single chain record per node and there is no delete API
   (deactivation keeps the record). Now deploys one chain shared by all
   subtests (develop's model); each subtest still gets its own `ChainEnv`.
   `packages/registry/chain_registry.go` (identical to develop) is untouched.
2. **EVM JSON-RPC 404** — test helpers built the old multi-chain URL
   `/v1/chains/{chainID}/evm`; the actual route is `/v1/chain/evm` (single
   chain per node). Fixed in `env.go::NewEVMJSONRPClient` (chainID param
   dropped, callers updated) and `evm_jsonrpc_test.go::newClusterTestEnv`.

Other tests in `tools/cluster/tests` were not touched; several have their own
base-version gaps (and some are explicitly skipped upstream).
