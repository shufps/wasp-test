package parameters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/iotaledger/hive.go/log"
	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotagrpc"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/packages/coin"
)

// L1ParamsFetcher provides the latest version of L1Params, and
// automatically refreshes it when the epoch is out of date
type L1ParamsFetcher interface {
	GetOrFetchLatest(ctx context.Context) (*L1Params, error)
}

type l1ParamsFetcher struct {
	grpcClient *iotagrpc.Client
	log        log.Logger
	mu         sync.Mutex
	latest     *L1Params
}

// NewL1ParamsFetcher creates a new L1ParamsFetcher backed by gRPC.
func NewL1ParamsFetcher(grpcClient *iotagrpc.Client, log log.Logger) L1ParamsFetcher {
	return &l1ParamsFetcher{
		grpcClient: grpcClient,
		log:        log.NewChildLogger("L1ParamsFetcher"),
	}
}

// GetOrFetchLatest returns the latest L1Params, or fetches it if necessary
func (f *l1ParamsFetcher) GetOrFetchLatest(ctx context.Context) (*L1Params, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.shouldFetch() {
		f.log.LogInfo("Fetching latest L1Params...")
		latest, err := FetchLatestGRPC(ctx, f.grpcClient)
		if err != nil {
			f.log.LogError("Failed to fetch latest L1Params", err)
			return nil, err
		}
		f.log.LogInfof("Fetched L1Params: epoch=%d protocolVersion=%d systemStateVersion=%d referenceGasPrice=%d epochStartTimestampMs=%d epochDurationMs=%d totalSupply=%d",
			latest.Protocol.Epoch.Int64(),
			latest.Protocol.ProtocolVersion.Int64(),
			latest.Protocol.SystemStateVersion.Int64(),
			latest.Protocol.ReferenceGasPrice.Int64(),
			latest.Protocol.EpochStartTimestampMs.Int64(),
			latest.Protocol.EpochDurationMs.Int64(),
			latest.BaseToken.TotalSupply,
		)
		f.latest = latest
	}

	return f.latest, nil
}

func (f *l1ParamsFetcher) shouldFetch() bool {
	if f.latest == nil {
		return true
	}
	now := time.Now()
	start := time.Unix(f.latest.Protocol.EpochStartTimestampMs.Int64(), 0)
	duration := time.Duration(f.latest.Protocol.EpochDurationMs.Int64()) * time.Millisecond
	return now.After(start.Add(duration))
}

// FetchLatestGRPC fetches the latest L1Params via gRPC, retrying on failure.
func FetchLatestGRPC(ctx context.Context, grpcClient *iotagrpc.Client) (*L1Params, error) {
	return iotaclient.Retry(
		ctx,
		func() (*L1Params, error) {
			epochInfo, err := grpcClient.GetEpochInfo(ctx)
			if err != nil {
				return nil, fmt.Errorf("can't get epoch info: %w", err)
			}
			meta, err := grpcClient.GetCoinMetadata(ctx, iotajsonrpc.IotaCoinType.String())
			if err != nil {
				return nil, fmt.Errorf("can't get coin metadata: %w", err)
			}
			if meta.Decimals != BaseTokenDecimals {
				return nil, fmt.Errorf("unsupported decimals: %d", meta.Decimals)
			}
			totalSupply, err := grpcClient.GetTotalSupply(ctx, iotajsonrpc.IotaCoinType.String())
			if err != nil {
				return nil, fmt.Errorf("can't get total supply: %w", err)
			}

			// Prefer the configured epoch duration decoded from BCS system state.
			// Fall back to deriving it from start/end timestamps if available,
			// and finally to 24h as a last resort.
			epochDurationMs := epochInfo.EpochDurationMs
			if epochDurationMs == 0 && epochInfo.EpochEndMs > 0 && epochInfo.EpochStartMs > 0 {
				epochDurationMs = epochInfo.EpochEndMs - epochInfo.EpochStartMs
			}
			if epochDurationMs == 0 {
				epochDurationMs = int64(24 * 60 * 60 * 1000) // last-resort default 24h
			}

			return &L1Params{
				Protocol: &Protocol{
					Epoch:                 iotajsonrpc.NewBigInt(epochInfo.EpochID),
					ProtocolVersion:       iotajsonrpc.NewBigInt(epochInfo.ProtocolVersion),
					SystemStateVersion:    iotajsonrpc.NewBigInt(epochInfo.SystemStateVersion),
					ReferenceGasPrice:     iotajsonrpc.NewBigInt(epochInfo.ReferenceGasPrice),
					EpochStartTimestampMs: iotajsonrpc.NewBigInt(uint64(epochInfo.EpochStartMs)),
					EpochDurationMs:       iotajsonrpc.NewBigInt(uint64(epochDurationMs)),
				},
				BaseToken: IotaCoinInfoFromL1Metadata(
					coin.BaseTokenType,
					meta,
					coin.Value(totalSupply),
				),
			}, nil
		},
		iotaclient.DefaultRetryCondition[*L1Params](),
		iotaclient.WaitForEffectsEnabled,
	)
}
