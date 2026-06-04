// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

package iscmoveclient

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/iotaledger/bcs-go"
	"github.com/iotaledger/hive.go/log"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotagrpc"
	filter_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/filter"
	ledger_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/ledger_service"
	types_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/types"
	"github.com/iotaledger/wasp/clients/iscmove"
)

const eventBufferSize = 64

// EventListener abstracts over WebSocket and gRPC event sources.
// Both implementations deliver the same high-level types so that
// feed.go does not need to know about the underlying transport.
type EventListener interface {
	// SubscribeEvents returns a channel of decoded ISC RequestEvents.
	SubscribeEvents(ctx context.Context) (<-chan iscmove.RequestEvent, error)
	// SubscribeAnchorUpdates returns a channel of fully-fetched AnchorWithRef
	// objects whenever the anchor on L1 is mutated.
	SubscribeAnchorUpdates(ctx context.Context) (<-chan *iscmove.AnchorWithRef, error)
	WaitUntilStopped()
}

// selectEventClient creates a gRPC-backed EventListener for the given grpcURL.
func selectEventClient(
	log log.Logger,
	grpcURL string,
	iscPackageID iotago.PackageID,
	anchorID iotago.ObjectID,
	httpClient NodeL1Client,
) (EventListener, error) {
	if !strings.HasPrefix(grpcURL, "grpc://") && !strings.HasPrefix(grpcURL, "grpcs://") {
		return nil, fmt.Errorf("unsupported URL: %q (must start with grpc:// or grpcs://)", grpcURL)
	}
	// Pass the full URL through: the stream client derives transport security
	// (plaintext vs TLS) from the scheme.
	return NewGRpcClientWrapper(log, grpcURL, iscPackageID, anchorID, httpClient), nil
}

// ── gRPC wrapper ─────────────────────────────────────────────────────────────

// GRpcClientWrapper implements EventListener using LedgerService.StreamCheckpoints.
type GRpcClientWrapper struct {
	grpcAddress   string
	iscPackageID  iotago.PackageID
	anchorAddress iotago.ObjectID
	httpClient    NodeL1Client
	log           log.Logger
	wg            sync.WaitGroup
}

func NewGRpcClientWrapper(
	log log.Logger,
	grpcAddress string,
	iscPackageID iotago.PackageID,
	anchorAddress iotago.ObjectID,
	httpClient NodeL1Client,
) EventListener {
	return &GRpcClientWrapper{
		grpcAddress:   grpcAddress,
		iscPackageID:  iscPackageID,
		anchorAddress: anchorAddress,
		httpClient:    httpClient,
		log:           log,
	}
}

// SubscribeEvents streams ISC RequestEvents via StreamCheckpoints filtered by
// the ISC request Move event struct tag.
func (g *GRpcClientWrapper) SubscribeEvents(ctx context.Context) (<-chan iscmove.RequestEvent, error) {
	// Struct tag: "0x{packageHex}::request::RequestEvent"
	structTag := fmt.Sprintf("0x%x::%s::%s",
		g.iscPackageID[:],
		iscmove.RequestModuleName,
		iscmove.RequestEventObjectName,
	)

	req := &ledger_pb.StreamCheckpointsRequest{
		ReadMask: &fieldmaskpb.FieldMask{Paths: []string{"events"}},
		EventsFilter: &filter_pb.EventFilter{
			Filter: &filter_pb.EventFilter_MoveEventType{
				MoveEventType: &filter_pb.MoveEventTypeFilter{
					StructTag: structTag,
				},
			},
		},
		FilterCheckpoints: boolPtr(true),
	}

	raw := iotagrpc.NewCheckpointStreamClient(g.grpcAddress, req, g.log).Start(ctx)

	out := make(chan iscmove.RequestEvent, eventBufferSize)
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case cp, ok := <-raw:
				if !ok {
					return
				}
				evts := cp.GetEvents()
				if evts == nil {
					continue
				}
				for _, ev := range evts.GetEvents() {
					bcsBytes := ev.GetBcsContents().GetData()
					if len(bcsBytes) == 0 {
						g.log.LogWarnf("grpc event: empty bcs_contents, skipping")
						continue
					}
					reqEvent, err := bcs.Unmarshal[iscmove.RequestEvent](bcsBytes)
					if err != nil {
						g.log.LogErrorf("grpc event: failed to unmarshal RequestEvent: %v", err)
						continue
					}
					select {
					case out <- reqEvent:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, nil
}

// SubscribeAnchorUpdates streams anchor mutations via StreamCheckpoints filtered
// by transactions that affect the anchor object, then fetches the full anchor via HTTP.
func (g *GRpcClientWrapper) SubscribeAnchorUpdates(ctx context.Context) (<-chan *iscmove.AnchorWithRef, error) {
	req := &ledger_pb.StreamCheckpointsRequest{
		ReadMask: &fieldmaskpb.FieldMask{Paths: []string{"transactions"}},
		TransactionsFilter: &filter_pb.TransactionFilter{
			Filter: &filter_pb.TransactionFilter_AffectedObject{
				AffectedObject: &filter_pb.ObjectIdFilter{
					ObjectRef: &types_pb.ObjectReference{
						ObjectId: &types_pb.ObjectId{
							ObjectId: g.anchorAddress[:],
						},
					},
				},
			},
		},
		FilterCheckpoints: boolPtr(true),
	}

	raw := iotagrpc.NewCheckpointStreamClient(g.grpcAddress, req, g.log).Start(ctx)

	out := make(chan *iscmove.AnchorWithRef, eventBufferSize)
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case cp, ok := <-raw:
				if !ok {
					return
				}
				if cp.GetExecutedTransactions() == nil {
					continue
				}
				// At least one tx in this checkpoint touched the anchor.
				// Fetch the current anchor state via HTTP (gRPC Object has no owner field).
				//
				// NOTE (gRPC migration — behavior change vs the old WebSocket path):
				// the old path fetched the EXACT per-transaction anchor version
				// (TryGetPastObject at the tx's anchor version), so every mutation was
				// delivered at its own state index. Here we fetch the LATEST anchor, so
				// when several anchor mutations land in quick succession the notifications
				// collapse to the most recent on-chain anchor and intermediate state
				// indices are not delivered individually.
				//
				// This is intentional and safe for our model: the chain only ever acts on
				// the latest confirmed L1 tip — cmt_log's VarLocalView/VarConsInsts keep
				// only the newest confirmed anchor and discard anything below it — and the
				// content of any skipped intermediate L2 blocks is back-filled by the state
				// manager (ChainFetchStateDiff walks PreviousL1Commitment to the common
				// ancestor and fetches missing blocks over P2P). So collapsing to the tip is
				// the desired BFT outcome, not a lost update.
				anchor, err := g.httpClient.GetAnchorFromObjectID(ctx, &g.anchorAddress)
				if err != nil {
					g.log.LogErrorf("grpc anchor update: failed to fetch anchor: %v", err)
					continue
				}
				g.log.LogDebugf("grpc anchor update: anchor %s fetched at %s", g.anchorAddress, time.Now())
				select {
				case out <- anchor:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (g *GRpcClientWrapper) WaitUntilStopped() {
	g.wg.Wait()
}

func boolPtr(b bool) *bool { return &b }
