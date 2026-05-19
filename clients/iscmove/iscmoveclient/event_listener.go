// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

package iscmoveclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/iotaledger/bcs-go"
	"github.com/iotaledger/hive.go/log"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotago/serialization"
	"github.com/iotaledger/wasp/clients/iota-go/iotagrpc"
	filter_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/filter"
	ledger_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/ledger_service"
	types_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/types"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
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

// selectEventClient picks the right EventListener based on the URL prefix.
func selectEventClient(
	log log.Logger,
	socketURL string,
	iscPackageID iotago.PackageID,
	anchorID iotago.ObjectID,
	httpClient *Client,
) (EventListener, error) {
	switch {
	case len(socketURL) >= 7 && socketURL[:7] == "grpc://":
		addr := socketURL[7:]
		return NewGRpcClientWrapper(log, addr, iscPackageID, anchorID, httpClient), nil
	case len(socketURL) >= 5 && (socketURL[:5] == "ws://" || socketURL[:6] == "wss://"):
		return NewWebSocketClientWrapper(log, socketURL, iscPackageID, anchorID, httpClient), nil
	default:
		return nil, fmt.Errorf("unsupported socket URL: %q (use grpc://, ws://, or wss://)", socketURL)
	}
}

// ── gRPC wrapper ─────────────────────────────────────────────────────────────

// GRpcClientWrapper implements EventListener using LedgerService.StreamCheckpoints.
type GRpcClientWrapper struct {
	grpcAddress   string
	iscPackageID  iotago.PackageID
	anchorAddress iotago.ObjectID
	httpClient    *Client
	log           log.Logger
	wg            sync.WaitGroup
}

func NewGRpcClientWrapper(
	log log.Logger,
	grpcAddress string,
	iscPackageID iotago.PackageID,
	anchorAddress iotago.ObjectID,
	httpClient *Client,
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

// ── WebSocket wrapper ─────────────────────────────────────────────────────────

// WebSocketClientWrapper implements EventListener using the existing JSON-RPC
// WebSocket subscriptions (iotax_subscribeEvent / iotax_subscribeTransaction).
type WebSocketClientWrapper struct {
	wsURL         string
	iscPackageID  iotago.PackageID
	anchorAddress iotago.ObjectID
	httpClient    *Client
	log           log.Logger
	wg            sync.WaitGroup
}

func NewWebSocketClientWrapper(
	log log.Logger,
	wsURL string,
	iscPackageID iotago.PackageID,
	anchorAddress iotago.ObjectID,
	httpClient *Client,
) EventListener {
	return &WebSocketClientWrapper{
		wsURL:         wsURL,
		iscPackageID:  iscPackageID,
		anchorAddress: anchorAddress,
		httpClient:    httpClient,
		log:           log,
	}
}

func (w *WebSocketClientWrapper) SubscribeEvents(ctx context.Context) (<-chan iscmove.RequestEvent, error) {
	wsClient, err := NewWebsocketClient(ctx, w.wsURL, "", iotaclient.WaitForEffectsEnabled, w.log)
	if err != nil {
		return nil, err
	}

	eventFilter := &iotajsonrpc.EventFilter{
		And: &iotajsonrpc.AndOrEventFilter{
			Filter1: &iotajsonrpc.EventFilter{MoveEventType: &iotago.StructTag{
				Address: &w.iscPackageID,
				Module:  iscmove.RequestModuleName,
				Name:    iscmove.RequestEventObjectName,
			}},
			Filter2: &iotajsonrpc.EventFilter{MoveEventField: &iotajsonrpc.EventFilterMoveEventField{
				Path:  iscmove.RequestEventAnchorFieldName,
				Value: w.anchorAddress.String(),
			}},
		},
	}

	rawEvents := make(chan *iotajsonrpc.IotaEvent, eventBufferSize)
	if err := wsClient.SubscribeEvent(ctx, eventFilter, rawEvents); err != nil {
		close(rawEvents)
		return nil, err
	}

	out := make(chan iscmove.RequestEvent, eventBufferSize)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-rawEvents:
				if !ok {
					return
				}
				reqEvent, err := bcs.Unmarshal[iscmove.RequestEvent](ev.Bcs)
				if err != nil {
					w.log.LogErrorf("ws event: failed to unmarshal RequestEvent: %v", err)
					continue
				}
				select {
				case out <- reqEvent:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (w *WebSocketClientWrapper) SubscribeAnchorUpdates(ctx context.Context) (<-chan *iscmove.AnchorWithRef, error) {
	wsClient, err := NewWebsocketClient(ctx, w.wsURL, "", iotaclient.WaitForEffectsEnabled, w.log)
	if err != nil {
		return nil, err
	}

	txFilter := &iotajsonrpc.TransactionFilter{ChangedObject: &w.anchorAddress}

	rawTxs := make(chan *serialization.TagJson[iotajsonrpc.IotaTransactionBlockEffects], eventBufferSize)
	if err := wsClient.SubscribeTransaction(ctx, txFilter, rawTxs); err != nil {
		close(rawTxs)
		return nil, err
	}

	out := make(chan *iscmove.AnchorWithRef, eventBufferSize)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case change, ok := <-rawTxs:
				if !ok {
					return
				}
				if change.Data.V1 == nil {
					continue
				}
				for _, obj := range change.Data.V1.Mutated {
					if *obj.Reference.ObjectID != w.anchorAddress {
						continue
					}
					anchor, err := w.httpClient.GetPastAnchorFromObjectID(ctx, &w.anchorAddress, obj.Reference.Version)
					if err != nil {
						w.log.LogErrorf("ws anchor update: failed to fetch anchor v%d: %v", obj.Reference.Version, err)
						continue
					}
					w.log.LogDebugf("ws anchor update: anchor %s v%d fetched", w.anchorAddress, obj.Reference.Version)
					select {
					case out <- anchor:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, nil
}

func (w *WebSocketClientWrapper) WaitUntilStopped() {
	w.wg.Wait()
}

func boolPtr(b bool) *bool { return &b }
