package iscmoveclient

import (
	"context"
	"fmt"
	"time"

	"github.com/iotaledger/hive.go/log"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/clients/iscmove"
	"github.com/iotaledger/wasp/packages/transaction"
)

type ChainFeed struct {
	eventClient   EventListener
	httpClient    *Client
	iscPackageID  iotago.PackageID
	anchorAddress iotago.ObjectID
	log           log.Logger
}

func NewChainFeed(
	ctx context.Context,
	iscPackageID iotago.PackageID,
	anchorAddress iotago.ObjectID,
	log log.Logger,
	socketURL string,
	httpClient *Client,
) (*ChainFeed, error) {
	eventClient, err := selectEventClient(log, socketURL, iscPackageID, anchorAddress, httpClient)
	if err != nil {
		return nil, err
	}

	return &ChainFeed{
		eventClient:   eventClient,
		httpClient:    httpClient,
		iscPackageID:  iscPackageID,
		anchorAddress: anchorAddress,
		log:           log.NewChildLogger("iscmove-chainfeed"),
	}, nil
}

func (f *ChainFeed) WaitUntilStopped() {
	f.eventClient.WaitUntilStopped()
}

func (f *ChainFeed) GetCurrentAnchor(ctx context.Context) (*iscmove.AnchorWithRef, error) {
	anchor, err := f.httpClient.GetAnchorFromObjectID(ctx, &f.anchorAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch anchor: %w", err)
	}
	return anchor, err
}

// FetchCurrentState fetches the current Anchor and all Requests owned by the
// anchor address.
func (f *ChainFeed) FetchCurrentState(ctx context.Context, maxAmountOfRequests int, requestCb func(error, *iscmove.RefWithObject[iscmove.Request])) (*iscmove.AnchorWithRef, error) {
	anchor, err := f.GetCurrentAnchor(ctx)
	if err != nil {
		return nil, err
	}

	// This was refactored from a return based function to a callback based one, as pulling many requests takes
	// a lot of time (~5-10 requests per second) and it would halt ISC on start up, until all requests are pulled.
	// This gives us the option to run this call in a separate goroutine.
	// During my testing I found, that just adding `go` in front of it, isn't enough, and it requires further synchronization from the caller.
	// I kept it as a callback based function for now, as pulling the requests needs improvement and it seems to be the way to go.
	err = f.httpClient.GetRequestsSorted(ctx, f.iscPackageID, &f.anchorAddress, maxAmountOfRequests, requestCb)

	return anchor, err
}

// SubscribeToUpdates starts fetching updated versions of the Anchor and newly received requests in background.
func (f *ChainFeed) SubscribeToUpdates(
	ctx context.Context,
	anchorID iotago.ObjectID,
	anchorCh chan<- *iscmove.AnchorWithRef,
	requestsCh chan<- *iscmove.RefWithObject[iscmove.Request],
) {
	go f.subscribeToAnchorUpdates(ctx, anchorCh)
	go f.subscribeToNewRequests(ctx, anchorID, requestsCh)
}

func (f *ChainFeed) subscribeToNewRequests(
	ctx context.Context,
	anchorID iotago.ObjectID,
	requests chan<- *iscmove.RefWithObject[iscmove.Request],
) {
	events, err := f.eventClient.SubscribeEvents(ctx)
	if err != nil {
		f.log.LogErrorf("subscribeToNewRequests: failed to subscribe: %s", err)
		return
	}
	f.consumeRequestEvents(ctx, events, requests, anchorID)
}

func (f *ChainFeed) consumeRequestEvents(
	ctx context.Context,
	events <-chan iscmove.RequestEvent,
	requests chan<- *iscmove.RefWithObject[iscmove.Request],
	anchorID iotago.ObjectID,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case reqEvent, ok := <-events:
			if !ok {
				return
			}

			// Drop requests that don't belong to our anchor.
			// (gRPC cannot filter by event field value, so we filter client-side.)
			if reqEvent.Anchor != anchorID {
				continue
			}

			reqWithObj, err := f.httpClient.GetRequestFromObjectID(ctx, &reqEvent.RequestID)
			if err != nil {
				f.log.LogErrorf("consumeRequestEvents: cannot fetch Request: %s", err)
				continue
			}

			requests <- reqWithObj
			f.log.LogDebugf("REQUEST[%s] SENT TO CHANNEL %s\n", reqEvent.RequestID.String(), time.Now().String())
		}
	}
}

func (f *ChainFeed) subscribeToAnchorUpdates(
	ctx context.Context,
	anchorCh chan<- *iscmove.AnchorWithRef,
) {
	anchors, err := f.eventClient.SubscribeAnchorUpdates(ctx)
	if err != nil {
		f.log.LogErrorf("subscribeToAnchorUpdates: failed to subscribe: %s", err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case anchor, ok := <-anchors:
			if !ok {
				return
			}
			f.log.LogDebugf("ANCHOR[%s] SENT TO CHANNEL %s\n", anchor.Object.ID.String(), time.Now().String())
			anchorCh <- anchor
		}
	}
}

func (f *ChainFeed) GetISCPackageID() iotago.PackageID {
	return f.iscPackageID
}

func (f *ChainFeed) GetChainGasCoin(ctx context.Context) (*iotago.ObjectRef, uint64, error) {
	anchor, err := f.httpClient.GetAnchorFromObjectID(ctx, &f.anchorAddress)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch anchor: %w", err)
	}
	metadata, err := transaction.StateMetadataFromBytes(anchor.Object.StateMetadata)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch anchor: %w", err)
	}
	getObjRes, err := f.httpClient.GetObject(ctx, iotaclient.GetObjectRequest{
		ObjectID: metadata.GasCoinObjectID,
		Options:  &iotajsonrpc.IotaObjectDataOptions{ShowBcs: true},
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch gas coin object: %w", err)
	}
	var moveGasCoin MoveCoin
	err = iotaclient.UnmarshalBCS(getObjRes.Data.Bcs.Data.MoveObject.BcsBytes, &moveGasCoin)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode gas coin object: %w", err)
	}
	gasCoinRef := getObjRes.Data.Ref()
	return &gasCoinRef, moveGasCoin.Balance, nil
}
