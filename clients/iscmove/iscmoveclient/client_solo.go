package iscmoveclient

import (
	"context"
	"fmt"

	"github.com/iotaledger/wasp/clients/iota-go/iotagrpc"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/clients/iscmove"
)

// SoloClient is the HTTP/JSON-RPC client used by solo and integration tests.
// It extends CLIClient with a gRPC backend for operations that require it
// (GetAssetsBagWithBalances, GetPastAnchorFromObjectID).
type SoloClient struct {
	*CLIClient
	grpcClient *iotagrpc.Client
}

// NewSoloClient wraps an existing CLIClient with a gRPC client.
// grpcClient may be nil if gRPC operations are not needed.
func NewSoloClient(httpClient *CLIClient, grpcClient *iotagrpc.Client) *SoloClient {
	return &SoloClient{
		CLIClient:  httpClient,
		grpcClient: grpcClient,
	}
}

// GetAssetsBagWithBalances fetches an AssetsBag and its coin/object balances via gRPC.
func (c *SoloClient) GetAssetsBagWithBalances(ctx context.Context, assetsBagID *iotago.ObjectID) (*iscmove.AssetsBagWithBalances, error) {
	if c.grpcClient == nil {
		panic("SoloClient.GetAssetsBagWithBalances: requires gRPC client — pass a non-nil grpcClient to NewSoloClient")
	}
	result, err := NewGRPCClient(c.grpcClient).GetAssetsBagWithBalances(ctx, assetsBagID)
	if err != nil {
		return nil, fmt.Errorf("GetAssetsBagWithBalances: %w", err)
	}
	return result, nil
}

// GetPastAnchorFromObjectID fetches the anchor at a specific version via gRPC.
func (c *SoloClient) GetPastAnchorFromObjectID(ctx context.Context, anchorObjectID *iotago.ObjectID, version uint64) (*iscmove.AnchorWithRef, error) {
	if c.grpcClient == nil {
		panic("SoloClient.GetPastAnchorFromObjectID: requires gRPC client — pass a non-nil grpcClient to NewSoloClient")
	}
	od, err := c.grpcClient.GetObjectBCS(ctx, anchorObjectID, &version)
	if err != nil {
		return nil, fmt.Errorf("GetPastAnchorFromObjectID: %w", err)
	}
	ref := iotago.ObjectRef{
		ObjectID: &od.ObjectID,
		Version:  od.Version,
		Digest:   &od.Digest,
	}
	return decodeAnchorBCS(iotago.Base64Data(od.Contents), ref, od.Owner)
}

// GetRequestFromObjectID passes *SoloClient as iscFetcher so GetAssetsBagWithBalances works.
func (c *SoloClient) GetRequestFromObjectID(ctx context.Context, reqID *iotago.ObjectID) (*iscmove.RefWithObject[iscmove.Request], error) {
	return getRequestFromObjectID(ctx, c, reqID)
}

// GetRequestsSorted passes *SoloClient as iscFetcher so GetAssetsBagWithBalances works.
func (c *SoloClient) GetRequestsSorted(ctx context.Context, packageID iotago.PackageID, anchorAddress *iotago.ObjectID, maxAmountOfRequests int, cb func(error, *iscmove.RefWithObject[iscmove.Request])) error {
	return getRequestsSorted(ctx, c, packageID, anchorAddress, maxAmountOfRequests, cb)
}

// GetRequests passes *SoloClient as iscFetcher so GetAssetsBagWithBalances works.
func (c *SoloClient) GetRequests(ctx context.Context, packageID iotago.PackageID, anchorAddress *iotago.ObjectID, maxAmountOfRequests int) ([]*iscmove.RefWithObject[iscmove.Request], error) {
	return getRequests(ctx, c, packageID, anchorAddress, maxAmountOfRequests)
}

// ReceiveRequestsAndTransition passes *SoloClient as iscFetcher so GetAssetsBagWithBalances works.
func (c *SoloClient) ReceiveRequestsAndTransition(ctx context.Context, req *ReceiveRequestsAndTransitionRequest) (*iotajsonrpc.IotaTransactionBlockResponse, error) {
	return receiveRequestsAndTransitionWith(ctx, c, c.CLIClient, req)
}
