// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

package iscmoveclient

// client_shared.go contains package-level helper functions for ISC-specific read
// operations (requests, anchors, coins) that are shared between *GRPCClient and
// *Client. Both types satisfy iscFetcher and can delegate to these helpers.

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"golang.org/x/exp/maps"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/clients/iscmove"
	"github.com/iotaledger/wasp/packages/cryptolib"
)

// iscFetcher is the minimal interface required by the shared ISC helpers.
// Both *GRPCClient and *Client satisfy it.
type iscFetcher interface {
	GetObject(ctx context.Context, req iotaclient.GetObjectRequest) (*iotajsonrpc.IotaObjectResponse, error)
	GetOwnedObjects(ctx context.Context, req iotaclient.GetOwnedObjectsRequest) (*iotajsonrpc.ObjectsPage, error)
	GetAssetsBagWithBalances(ctx context.Context, assetsBagID *iotago.ObjectID) (*iscmove.AssetsBagWithBalances, error)
}

// getAnchorFromObjectID fetches and decodes an anchor using any iscFetcher.
// Works with both gRPC (GRPCClient) and JSON-RPC (Client) transports.
func getAnchorFromObjectID(ctx context.Context, f iscFetcher, anchorObjectID *iotago.ObjectID) (*iscmove.AnchorWithRef, error) {
	resp, err := f.GetObject(ctx, iotaclient.GetObjectRequest{
		ObjectID: anchorObjectID,
		Options:  &iotajsonrpc.IotaObjectDataOptions{ShowBcs: true, ShowOwner: true},
	})
	if err != nil {
		return nil, fmt.Errorf("GetAnchorFromObjectID: %w", err)
	}
	if resp.Data == nil {
		return nil, fmt.Errorf("GetAnchorFromObjectID: object %s not found", *anchorObjectID)
	}
	ref := resp.Data.Ref()
	var owner *iotago.Address
	if resp.Data.Owner != nil && resp.Data.Owner.ObjectOwnerInternal != nil {
		owner = resp.Data.Owner.ObjectOwnerInternal.AddressOwner
	}
	return decodeAnchorBCS(resp.Data.Bcs.Data.MoveObject.BcsBytes, ref, owner)
}

// getCoin fetches and decodes a MoveCoin using any iscFetcher.
func getCoin(ctx context.Context, f iscFetcher, coinID *iotago.ObjectID) (*MoveCoin, error) {
	resp, err := f.GetObject(ctx, iotaclient.GetObjectRequest{
		ObjectID: coinID,
		Options:  &iotajsonrpc.IotaObjectDataOptions{ShowBcs: true},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call GetObject: %w", err)
	}
	var moveCoin MoveCoin
	if err := iotaclient.UnmarshalBCS(resp.Data.Bcs.Data.MoveObject.BcsBytes, &moveCoin); err != nil {
		return nil, fmt.Errorf("failed to unmarshal MoveCoin: %w", err)
	}
	return &moveCoin, nil
}

// getRequestFromObjectID fetches and decodes a single Request by ID.
func getRequestFromObjectID(ctx context.Context, f iscFetcher, reqID *iotago.ObjectID) (*iscmove.RefWithObject[iscmove.Request], error) {
	resp, err := f.GetObject(ctx, iotaclient.GetObjectRequest{
		ObjectID: reqID,
		Options:  &iotajsonrpc.IotaObjectDataOptions{ShowBcs: true, ShowOwner: true},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get request content: %w", err)
	}
	if resp.Data == nil {
		return nil, fmt.Errorf("request %s not found", *reqID)
	}
	return parseRequestAndFetchAssetsBag(ctx, f, resp.Data)
}

// parseRequestAndFetchAssetsBag decodes a Request object and enriches it with AssetsBag balances.
func parseRequestAndFetchAssetsBag(ctx context.Context, f iscFetcher, obj *iotajsonrpc.IotaObjectData) (*iscmove.RefWithObject[iscmove.Request], error) {
	type intermediateMoveRequest struct {
		ID        iotago.ObjectID
		Sender    *cryptolib.Address
		AssetsBag iscmove.Referent[iscmove.AssetsBag]
		Message   iscmove.Message
		Allowance []byte
		GasBudget uint64
	}

	var intermediateRequest intermediateMoveRequest
	if err := iotaclient.UnmarshalBCS(obj.Bcs.Data.MoveObject.BcsBytes, &intermediateRequest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BCS: %w", err)
	}
	bals, err := f.GetAssetsBagWithBalances(ctx, &intermediateRequest.AssetsBag.Value.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch AssetsBag of Request: %w", err)
	}

	req := MoveRequest{
		ID:     intermediateRequest.ID,
		Sender: intermediateRequest.Sender,
		AssetsBag: iscmove.Referent[iscmove.AssetsBagWithBalances]{
			ID:    intermediateRequest.AssetsBag.ID,
			Value: bals,
		},
		Message:   intermediateRequest.Message,
		Allowance: intermediateRequest.Allowance,
		GasBudget: intermediateRequest.GasBudget,
	}

	return &iscmove.RefWithObject[iscmove.Request]{
		ObjectRef: obj.Ref(),
		Object:    req.ToRequest(),
		Owner:     obj.Owner.AddressOwner,
	}, nil
}

// pullRequests pages through GetOwnedObjects to collect up to maxAmountOfRequests Request objects.
func pullRequests(ctx context.Context, f iscFetcher, packageID iotago.Address, anchorAddress *iotago.ObjectID, maxAmountOfRequests int) (map[iotago.ObjectID]*iotajsonrpc.IotaObjectData, error) {
	pulledRequests := make(map[iotago.ObjectID]*iotajsonrpc.IotaObjectData, maxAmountOfRequests)

	query := &iotajsonrpc.IotaObjectResponseQuery{
		Filter: &iotajsonrpc.IotaObjectDataFilter{
			StructType: &iotago.StructTag{
				Address: &packageID,
				Module:  iscmove.RequestModuleName,
				Name:    iscmove.RequestObjectName,
			},
		},
		Options: &iotajsonrpc.IotaObjectDataOptions{
			ShowBcs:   true,
			ShowOwner: true,
		},
	}

	var cursor *iotago.ObjectID
	for len(pulledRequests) < maxAmountOfRequests {
		objs, err := f.GetOwnedObjects(ctx, iotaclient.GetOwnedObjectsRequest{
			Address: anchorAddress,
			Query:   query,
			Cursor:  cursor,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to fetch requests: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context error while fetching requests: %w", err)
		}
		if objs == nil || len(objs.Data) == 0 {
			break
		}
		for _, req := range objs.Data {
			if req.Data == nil || req.Data.ObjectID == nil {
				continue
			}
			pulledRequests[*req.Data.ObjectID] = req.Data
			if len(pulledRequests) >= maxAmountOfRequests {
				break
			}
		}
		if objs.NextCursor == nil {
			break
		}
		cursor = objs.NextCursor
	}

	return pulledRequests, nil
}

// getRequestsSorted fetches up to maxAmountOfRequests requests sorted by ObjectID,
// calling cb for each decoded request.
func getRequestsSorted(ctx context.Context, f iscFetcher, packageID iotago.PackageID, anchorAddress *iotago.ObjectID, maxAmountOfRequests int, cb func(error, *iscmove.RefWithObject[iscmove.Request])) error {
	pulled, err := pullRequests(ctx, f, packageID, anchorAddress, maxAmountOfRequests)
	if err != nil {
		return err
	}

	keys := maps.Keys(pulled)
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i][:], keys[j][:]) < 0
	})

	var sorted []iotago.ObjectID
	if len(keys) >= maxAmountOfRequests {
		sorted = keys[:maxAmountOfRequests]
	} else {
		sorted = keys
	}

	for _, reqID := range sorted {
		ref, err := parseRequestAndFetchAssetsBag(ctx, f, pulled[reqID])
		cb(err, ref)
	}
	return nil
}

// getRequests fetches all requests (unsorted) up to maxAmountOfRequests.
func getRequests(ctx context.Context, f iscFetcher, packageID iotago.PackageID, anchorAddress *iotago.ObjectID, maxAmountOfRequests int) ([]*iscmove.RefWithObject[iscmove.Request], error) {
	pulled, err := pullRequests(ctx, f, packageID, anchorAddress, maxAmountOfRequests)
	if err != nil {
		return nil, err
	}

	result := make([]*iscmove.RefWithObject[iscmove.Request], 0, len(pulled))
	for _, data := range pulled {
		req, err := parseRequestAndFetchAssetsBag(ctx, f, data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode request: %w", err)
		}
		result = append(result, req)
	}
	return result, nil
}
