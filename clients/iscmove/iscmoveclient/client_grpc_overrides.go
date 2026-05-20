// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

package iscmoveclient

// This file provides methods on Client that shadow the identically-named
// methods on the embedded *iotaclient.Client, routing all calls to the
// gRPC StateService instead of the indexer-backed iotax_* JSON-RPC calls.
// gRPC is now mandatory; Client.grpcClient must always be set.

import (
	"context"
	"fmt"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotago/serialization"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/clients/iota-go/iotagrpc"
	"github.com/iotaledger/wasp/clients/iota-go/iotasigner"
	"github.com/iotaledger/wasp/clients/iscmove"
)

// GetObject fetches a single object by ID via gRPC.
// Constructs a synthetic IotaObjectResponse from the decoded VersionedObject BCS.
func (c *Client) GetObject(ctx context.Context, req iotaclient.GetObjectRequest) (*iotajsonrpc.IotaObjectResponse, error) {
	od, err := c.grpcClient.GetObjectBCS(ctx, req.ObjectID, nil)
	if err != nil {
		return nil, err
	}
	return objectDataToResponse(od), nil
}

// GetOwnedObjects returns a page of objects owned by the given address via gRPC.
func (c *Client) GetOwnedObjects(ctx context.Context, req iotaclient.GetOwnedObjectsRequest) (*iotajsonrpc.ObjectsPage, error) {
	objectType := ""
	if req.Query != nil && req.Query.Filter != nil && req.Query.Filter.StructType != nil {
		st := req.Query.Filter.StructType
		objectType = fmt.Sprintf("0x%x::%s::%s", st.Address[:], st.Module, st.Name)
	}
	var cursor []byte
	if req.Cursor != nil {
		cursor = req.Cursor[:]
	}
	var limit uint32
	if req.Limit != nil {
		limit = uint32(*req.Limit)
	}
	page, err := c.grpcClient.ListOwnedObjectsPage(ctx, (*iotago.Address)(req.Address), objectType, cursor, limit)
	if err != nil {
		return nil, err
	}
	result := &iotajsonrpc.ObjectsPage{
		HasNextPage: page.HasNextPage,
		NextCursor:  page.NextCursor,
	}
	for _, od := range page.Objects {
		result.Data = append(result.Data, *objectDataToResponse(od))
	}
	return result, nil
}

// objectDataToResponse builds a synthetic IotaObjectResponse from a gRPC ObjectData.
func objectDataToResponse(od *iotagrpc.ObjectData) *iotajsonrpc.IotaObjectResponse {
	objID := od.ObjectID
	digest := od.Digest
	data := &iotajsonrpc.IotaObjectData{
		ObjectID: &objID,
		Version:  iotajsonrpc.NewBigInt(od.Version),
		Digest:   &digest,
		Bcs: &serialization.TagJson[iotajsonrpc.IotaRawData]{
			Data: iotajsonrpc.IotaRawData{
				MoveObject: &iotajsonrpc.IotaRawMoveObject{
					BcsBytes: iotago.Base64Data(od.Contents),
				},
			},
		},
	}
	if od.Owner != nil {
		data.Owner = &iotajsonrpc.ObjectOwner{
			ObjectOwnerInternal: &iotajsonrpc.ObjectOwnerInternal{
				AddressOwner: od.Owner,
			},
		}
	}
	return &iotajsonrpc.IotaObjectResponse{Data: data}
}

// GetCoins returns up to req.Limit coin objects of req.CoinType owned by req.Owner via gRPC.
func (c *Client) GetCoins(ctx context.Context, req iotaclient.GetCoinsRequest) (*iotajsonrpc.CoinPage, error) {
	coinType := iotajsonrpc.IotaCoinType.String()
	if req.CoinType != nil && *req.CoinType != "" {
		coinType = *req.CoinType
	}
	var cursor []byte
	if req.Cursor != nil {
		cursor = req.Cursor[:]
	}
	return c.grpcClient.GetCoins(ctx, req.Owner, coinType, cursor, uint32(req.Limit))
}

// GetAllCoins returns all coin objects owned by req.Owner (paginated) via gRPC.
func (c *Client) GetAllCoins(ctx context.Context, req iotaclient.GetAllCoinsRequest) (*iotajsonrpc.CoinPage, error) {
	var cursor []byte
	if req.Cursor != nil {
		cursor = req.Cursor[:]
	}
	return c.grpcClient.GetAllCoins(ctx, req.Owner, cursor, uint32(req.Limit))
}

// GetCoinObjsForTargetAmount fetches IOTA coins to cover targetAmount+gasAmount via gRPC.
func (c *Client) GetCoinObjsForTargetAmount(
	ctx context.Context,
	address *iotago.Address,
	targetAmount uint64,
	gasAmount uint64,
) (iotajsonrpc.Coins, error) {
	return c.grpcClient.GetCoinObjsForTargetAmount(ctx, address, targetAmount, gasAmount)
}

// GetCoinMetadata returns metadata for coinType via gRPC.
func (c *Client) GetCoinMetadata(ctx context.Context, coinType string) (*iotajsonrpc.IotaCoinMetadata, error) {
	return c.grpcClient.GetCoinMetadata(ctx, coinType)
}

// GetReferenceGasPrice returns the current reference gas price via gRPC.
func (c *Client) GetReferenceGasPrice(ctx context.Context) (*iotajsonrpc.BigInt, error) {
	return c.grpcClient.GetReferenceGasPrice(ctx)
}

// GetAnchorFromObjectID fetches the anchor via gRPC.
// This shadows the same method in client_anchor.go.
func (c *Client) GetAnchorFromObjectID(
	ctx context.Context,
	anchorObjectID *iotago.ObjectID,
) (*iscmove.AnchorWithRef, error) {
	return c.getAnchorFromObjectIDGRPC(ctx, anchorObjectID, nil)
}

// GetPastAnchorFromObjectID fetches the anchor at a specific version via gRPC.
// This shadows the same method in client_anchor.go.
func (c *Client) GetPastAnchorFromObjectID(
	ctx context.Context,
	anchorObjectID *iotago.ObjectID,
	version uint64,
) (*iscmove.AnchorWithRef, error) {
	return c.getAnchorFromObjectIDGRPC(ctx, anchorObjectID, &version)
}

func (c *Client) getAnchorFromObjectIDGRPC(
	ctx context.Context,
	anchorObjectID *iotago.ObjectID,
	version *uint64,
) (*iscmove.AnchorWithRef, error) {
	od, err := c.grpcClient.GetObjectBCS(ctx, anchorObjectID, version)
	if err != nil {
		return nil, fmt.Errorf("getAnchorFromObjectIDGRPC: %w", err)
	}
	ref := iotago.ObjectRef{
		ObjectID: &od.ObjectID,
		Version:  od.Version,
		Digest:   &od.Digest,
	}
	return decodeAnchorBCS(iotago.Base64Data(od.Contents), ref, od.Owner)
}

// SimulateTransaction dry-runs a transaction via gRPC.
func (c *Client) SimulateTransaction(ctx context.Context, txBytes []byte) error {
	return c.grpcClient.SimulateTransaction(ctx, txBytes)
}

// ExecuteTransaction submits a signed transaction via gRPC.
func (c *Client) ExecuteTransaction(ctx context.Context, txBytes []byte, signatures []*iotasigner.Signature) (string, error) {
	sigBytes := make([][]byte, len(signatures))
	for i, sig := range signatures {
		sigBytes[i] = sig.Bytes()
	}
	return c.grpcClient.ExecuteTransaction(ctx, txBytes, sigBytes)
}
