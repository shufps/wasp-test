// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

package iscmoveclient

// GRPCClient is the wasp node's exclusive L1 client.
// It uses gRPC for every operation and has no JSON-RPC / *iotaclient.Client dependency.
// Created via NewGRPCClient; used by nodeconn and ChainFeed.

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	bcs "github.com/iotaledger/bcs-go"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotago/serialization"
	"github.com/iotaledger/wasp/clients/iota-go/iotagrpc"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/clients/iota-go/iotasigner"
	"github.com/iotaledger/wasp/clients/iscmove"
)

// GRPCClient is a pure-gRPC L1 client for the wasp node. It has no embedded
// *iotaclient.Client and never falls back to JSON-RPC.
type GRPCClient struct {
	grpc *iotagrpc.Client
}

// NewGRPCClient creates a GRPCClient backed exclusively by gRPC.
func NewGRPCClient(grpcClient *iotagrpc.Client) *GRPCClient {
	return &GRPCClient{grpc: grpcClient}
}

// ── Health ────────────────────────────────────────────────────────────────────

func (c *GRPCClient) Health(ctx context.Context) error {
	_, err := c.grpc.GetEpochInfo(ctx)
	return err
}

// ── Object queries ────────────────────────────────────────────────────────────

// GetObject fetches a single object by ID via gRPC.
func (c *GRPCClient) GetObject(ctx context.Context, req iotaclient.GetObjectRequest) (*iotajsonrpc.IotaObjectResponse, error) {
	od, err := c.grpc.GetObjectBCS(ctx, req.ObjectID, nil)
	if err != nil {
		return nil, err
	}
	return grpcObjectDataToResponse(od), nil
}

// GetOwnedObjects returns a page of objects owned by the given address via gRPC.
func (c *GRPCClient) GetOwnedObjects(ctx context.Context, req iotaclient.GetOwnedObjectsRequest) (*iotajsonrpc.ObjectsPage, error) {
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
	page, err := c.grpc.ListOwnedObjectsPage(ctx, (*iotago.Address)(req.Address), objectType, cursor, limit)
	if err != nil {
		return nil, err
	}
	result := &iotajsonrpc.ObjectsPage{
		HasNextPage: page.HasNextPage,
		NextCursor:  page.NextCursor,
	}
	for _, od := range page.Objects {
		result.Data = append(result.Data, *grpcObjectDataToResponse(od))
	}
	return result, nil
}

// ── Coin queries ──────────────────────────────────────────────────────────────

func (c *GRPCClient) GetCoins(ctx context.Context, req iotaclient.GetCoinsRequest) (*iotajsonrpc.CoinPage, error) {
	coinType := iotajsonrpc.IotaCoinType.String()
	if req.CoinType != nil && *req.CoinType != "" {
		coinType = *req.CoinType
	}
	var cursor []byte
	if req.Cursor != nil {
		cursor = req.Cursor[:]
	}
	return c.grpc.GetCoins(ctx, req.Owner, coinType, cursor, uint32(req.Limit))
}

func (c *GRPCClient) GetAllCoins(ctx context.Context, req iotaclient.GetAllCoinsRequest) (*iotajsonrpc.CoinPage, error) {
	var cursor []byte
	if req.Cursor != nil {
		cursor = req.Cursor[:]
	}
	return c.grpc.GetAllCoins(ctx, req.Owner, cursor, uint32(req.Limit))
}

func (c *GRPCClient) GetCoinObjsForTargetAmount(ctx context.Context, address *iotago.Address, targetAmount uint64, gasAmount uint64) (iotajsonrpc.Coins, error) {
	return c.grpc.GetCoinObjsForTargetAmount(ctx, address, targetAmount, gasAmount)
}

func (c *GRPCClient) GetCoinMetadata(ctx context.Context, coinType string) (*iotajsonrpc.IotaCoinMetadata, error) {
	return c.grpc.GetCoinMetadata(ctx, coinType)
}

func (c *GRPCClient) GetReferenceGasPrice(ctx context.Context) (*iotajsonrpc.BigInt, error) {
	return c.grpc.GetReferenceGasPrice(ctx)
}

// ── ISC-specific reads ────────────────────────────────────────────────────────

// GetAnchorFromObjectID fetches the current anchor via gRPC.
func (c *GRPCClient) GetAnchorFromObjectID(ctx context.Context, anchorObjectID *iotago.ObjectID) (*iscmove.AnchorWithRef, error) {
	return c.getAnchorBCS(ctx, anchorObjectID, nil)
}

// GetPastAnchorFromObjectID fetches the anchor at a specific version via gRPC.
func (c *GRPCClient) GetPastAnchorFromObjectID(ctx context.Context, anchorObjectID *iotago.ObjectID, version uint64) (*iscmove.AnchorWithRef, error) {
	return c.getAnchorBCS(ctx, anchorObjectID, &version)
}

func (c *GRPCClient) getAnchorBCS(ctx context.Context, anchorObjectID *iotago.ObjectID, version *uint64) (*iscmove.AnchorWithRef, error) {
	od, err := c.grpc.GetObjectBCS(ctx, anchorObjectID, version)
	if err != nil {
		return nil, fmt.Errorf("getAnchorBCS: %w", err)
	}
	ref := iotago.ObjectRef{
		ObjectID: &od.ObjectID,
		Version:  od.Version,
		Digest:   &od.Digest,
	}
	return decodeAnchorBCS(iotago.Base64Data(od.Contents), ref, od.Owner)
}

// GetAssetsBagWithBalances fetches an AssetsBag and its coin/object balances via gRPC.
func (c *GRPCClient) GetAssetsBagWithBalances(ctx context.Context, assetsBagID *iotago.ObjectID) (*iscmove.AssetsBagWithBalances, error) {
	fields, err := c.grpc.GetDynamicFields(ctx, assetsBagID)
	if err != nil {
		return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: GetDynamicFields: %w", err)
	}

	if os.Getenv("DEBUG") != "" {
		fmt.Fprintf(os.Stderr, "GRPCClient.GetAssetsBagWithBalances: assetsBagID=%s fields=%d\n", assetsBagID, len(fields))
		for i, df := range fields {
			fmt.Fprintf(os.Stderr, "  field[%d]: valueType=%q valueBCSLen=%d childID=%v\n", i, df.ValueType, len(df.ValueBCS), df.ChildID)
		}
	}

	bag := iscmove.AssetsBagWithBalances{
		AssetsBag: iscmove.AssetsBag{
			ID:   *assetsBagID,
			Size: uint64(len(fields)),
		},
		Assets: *iscmove.NewEmptyAssets(),
	}

	for _, df := range fields {
		if df.ValueType == "" {
			continue
		}
		rt, err := iotago.NewResourceType(df.ValueType)
		if err != nil {
			return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: parse value type %q: %w", df.ValueType, err)
		}

		if rt.Module == "balance" && rt.ObjectName == "Balance" && rt.SubType1 != nil {
			coinType, err := iotajsonrpc.CoinTypeFromString(rt.SubType1.String())
			if err != nil {
				return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: parse coin type: %w", err)
			}
			balance, err := decodeBalanceBCS(df.ValueBCS)
			if err != nil {
				return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: decode balance for %s: %w", coinType, err)
			}
			bag.SetCoin(coinType, iotajsonrpc.CoinValue(balance))
		} else if df.ChildID != nil {
			typ, err := iotago.ObjectTypeFromString(df.ValueType)
			if err != nil {
				return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: parse object type %q: %w", df.ValueType, err)
			}
			bag.AddObject(*df.ChildID, typ)
		}
	}

	return &bag, nil
}

// GetCoin fetches and decodes a MoveCoin by object ID.
func (c *GRPCClient) GetCoin(ctx context.Context, coinID *iotago.ObjectID) (*MoveCoin, error) {
	return getCoin(ctx, c, coinID)
}

// GetRequestFromObjectID fetches and decodes a single Request.
func (c *GRPCClient) GetRequestFromObjectID(ctx context.Context, reqID *iotago.ObjectID) (*iscmove.RefWithObject[iscmove.Request], error) {
	return getRequestFromObjectID(ctx, c, reqID)
}

// GetRequestsSorted fetches requests sorted by ObjectID, calling cb for each.
func (c *GRPCClient) GetRequestsSorted(ctx context.Context, packageID iotago.PackageID, anchorAddress *iotago.ObjectID, maxAmountOfRequests int, cb func(error, *iscmove.RefWithObject[iscmove.Request])) error {
	return getRequestsSorted(ctx, c, packageID, anchorAddress, maxAmountOfRequests, cb)
}

// GetRequests fetches all requests (unsorted) up to maxAmountOfRequests.
func (c *GRPCClient) GetRequests(ctx context.Context, packageID iotago.PackageID, anchorAddress *iotago.ObjectID, maxAmountOfRequests int) ([]*iscmove.RefWithObject[iscmove.Request], error) {
	return getRequests(ctx, c, packageID, anchorAddress, maxAmountOfRequests)
}

// ── Transaction execution ─────────────────────────────────────────────────────

// SimulateTransaction dry-runs a transaction via gRPC.
func (c *GRPCClient) SimulateTransaction(ctx context.Context, txBytes []byte) error {
	return c.grpc.SimulateTransaction(ctx, txBytes)
}

// ExecuteTransaction submits a signed transaction via gRPC.
func (c *GRPCClient) ExecuteTransaction(ctx context.Context, txBytes []byte, signatures []*iotasigner.Signature) (string, error) {
	sigBytes := make([][]byte, len(signatures))
	for i, sig := range signatures {
		sigBytes[i] = sig.Bytes()
	}
	return c.grpc.ExecuteTransaction(ctx, txBytes, sigBytes)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// grpcObjectDataToResponse builds a synthetic IotaObjectResponse from a gRPC ObjectData.
func grpcObjectDataToResponse(od *iotagrpc.ObjectData) *iotajsonrpc.IotaObjectResponse {
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

// decodeBalanceBCS decodes a BCS-encoded Move Balance<T> value.
func decodeBalanceBCS(data []byte) (uint64, error) {
	if len(data) == 8 {
		return binary.LittleEndian.Uint64(data), nil
	}
	v, err := bcs.Unmarshal[uint64](data)
	if err != nil {
		return 0, fmt.Errorf("decodeBalanceBCS: %w", err)
	}
	return v, nil
}
