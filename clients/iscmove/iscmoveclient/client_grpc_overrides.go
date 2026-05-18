// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

package iscmoveclient

// This file provides methods on Client that shadow the identically-named
// methods on the embedded *iotaclient.Client.  When a gRPC client is
// attached (via WithGRPCClient), these methods route to gRPC StateService
// instead of the indexer-backed iotax_* JSON-RPC calls.  When no gRPC
// client is present they fall through to the JSON-RPC implementation.

import (
	"context"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
)

// GetCoins returns up to req.Limit coin objects of req.CoinType owned by
// req.Owner, starting after req.Cursor.  Routes to gRPC when available.
func (c *Client) GetCoins(ctx context.Context, req iotaclient.GetCoinsRequest) (*iotajsonrpc.CoinPage, error) {
	if c.grpcClient != nil {
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
	return c.Client.GetCoins(ctx, req)
}

// GetAllCoins returns all coin objects owned by req.Owner (paginated).
// Routes to gRPC when available.
func (c *Client) GetAllCoins(ctx context.Context, req iotaclient.GetAllCoinsRequest) (*iotajsonrpc.CoinPage, error) {
	if c.grpcClient != nil {
		var cursor []byte
		if req.Cursor != nil {
			cursor = req.Cursor[:]
		}
		return c.grpcClient.GetAllCoins(ctx, req.Owner, cursor, uint32(req.Limit))
	}
	return c.Client.GetAllCoins(ctx, req)
}

// GetCoinObjsForTargetAmount fetches IOTA coins to cover targetAmount+gasAmount.
// Routes to gRPC when available.
func (c *Client) GetCoinObjsForTargetAmount(
	ctx context.Context,
	address *iotago.Address,
	targetAmount uint64,
	gasAmount uint64,
) (iotajsonrpc.Coins, error) {
	if c.grpcClient != nil {
		return c.grpcClient.GetCoinObjsForTargetAmount(ctx, address, targetAmount, gasAmount)
	}
	return c.Client.GetCoinObjsForTargetAmount(ctx, address, targetAmount, gasAmount)
}

// GetCoinMetadata returns metadata for coinType.
// Routes to gRPC when available.
func (c *Client) GetCoinMetadata(ctx context.Context, coinType string) (*iotajsonrpc.IotaCoinMetadata, error) {
	if c.grpcClient != nil {
		return c.grpcClient.GetCoinMetadata(ctx, coinType)
	}
	return c.Client.GetCoinMetadata(ctx, coinType)
}

// GetReferenceGasPrice returns the current reference gas price.
// Routes to gRPC when available.
func (c *Client) GetReferenceGasPrice(ctx context.Context) (*iotajsonrpc.BigInt, error) {
	if c.grpcClient != nil {
		return c.grpcClient.GetReferenceGasPrice(ctx)
	}
	return c.Client.GetReferenceGasPrice(ctx)
}
