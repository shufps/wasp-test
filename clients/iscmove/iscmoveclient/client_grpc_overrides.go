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
	"fmt"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/clients/iota-go/iotasigner"
	"github.com/iotaledger/wasp/clients/iscmove"
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

// GetAnchorFromObjectID fetches the anchor, routing to gRPC when available.
// This shadows the same method in client_anchor.go.
func (c *Client) GetAnchorFromObjectID(
	ctx context.Context,
	anchorObjectID *iotago.ObjectID,
) (*iscmove.AnchorWithRef, error) {
	if c.grpcClient != nil {
		return c.getAnchorFromObjectIDGRPC(ctx, anchorObjectID, nil)
	}
	getObjectResponse, err := c.Client.GetObject(ctx, iotaclient.GetObjectRequest{
		ObjectID: anchorObjectID,
		Options:  &iotajsonrpc.IotaObjectDataOptions{ShowBcs: true, ShowOwner: true},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get anchor content: %w", err)
	}
	if getObjectResponse.Error != nil {
		return nil, fmt.Errorf("failed to get anchor content: %s", getObjectResponse.Error.Data.String())
	}
	return decodeAnchorBCS(
		getObjectResponse.Data.Bcs.Data.MoveObject.BcsBytes,
		getObjectResponse.Data.Ref(),
		getObjectResponse.Data.Owner.AddressOwner,
	)
}

// GetPastAnchorFromObjectID fetches the anchor at a specific version,
// routing to gRPC when available.
// This shadows the same method in client_anchor.go.
func (c *Client) GetPastAnchorFromObjectID(
	ctx context.Context,
	anchorObjectID *iotago.ObjectID,
	version uint64,
) (*iscmove.AnchorWithRef, error) {
	if c.grpcClient != nil {
		return c.getAnchorFromObjectIDGRPC(ctx, anchorObjectID, &version)
	}
	getObjectResponse, err := c.TryGetPastObject(ctx, iotaclient.TryGetPastObjectRequest{
		ObjectID: anchorObjectID,
		Version:  version,
		Options:  &iotajsonrpc.IotaObjectDataOptions{ShowBcs: true, ShowOwner: true},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get anchor content: %w", err)
	}
	if getObjectResponse.Data.ObjectDeleted != nil {
		return nil, fmt.Errorf("failed to get anchor content: deleted")
	}
	if getObjectResponse.Data.ObjectNotExists != nil {
		return nil, fmt.Errorf("failed to get anchor content: object does not exist")
	}
	if getObjectResponse.Data.VersionNotFound != nil {
		return nil, fmt.Errorf("failed to get anchor content: version not found")
	}
	if getObjectResponse.Data.VersionTooHigh != nil {
		return nil, fmt.Errorf("failed to get anchor content: version too high")
	}
	return decodeAnchorBCS(
		getObjectResponse.Data.VersionFound.Bcs.Data.MoveObject.BcsBytes,
		getObjectResponse.Data.VersionFound.Ref(),
		getObjectResponse.Data.VersionFound.Owner.AddressOwner,
	)
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

// SimulateTransaction dry-runs a transaction via gRPC when available,
// falling back to JSON-RPC DryRunTransaction.
func (c *Client) SimulateTransaction(ctx context.Context, txBytes []byte) error {
	if c.grpcClient != nil {
		return c.grpcClient.SimulateTransaction(ctx, txBytes)
	}
	res, err := c.Client.DryRunTransaction(ctx, txBytes)
	if err != nil {
		return err
	}
	if res.Effects.Data.IsFailed() {
		return fmt.Errorf("dry-run failed: %s", res.Effects.Data.V1.Status.Error)
	}
	return nil
}

// ExecuteTransaction submits a signed transaction via gRPC when available,
// falling back to JSON-RPC ExecuteTransactionBlock.
func (c *Client) ExecuteTransaction(ctx context.Context, txBytes []byte, signatures []*iotasigner.Signature) (string, error) {
	if c.grpcClient != nil {
		sigBytes := make([][]byte, len(signatures))
		for i, sig := range signatures {
			sigBytes[i] = sig.Bytes()
		}
		return c.grpcClient.ExecuteTransaction(ctx, txBytes, sigBytes)
	}
	res, err := c.Client.ExecuteTransactionBlock(ctx, iotaclient.ExecuteTransactionBlockRequest{
		TxDataBytes: txBytes,
		Signatures:  signatures,
		Options: &iotajsonrpc.IotaTransactionBlockResponseOptions{
			ShowObjectChanges: true,
			ShowEffects:       true,
		},
		RequestType: iotajsonrpc.TxnRequestTypeWaitForLocalExecution,
	})
	if err != nil {
		return "", err
	}
	if !res.Effects.Data.IsSuccess() {
		return "", fmt.Errorf("error executing tx: %s Digest: %s", res.Effects.Data.V1.Status.Error, res.Digest)
	}
	return res.Digest.String(), nil
}
