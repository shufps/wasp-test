// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

package iscmoveclient

// NodeL1Client is the interface used by the wasp node (nodeconn, ChainFeed, EventListener)
// for all L1 interactions. It is implemented by *GRPCClient.
//
// Wasp-cli and solo use *Client (HTTP) which implements clients.L2Client.

import (
	"context"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/clients/iota-go/iotasigner"
	"github.com/iotaledger/wasp/clients/iscmove"
)

type NodeL1Client interface {
	Health(ctx context.Context) error
	GetObject(ctx context.Context, req iotaclient.GetObjectRequest) (*iotajsonrpc.IotaObjectResponse, error)
	GetAnchorFromObjectID(ctx context.Context, anchorObjectID *iotago.ObjectID) (*iscmove.AnchorWithRef, error)
	GetRequestFromObjectID(ctx context.Context, reqID *iotago.ObjectID) (*iscmove.RefWithObject[iscmove.Request], error)
	GetRequestsSorted(ctx context.Context, packageID iotago.PackageID, anchorAddress *iotago.ObjectID, maxAmountOfRequests int, cb func(error, *iscmove.RefWithObject[iscmove.Request])) error
	ExecuteTransaction(ctx context.Context, txBytes []byte, signatures []*iotasigner.Signature) (string, error)
	SimulateTransaction(ctx context.Context, txBytes []byte) error
}

// compile-time check
var _ NodeL1Client = &GRPCClient{}
