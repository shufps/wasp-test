package iscmoveclient

import (
	"context"
	"fmt"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"

	"github.com/iotaledger/wasp/clients/iscmove"
	"github.com/iotaledger/wasp/packages/cryptolib"
)

type StartNewChainRequest struct {
	Signer        cryptolib.Signer
	AnchorOwner   *cryptolib.Address
	PackageID     iotago.PackageID
	StateMetadata []byte
	InitCoinRef   *iotago.ObjectRef
	GasPayments   []*iotago.ObjectRef
	GasPrice      uint64
	GasBudget     uint64
}

// the only exception which doesn't use committee's GasCoin (the one in StateMetadata) for paying gas fee;
// this func automatically picks a coin.
func (c *CLIClient) StartNewChain(
	ctx context.Context,
	req *StartNewChainRequest,
) (*iscmove.AnchorWithRef, error) {
	ptb := iotago.NewProgrammableTransactionBuilder()
	var argInitCoin iotago.Argument
	if req.InitCoinRef != nil {
		ptb = PTBOptionSomeIotaCoin(ptb, req.InitCoinRef)
	} else {
		ptb = PTBOptionNoneIotaCoin(ptb)
	}
	argInitCoin = ptb.LastCommandResultArg()

	ptb = PTBStartNewChain(ptb, req.PackageID, req.StateMetadata, argInitCoin, req.AnchorOwner)
	txnResponse, err := c.SignAndExecutePTB(
		ctx,
		req.Signer,
		ptb.Finish(),
		req.GasPayments,
		req.GasPrice,
		req.GasBudget,
	)
	if err != nil {
		return nil, fmt.Errorf("start new chain PTB failed: %w", err)
	}

	anchorRef, err := txnResponse.GetCreatedObjectInfo(iscmove.AnchorModuleName, iscmove.AnchorObjectName)
	if err != nil {
		return nil, fmt.Errorf("failed to GetCreatedObjectInfo: %w", err)
	}
	return c.GetAnchorFromObjectID(ctx, anchorRef.ObjectID)
}

type ReceiveRequestsAndTransitionRequest struct {
	Signer           cryptolib.Signer
	PackageID        iotago.PackageID
	AnchorRef        *iotago.ObjectRef
	ConsumedRequests []iotago.ObjectRef
	SentAssets       []SentAssets
	StateMetadata    []byte
	TopUpAmount      uint64
	GasPayment       *iotago.ObjectRef
	GasPrice         uint64
	GasBudget        uint64
}

func (c *CLIClient) ReceiveRequestsAndTransition(
	ctx context.Context,
	req *ReceiveRequestsAndTransitionRequest,
) (*iotajsonrpc.IotaTransactionBlockResponse, error) {
	return receiveRequestsAndTransitionWith(ctx, c, c, req)
}

// receiveRequestsAndTransitionWith is the shared implementation used by both CLIClient and SoloClient.
// fetcher supplies GetAssetsBagWithBalances; ptbSigner supplies SignAndExecutePTB.
func receiveRequestsAndTransitionWith(
	ctx context.Context,
	fetcher iscFetcher,
	ptbSigner *CLIClient,
	req *ReceiveRequestsAndTransitionRequest,
) (*iotajsonrpc.IotaTransactionBlockResponse, error) {
	consumed := make([]ConsumedRequest, 0, len(req.ConsumedRequests))
	for _, reqRef := range req.ConsumedRequests {
		reqWithObj, err := getRequestFromObjectID(ctx, fetcher, reqRef.ObjectID)
		if err != nil {
			return nil, err
		}
		assetsBag, err := fetcher.GetAssetsBagWithBalances(ctx, &reqWithObj.Object.AssetsBag.ID)
		if err != nil {
			return nil, err
		}
		consumed = append(consumed, ConsumedRequest{
			RequestRef: reqRef,
			Assets:     assetsBag,
		})
	}

	ptb := iotago.NewProgrammableTransactionBuilder()
	ptb = PTBReceiveRequestsAndTransition(
		ptb,
		req.PackageID,
		ptb.MustObj(iotago.ObjectArg{ImmOrOwnedObject: req.AnchorRef}),
		consumed,
		req.SentAssets,
		req.StateMetadata,
		req.TopUpAmount,
	)
	return ptbSigner.SignAndExecutePTB(
		ctx,
		req.Signer,
		ptb.Finish(),
		[]*iotago.ObjectRef{req.GasPayment},
		req.GasPrice,
		req.GasBudget,
	)
}

// GetAnchorFromObjectID fetches the current anchor via JSON-RPC (HTTP client path).
func (c *CLIClient) GetAnchorFromObjectID(ctx context.Context, anchorObjectID *iotago.ObjectID) (*iscmove.AnchorWithRef, error) {
	return getAnchorFromObjectID(ctx, c, anchorObjectID)
}

// GetPastAnchorFromObjectID is not supported on CLIClient — use SoloClient.
func (c *CLIClient) GetPastAnchorFromObjectID(_ context.Context, _ *iotago.ObjectID, _ uint64) (*iscmove.AnchorWithRef, error) {
	panic("CLIClient.GetPastAnchorFromObjectID: requires gRPC — use SoloClient")
}

func decodeAnchorBCS(bcsBytes iotago.Base64Data, ref iotago.ObjectRef, owner *iotago.Address) (*iscmove.AnchorWithRef, error) {
	var moveAnchor moveAnchor
	err := iotaclient.UnmarshalBCS(bcsBytes, &moveAnchor)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal BCS: %w", err)
	}
	return &iscmove.AnchorWithRef{
		ObjectRef: ref,
		Object:    moveAnchor.ToAnchor(),
		Owner:     owner,
	}, nil
}
