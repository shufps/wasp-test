package iscmoveclient

import (
	"context"
	"fmt"
	"os"

	bcs "github.com/iotaledger/bcs-go"
	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/clients/iscmove"
	"github.com/iotaledger/wasp/packages/cryptolib"
)

// CLIClient is the HTTP/JSON-RPC client used by wasp-cli and apilib.
// It has no gRPC dependency; all operations use JSON-RPC via the embedded *iotaclient.Client.
//
// Methods that inherently require gRPC (GetAssetsBagWithBalances,
// GetPastAnchorFromObjectID) are present as panic stubs so that the type
// satisfies the L2Client interface, but must never be called on a CLIClient.
// Solo and tests use *SoloClient which overrides those methods.
type CLIClient struct {
	*iotaclient.Client
	faucetURL string
}

// NewCLIClient wraps an existing *iotaclient.Client.
func NewCLIClient(client *iotaclient.Client, faucetURL string) *CLIClient {
	return &CLIClient{Client: client, faucetURL: faucetURL}
}

// NewHTTPClient creates a CLIClient backed by the JSON-RPC endpoint at apiURL.
func NewHTTPClient(apiURL, faucetURL string, waitUntilEffectsVisible *iotaclient.WaitParams) *CLIClient {
	return NewCLIClient(
		iotaclient.NewHTTP(apiURL, waitUntilEffectsVisible),
		faucetURL,
	)
}

func (c *CLIClient) RequestFunds(ctx context.Context, address cryptolib.Address) error {
	if c.faucetURL == "" {
		panic("missing faucetURL")
	}
	return iotaclient.RequestFundsFromFaucet(ctx, address.AsIotaAddress(), c.faucetURL)
}

func (c *CLIClient) SignAndExecutePTB(
	ctx context.Context,
	cryptolibSigner cryptolib.Signer,
	pt iotago.ProgrammableTransaction,
	gasPayments []*iotago.ObjectRef,
	gasPrice uint64,
	gasBudget uint64,
) (*iotajsonrpc.IotaTransactionBlockResponse, error) {
	signer := cryptolib.SignerToIotaSigner(cryptolibSigner)
	var err error
	if len(gasPayments) == 0 {
		gasPayments, err = c.FindCoinsForGasPayment(ctx, signer.Address(), pt, gasPrice, gasBudget)
		if err != nil {
			return nil, err
		}
	}

	if os.Getenv("DEBUG") != "" {
		pt.Print("-- SignAndExecutePTB -- ")
	}
	tx := iotago.NewProgrammable(signer.Address(), pt, gasPayments, gasBudget, gasPrice)

	txnBytes, err := bcs.Marshal(&tx)
	if err != nil {
		return nil, fmt.Errorf("can't marshal transaction into BCS encoding: %w", err)
	}
	txnResponse, err := c.SignAndExecuteTransaction(
		ctx,
		&iotaclient.SignAndExecuteTransactionRequest{
			TxDataBytes: txnBytes,
			Signer:      signer,
			Options: &iotajsonrpc.IotaTransactionBlockResponseOptions{
				ShowEffects:        true,
				ShowObjectChanges:  true,
				ShowBalanceChanges: true,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("can't execute the transaction: %w", err)
	}
	if !txnResponse.Effects.Data.IsSuccess() {
		return nil, fmt.Errorf("failed to execute the transaction: %s", txnResponse.Effects.Data.V1.Status.Error)
	}
	return txnResponse, nil
}

func (c *CLIClient) DevInspectPTB(
	ctx context.Context,
	cryptolibSigner cryptolib.Signer,
	pt iotago.ProgrammableTransaction,
	gasPayments []*iotago.ObjectRef,
	gasPrice uint64,
	gasBudget uint64,
) (*iotajsonrpc.DevInspectResults, error) {
	signer := cryptolib.SignerToIotaSigner(cryptolibSigner)
	var err error
	if len(gasPayments) == 0 {
		gasPayments, err = c.FindCoinsForGasPayment(ctx, signer.Address(), pt, gasPrice, gasBudget)
		if err != nil {
			return nil, err
		}
	}

	tx := iotago.NewProgrammable(signer.Address(), pt, gasPayments, gasBudget, gasPrice)
	txnBytes, err := bcs.Marshal(&tx.V1.Kind)
	if err != nil {
		return nil, fmt.Errorf("can't marshal transaction into BCS encoding: %w", err)
	}
	txnResponse, err := c.DevInspectTransactionBlock(
		ctx,
		iotaclient.DevInspectTransactionBlockRequest{
			SenderAddress: signer.Address(),
			TxKindBytes:   txnBytes,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("can't execute the transaction: %w", err)
	}
	if txnResponse.Error != "" {
		return nil, fmt.Errorf("execute error: %s", txnResponse.Error)
	}
	if !txnResponse.Effects.Data.IsSuccess() {
		return nil, fmt.Errorf("failed to execute the transaction: %s", txnResponse.Effects.Data.V1.Status.Error)
	}
	return txnResponse, nil
}

// GetAssetsBagWithBalances is not supported on CLIClient — use SoloClient.
func (c *CLIClient) GetAssetsBagWithBalances(_ context.Context, _ *iotago.ObjectID) (*iscmove.AssetsBagWithBalances, error) {
	panic("CLIClient.GetAssetsBagWithBalances: requires gRPC — use SoloClient")
}
