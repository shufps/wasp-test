package iscmoveclient

import (
	"context"
	"encoding/json"
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

// GetAssetsBagWithBalances fetches an AssetsBag and its coin/object balances via JSON-RPC
// (iotax_getDynamicFields + iota_getObject). This is the pre-migration implementation; the
// gRPC node client (GRPCClient) has its own BCS-based variant.
func (c *CLIClient) GetAssetsBagWithBalances(
	ctx context.Context,
	assetsBagID *iotago.ObjectID,
) (*iscmove.AssetsBagWithBalances, error) {
	fields, err := c.GetDynamicFields(ctx, iotaclient.GetDynamicFieldsRequest{ParentObjectID: assetsBagID})
	if err != nil {
		return nil, fmt.Errorf("failed to get DynamicFields in AssetsBag: %w", err)
	}

	bag := iscmove.AssetsBagWithBalances{
		AssetsBag: iscmove.AssetsBag{
			ID:   *assetsBagID,
			Size: uint64(len(fields.Data)),
		},
		Assets: *iscmove.NewEmptyAssets(),
	}
	for _, data := range fields.Data {
		// for coins the "field name" is of type 0x1::ascii::String
		// for non-coins it's 0x2::object::ID
		isCoin, err := iotago.IsSameResource(data.Name.Type, "0x1::ascii::String")
		if err != nil {
			return nil, fmt.Errorf("failed to check if resource is coin: %w", err)
		}

		if isCoin {
			resGetObject, err := c.GetObject(ctx, iotaclient.GetObjectRequest{
				ObjectID: &data.ObjectID,
				Options:  &iotajsonrpc.IotaObjectDataOptions{ShowContent: true},
			})
			if err != nil {
				return nil, fmt.Errorf("failed to call GetObject for Balance: %w", err)
			}

			if resGetObject.Data == nil || resGetObject.Data.Content == nil || resGetObject.Data.Content.Data.MoveObject == nil {
				return nil, fmt.Errorf("content data of AssetBag nil! (%s)", assetsBagID)
			}
			var coinBalance struct {
				ID    *iotajsonrpc.MoveUID
				Name  *iotago.ResourceType
				Value *iotajsonrpc.BigInt
			}

			err = json.Unmarshal(resGetObject.Data.Content.Data.MoveObject.Fields, &coinBalance)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal fields in Balance: %w", err)
			}

			cointype, err := iotajsonrpc.CoinTypeFromString("0x" + data.Name.Value.(string))
			if err != nil {
				return nil, fmt.Errorf("failed to convert cointype from iotajsonrpc: %w", err)
			}

			bag.SetCoin(cointype, iotajsonrpc.CoinValue(coinBalance.Value.Uint64()))
		} else {
			// non-coin asset (i.e. an "object", nft, etc)
			typ, err := iotago.ObjectTypeFromString(data.ObjectType)
			if err != nil {
				return nil, fmt.Errorf("failed to parse ObjectType: %w", err)
			}
			bag.AddObject(data.ObjectID, typ)
		}
	}

	return &bag, nil
}
