package iscmoveclient

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/clients/iscmove"
	"github.com/iotaledger/wasp/packages/cryptolib"
)

type CreateAndSendRequestRequest struct {
	Signer        cryptolib.Signer
	PackageID     iotago.PackageID
	AnchorAddress *iotago.ObjectID
	AssetsBagRef  *iotago.ObjectRef
	Message       *iscmove.Message
	// AllowanceBCS is either empty or a BCS-encoded iscmove.Allowance
	AllowanceBCS     []byte
	OnchainGasBudget uint64
	GasPayments      []*iotago.ObjectRef // optional
	GasPrice         uint64
	GasBudget        uint64
}

func (c *CLIClient) CreateAndSendRequest(
	ctx context.Context,
	req *CreateAndSendRequestRequest,
) (*iotajsonrpc.IotaTransactionBlockResponse, error) {
	anchorRes, err := c.GetObject(ctx, iotaclient.GetObjectRequest{ObjectID: req.AnchorAddress})
	if err != nil {
		return nil, fmt.Errorf("failed to get anchor ref: %w", err)
	}
	anchorRef := anchorRes.Data.Ref()

	ptb := iotago.NewProgrammableTransactionBuilder()
	ptb = PTBCreateAndSendRequest(
		ptb,
		req.PackageID,
		*anchorRef.ObjectID,
		ptb.MustObj(iotago.ObjectArg{ImmOrOwnedObject: req.AssetsBagRef}),
		req.Message,
		req.AllowanceBCS,
		req.OnchainGasBudget,
	)

	return c.SignAndExecutePTB(
		ctx,
		req.Signer,
		ptb.Finish(),
		req.GasPayments,
		req.GasPrice,
		req.GasBudget,
	)
}

type CreateAndSendRequestWithAssetsRequest struct {
	Signer        cryptolib.Signer
	PackageID     iotago.PackageID
	AnchorAddress *iotago.ObjectID
	Assets        *iscmove.Assets
	Message       *iscmove.Message
	// AllowanceBCS is either empty or a BCS-encoded iscmove.Allowance
	AllowanceBCS     []byte
	OnchainGasBudget uint64
	GasPayments      []*iotago.ObjectRef // optional
	GasPrice         uint64
	GasBudget        uint64
}

func (c *CLIClient) selectProperGasCoinAndBalance(ctx context.Context, req *CreateAndSendRequestWithAssetsRequest) (*iotajsonrpc.Coin, uint64, error) {
	iotaBalance := req.Assets.BaseToken()

	coinOptions, err := c.GetCoinObjsForTargetAmount(ctx, req.Signer.Address().AsIotaAddress(), iotaBalance.Uint64(), iotaclient.DefaultGasBudget)
	if err != nil {
		return nil, 0, err
	}

	coin, err := coinOptions.PickCoinNoLess(iotaBalance.Uint64())
	if err != nil {
		return nil, 0, err
	}

	return coin, iotaBalance.Uint64(), nil
}

func (c *CLIClient) CreateAndSendRequestWithAssets(
	ctx context.Context,
	req *CreateAndSendRequestWithAssetsRequest,
) (*iotajsonrpc.IotaTransactionBlockResponse, error) {
	anchorRes, err := c.GetObject(ctx, iotaclient.GetObjectRequest{ObjectID: req.AnchorAddress})
	if err != nil {
		return nil, fmt.Errorf("failed to get anchor ref: %w", err)
	}
	anchorRef := anchorRes.Data.Ref()

	allCoins, err := c.GetAllCoins(ctx, iotaclient.GetAllCoinsRequest{Owner: req.Signer.Address().AsIotaAddress()})
	if err != nil {
		return nil, fmt.Errorf("failed to get anchor ref: %w", err)
	}
	var placedCoins []lo.Tuple2[*iotajsonrpc.Coin, uint64]
	// assume we can find it in the first page
	for cointype, bal := range req.Assets.Coins.Iterate() {
		if lo.Must(iotago.IsSameResource(cointype.String(), iotajsonrpc.IotaCoinType.String())) {
			continue
		}

		coin, ok := lo.Find(allCoins.Data, func(coin *iotajsonrpc.Coin) bool {
			if !lo.Must(iotago.IsSameResource(cointype.String(), string(coin.CoinType))) {
				return false
			}
			if lo.ContainsBy(req.GasPayments, func(ref *iotago.ObjectRef) bool {
				return ref.ObjectID.Equals(*coin.CoinObjectID)
			}) {
				return false
			}
			return coin.Balance.Uint64() >= bal.Uint64()
		})
		if !ok {
			return nil, fmt.Errorf("cannot find coin for type %s", cointype)
		}
		placedCoins = append(placedCoins, lo.Tuple2[*iotajsonrpc.Coin, uint64]{A: coin, B: bal.Uint64()})
	}

	ptb := iotago.NewProgrammableTransactionBuilder()
	ptb = PTBAssetsBagNew(ptb, req.PackageID, req.Signer.Address())
	argAssetsBag := ptb.LastCommandResultArg()

	gasCoin, balance, err := c.selectProperGasCoinAndBalance(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to find an IOTA coin with proper balance ref: %w", err)
	}

	if balance > 0 {
		ptb = PTBAssetsBagPlaceCoinWithAmount(
			ptb,
			req.PackageID,
			argAssetsBag,
			iotago.GetArgumentGasCoin(),
			iotajsonrpc.CoinValue(balance),
			iotajsonrpc.IotaCoinType,
		)
	}

	for _, tuple := range placedCoins {
		ptb = PTBAssetsBagPlaceCoinWithAmount(
			ptb,
			req.PackageID,
			argAssetsBag,
			ptb.MustObj(iotago.ObjectArg{ImmOrOwnedObject: tuple.A.Ref()}),
			iotajsonrpc.CoinValue(tuple.B),
			tuple.A.CoinType,
		)
	}

	for id, t := range req.Assets.Objects.Iterate() {
		objRes, err := c.GetObject(ctx, iotaclient.GetObjectRequest{ObjectID: &id})
		if err != nil {
			return nil, fmt.Errorf("failed to get object %s: %w", id, err)
		}
		ref := objRes.Data.Ref()
		ptb = PTBAssetsBagPlaceObject(
			ptb,
			req.PackageID,
			argAssetsBag,
			ptb.MustObj(iotago.ObjectArg{ImmOrOwnedObject: &ref}),
			t,
		)
	}

	ptb = PTBCreateAndSendRequest(
		ptb,
		req.PackageID,
		*anchorRef.ObjectID,
		argAssetsBag,
		req.Message,
		req.AllowanceBCS,
		req.OnchainGasBudget,
	)
	return c.SignAndExecutePTB(
		ctx,
		req.Signer,
		ptb.Finish(),
		[]*iotago.ObjectRef{gasCoin.Ref()},
		req.GasPrice,
		req.GasBudget,
	)
}

// GetRequestFromObjectID fetches and decodes a single Request by ID.
func (c *CLIClient) GetRequestFromObjectID(ctx context.Context, reqID *iotago.ObjectID) (*iscmove.RefWithObject[iscmove.Request], error) {
	return getRequestFromObjectID(ctx, c, reqID)
}

// GetRequestsSorted fetches requests sorted by ObjectID, calling cb for each.
func (c *CLIClient) GetRequestsSorted(ctx context.Context, packageID iotago.PackageID, anchorAddress *iotago.ObjectID, maxAmountOfRequests int, cb func(error, *iscmove.RefWithObject[iscmove.Request])) error {
	return getRequestsSorted(ctx, c, packageID, anchorAddress, maxAmountOfRequests, cb)
}

// GetRequests fetches all requests (unsorted) up to maxAmountOfRequests.
func (c *CLIClient) GetRequests(ctx context.Context, packageID iotago.PackageID, anchorAddress *iotago.ObjectID, maxAmountOfRequests int) ([]*iscmove.RefWithObject[iscmove.Request], error) {
	return getRequests(ctx, c, packageID, anchorAddress, maxAmountOfRequests)
}
