package iscmoveclient

import (
	"context"

	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iscmove"
)

func (c *Client) GetAssetsBagWithBalances(
	ctx context.Context,
	assetsBagID *iotago.ObjectID,
) (*iscmove.AssetsBagWithBalances, error) {
	return c.getAssetsBagWithBalancesGRPC(ctx, assetsBagID)
}
