package iscmoveclient

import (
	"context"

	"github.com/iotaledger/wasp/clients/iota-go/iotago"
)

// GetCoin fetches and decodes a MoveCoin by object ID.
func (c *CLIClient) GetCoin(ctx context.Context, coinID *iotago.ObjectID) (*MoveCoin, error) {
	return getCoin(ctx, c, coinID)
}
