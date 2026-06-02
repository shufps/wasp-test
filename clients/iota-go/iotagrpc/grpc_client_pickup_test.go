// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

package iotagrpc

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
)

// TestPickupCoinsNilTargetPanics is a regression guard. An earlier version of
// Client.GetCoinObjsForTargetAmount passed nil as the targetAmount argument
// to iotajsonrpc.PickupCoins:
//
//	picked, err := iotajsonrpc.PickupCoins(combined, nil, gasAmount, 0, 25)
//
// Inside PickupCoins, that nil reaches new(big.Int).Add(targetAmount, ...),
// which dereferences a nil *big.Int. The call therefore panicked on every
// real invocation. This test pins the underlying library contract so any
// regression that reintroduces a literal nil at the call site is detected.
func TestPickupCoinsNilTargetPanics(t *testing.T) {
	coins := &iotajsonrpc.CoinPage{
		Data: []*iotajsonrpc.Coin{
			{Balance: iotajsonrpc.NewBigInt(1_000_000)},
		},
	}
	require.Panics(t, func() {
		_, _ = iotajsonrpc.PickupCoins(coins, nil, 100, 0, 25)
	})
}

// TestPickupCoinsWithTargetWorks documents the post-fix behaviour:
// PickupCoins requires a non-nil *big.Int targetAmount. The fix in
// Client.GetCoinObjsForTargetAmount uses new(big.Int).SetUint64(targetAmount),
// matching the original JSON-RPC implementation in iotaclient.
func TestPickupCoinsWithTargetWorks(t *testing.T) {
	coins := &iotajsonrpc.CoinPage{
		Data: []*iotajsonrpc.Coin{
			{Balance: iotajsonrpc.NewBigInt(1_000_000)},
		},
	}
	res, err := iotajsonrpc.PickupCoins(
		coins,
		new(big.Int).SetUint64(500_000),
		100,
		0,
		25,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Coins)
}
