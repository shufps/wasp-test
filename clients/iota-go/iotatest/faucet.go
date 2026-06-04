package iotatest

import (
	"context"
	"time"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotasigner"
	"github.com/iotaledger/wasp/packages/testutil/testkey"
)

func MakeSignerWithFunds(index int, faucetURL string, reader ...iotaclient.CoinReader) iotasigner.Signer {
	return MakeSignerWithFundsFromSeed(testkey.NewTestSeedBytes(), index, faucetURL, reader...)
}

func MakeSignerWithFundsFromSeed(
	seed []byte,
	index int,
	faucetURL string,
	reader ...iotaclient.CoinReader,
) iotasigner.Signer {
	keySchemeFlag := iotasigner.KeySchemeFlagDefault

	// there are only 256 different signers can be generated
	signer := iotasigner.NewSignerByIndex(seed, keySchemeFlag, index)
	err := iotaclient.RequestFundsFromFaucet(context.Background(), signer.Address(), faucetURL)
	if err != nil {
		panic(err)
	}
	// On public networks faucet funding is asynchronous: when a reader is
	// given, wait until the coins are actually visible before returning
	// (the local node's faucet is effectively synchronous, so local callers
	// may omit it).
	if len(reader) > 0 {
		_, err := iotaclient.WaitForCoins(context.Background(), reader[0], signer.Address(), 1, 30*time.Second)
		if err != nil {
			panic(err)
		}
	}
	return signer
}
